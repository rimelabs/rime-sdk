import { randomUUID } from "node:crypto";
import { debuglog } from "node:util";
const log = debuglog("rime-sdk");
import { AudioFormat, Converter } from "./audio.js";
import { policy, abortable } from "./policy.js";
import { ByteQueue } from "./queue.js";
import { SentenceBuffer, ready } from "./sentences.js";
import {
  grpc,
  openStream,
  header,
  textMessage,
  rpcError,
} from "./transport.js";
import {
  RimeError,
  RimeInputError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeStreamError,
  RimeAudioFormatError,
} from "./errors.js";

export interface StreamOwner {
  prepare(
    signal: AbortSignal,
  ): Promise<{ client: grpc.Client; metadata: grpc.Metadata }>;
  checkOpen(): void;
  forget(stream: AudioStream): void;
}
export type TextSource = string | AsyncIterable<string>;
export const constructionKey = Symbol("private stream constructor");
export class AudioStream implements AsyncIterableIterator<Uint8Array> {
  private requestIdValue: string | null = null;
  private readonly controller = new AbortController();
  private readonly queue = new ByteQueue(policy.outputBytes);
  private worker: Promise<void> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private call: ReturnType<typeof openStream> | null = null;
  private failure: RimeError | null = null;
  private finished = false;
  private reading = false;
  private inputDone = false;
  private sourceWaiting = false;
  private outputWaiting = false;
  private submitted = false;
  private received = false;
  private startedAt = 0;
  private progressAt = 0;
  private readonly operationId = randomUUID();
  private bytesReceived = 0;
  private bytesDelivered = 0;
  constructor(
    key: symbol,
    private owner: StreamOwner,
    private source: TextSource | null,
    private voice: string,
    private language: string,
    private formatValue: AudioFormat,
    private timeout: number | null,
  ) {
    if (key !== constructionKey)
      throw new TypeError("AudioStream is returned by client.tts.stream()");
  }
  get format(): AudioFormat {
    return this.formatValue;
  }
  get requestId(): string | null {
    return this.requestIdValue;
  }
  private start() {
    if (this.failure) throw this.failure;
    if (this.finished || this.worker) return;
    this.owner.checkOpen();
    this.startedAt = performance.now();
    this.worker = this.run();
    this.timer = setInterval(() => {
      if (this.finished || this.failure) return;
      const now = performance.now();
      if (
        this.timeout !== null &&
        now - this.startedAt >= this.timeout * 1000
      ) {
        this.fail(
          new RimeTimeoutError(
            "Overall synthesis deadline expired",
            this.requestId,
          ),
        );
        return;
      }
      if (
        this.call &&
        !this.call.destroyed &&
        this.submitted &&
        !this.sourceWaiting &&
        !this.outputWaiting &&
        this.queue.size === 0
      ) {
        const limit = this.received
          ? policy.progressTimeout
          : policy.firstAudioTimeout;
        if (now - this.progressAt >= limit * 1000)
          this.fail(
            new RimeTimeoutError(
              "Synthesis output stopped making progress",
              this.requestId,
            ),
          );
      } else this.progressAt = now;
    }, 20);
  }
  private fail(error: RimeError) {
    if (this.finished || this.failure) return;
    if (error.requestId === null && this.requestIdValue !== null)
      error = new (error.constructor as typeof RimeError)(
        error.message,
        this.requestIdValue,
        { cause: error.cause },
      );
    this.failure = error;
    if (this.timer) clearInterval(this.timer);
    this.queue.finish(error);
    this.controller.abort(error);
    this.call?.cancel();
  }
  private async write(message: unknown) {
    const call = this.call!;
    await abortable(
      new Promise<void>((resolve, reject) =>
        call.write(message, (error: Error | null | undefined) =>
          error ? reject(error) : resolve(),
        ),
      ),
      this.controller.signal,
    );
  }
  private async produce() {
    const source = this.source!;
    let iterator: AsyncIterator<string> | undefined;
    const buffer = new SentenceBuffer(policy.sentenceBytes);
    let meaningful = false;
    try {
      try {
        iterator = (
          typeof source === "string"
            ? (async function* () {
                yield source;
              })()
            : source
        )[Symbol.asyncIterator]();
      } catch (error) {
        if (this.controller.signal.aborted) throw this.controller.signal.reason;
        throw new RimeInputError("The text source failed", this.requestId, {
          cause: error,
        });
      }
      for (;;) {
        this.sourceWaiting = true;
        let result: IteratorResult<string>;
        try {
          result = await abortable(iterator.next(), this.controller.signal);
        } catch (error) {
          if (this.controller.signal.aborted)
            throw this.controller.signal.reason;
          throw new RimeInputError("The text source failed", this.requestId, {
            cause: error,
          });
        } finally {
          this.sourceWaiting = false;
        }
        if (result.done) break;
        const chunk = result.value;
        if (typeof chunk !== "string")
          throw new RimeInputError("The text source must yield strings");
        if (chunk.trim()) meaningful = true;
        // Never split a surrogate pair when limiting native detector input size.
        for (let offset = 0; offset < chunk.length;) {
          let end = Math.min(chunk.length, offset + policy.sourceChars);
          if (end < chunk.length && /[\uD800-\uDBFF]/.test(chunk[end - 1]!))
            end++;
          for (const sentence of buffer.feed(chunk.slice(offset, end))) {
            if (!this.submitted) this.progressAt = performance.now();
            this.submitted = true;
            await this.write(textMessage(sentence));
          }
          offset = end;
        }
      }
      if (!meaningful)
        throw new RimeInputError(
          "The text source contained no meaningful text",
        );
      for (const sentence of buffer.feed("", true)) {
        if (!this.submitted) this.progressAt = performance.now();
        this.submitted = true;
        await this.write(textMessage(sentence));
      }
      this.call!.end();
      this.inputDone = true;
    } catch (error) {
      if (
        error instanceof RimeError ||
        this.controller.signal.aborted ||
        (error && typeof error === "object" && "code" in error)
      )
        throw error;
      throw new RimeInputError("The text source failed", this.requestId, {
        cause: error,
      });
    } finally {
      if (iterator?.return) {
        const cleanup = Promise.resolve(iterator.return());
        cleanup.catch(() => {});
        let timer: ReturnType<typeof setTimeout> | undefined;
        await Promise.race([
          cleanup,
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, policy.cleanupTimeout * 1000);
          }),
        ]).catch(() => {});
        clearTimeout(timer);
      }
    }
  }
  private async put(data: Buffer) {
    for (let i = 0; i < data.length; i += policy.outputChunkBytes) {
      this.outputWaiting = true;
      try {
        await this.queue.put(data.subarray(i, i + policy.outputChunkBytes));
      } finally {
        this.outputWaiting = false;
      }
    }
  }
  private async read(
    metadataPromise: Promise<grpc.Metadata>,
    statusPromise: Promise<grpc.StatusObject>,
  ) {
    const metadata = await abortable(metadataPromise, this.controller.signal);
    const contentType = metadata.get("x-rime-audio-content-type")[0];
    // A rejection can send headers before its failure status. Check the
    // format only when audio arrives or the service completes successfully.
    const converter = new Converter(this.format);
    for await (const response of this.call!) {
      this.progressAt = performance.now();
      if (response.audio.length) {
        if (contentType !== "audio/pcm")
          throw new RimeAudioFormatError(
            "Expected raw audio/pcm from the service",
            this.requestId,
          );
        this.received = true;
        this.bytesReceived += response.audio.length;
        await this.put(converter.process(response.audio));
      }
    }
    const status = await abortable(statusPromise, this.controller.signal);
    this.requestIdValue ??=
      status.metadata.get("x-request-id")[0]?.toString() ?? null;
    if (status.code !== grpc.status.OK)
      throw rpcError(status.code, this.requestId);
    if (contentType !== "audio/pcm")
      throw new RimeAudioFormatError(
        "Expected raw audio/pcm from the service",
        this.requestId,
      );
    if (!this.inputDone)
      throw new RimeStreamError(
        "The service completed before input finished",
        this.requestId,
      );
    await this.put(converter.process(new Uint8Array(), true));
  }
  private async run() {
    let tasks: Promise<void>[] = [];
    try {
      await abortable(ready, this.controller.signal);
      const prepared = await this.owner.prepare(this.controller.signal);
      this.call = openStream(prepared.client, prepared.metadata);
      const metadataPromise = new Promise<grpc.Metadata>((resolve, reject) => {
        this.call!.once("metadata", (metadata: grpc.Metadata) => {
          this.requestIdValue =
            metadata.get("x-request-id")[0]?.toString() ?? null;
          resolve(metadata);
        });
        // A trailers-only response has no metadata event, including an empty
        // successful response. Let the reader validate its final status.
        this.call!.once("status", () => resolve(new grpc.Metadata()));
        this.call!.once("error", reject);
      });
      metadataPromise.catch(() => {});
      const statusPromise = new Promise<grpc.StatusObject>((resolve) =>
        this.call!.once("status", resolve),
      );
      this.call.on("error", () => {});
      await this.write(header(this.voice, this.language));
      tasks = [this.produce(), this.read(metadataPromise, statusPromise)];
      await Promise.all(tasks);
      this.queue.finish();
    } catch (error) {
      if (error instanceof RimeError) this.fail(error);
      else if (
        error &&
        typeof error === "object" &&
        "code" in error &&
        typeof error.code === "number"
      ) {
        const metadata = "metadata" in error ? error.metadata : null;
        if (metadata instanceof grpc.Metadata)
          this.requestIdValue ??=
            metadata.get("x-request-id")[0]?.toString() ?? null;
        this.fail(rpcError(error.code, this.requestId));
      } else
        this.fail(
          new RimeStreamError("Synthesis failed", this.requestId, {
            cause: error,
          }),
        );
    } finally {
      if (this.failure) this.controller.abort(this.failure);
      await Promise.allSettled(tasks);
      this.source = null;
      if (this.failure) this.owner.forget(this);
      log(
        "operation=%s request=%s received=%d delivered=%d duration_ms=%d",
        this.operationId,
        this.requestId,
        this.bytesReceived,
        this.bytesDelivered,
        Math.round(performance.now() - this.startedAt),
      );
    }
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  async next(): Promise<IteratorResult<Uint8Array>> {
    if (this.reading)
      throw new RimeInputError(
        "AudioStream permits only one concurrent reader",
      );
    this.reading = true;
    try {
      this.start();
      const result = await this.queue.get();
      if (this.failure) throw this.failure;
      if (result.done) {
        await this.cleanup();
        if (this.failure) throw this.failure;
        this.finished = true;
      }
      if (!result.done) this.bytesDelivered += result.value.byteLength;
      return result;
    } finally {
      this.reading = false;
    }
  }
  private async cleanup() {
    if (this.timer) clearInterval(this.timer);
    await this.worker;
    this.owner.forget(this);
  }
  async cancel(): Promise<void> {
    if (!this.finished && !this.failure)
      this.fail(new RimeCancelledError("Synthesis cancelled", this.requestId));
    await this.cleanup();
    this.source = null;
  }
  async return(): Promise<IteratorResult<Uint8Array>> {
    await this.cancel();
    return { done: true, value: undefined };
  }
  async [Symbol.asyncDispose]() {
    await this.cancel();
  }
}
