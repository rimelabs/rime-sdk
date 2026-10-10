import { randomUUID } from "node:crypto";
import { debuglog } from "node:util";
const log = debuglog("rime-sdk");
import { AudioFormat, Converter } from "./audio.js";
import { policy } from "./policy.js";
import { abortable } from "../cancellation.js";
import { ByteQueue } from "./queue.js";
import { SentenceBuffer, ready } from "./sentences.js";
import { SynthesisCall, type PreparedConnection } from "./transport.js";
import {
  RimeError,
  RimeInputError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeStreamError,
} from "../errors.js";

export interface StreamOwner {
  prepare(signal: AbortSignal): Promise<PreparedConnection>;
  checkOpen(): void;
  forget(stream: AudioStream): void;
}
export type TextSource = AsyncIterable<string>;
export const constructionKey = Symbol("private stream constructor");
export class AudioStream implements AsyncIterableIterator<Uint8Array> {
  private readonly controller = new AbortController();
  private readonly queue = new ByteQueue(
    policy.outputBytes,
    policy.outputChunkBytes,
  );
  private worker: Promise<void> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private call: SynthesisCall | null = null;
  private failure: RimeError | null = null;
  private finished = false;
  private reading = false;
  private inputDone = false;
  private sourceWaiting = false;
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
      throw new TypeError(
        "AudioStream is returned by client.tts.synthesize() or client.tts.stream()",
      );
  }
  get format(): AudioFormat {
    return this.formatValue;
  }
  get requestId(): string | null {
    return this.call?.requestId ?? null;
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
        !this.call.done &&
        this.submitted &&
        !this.sourceWaiting &&
        !this.queue.hasPendingOutput
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
    if (error.requestId === null && this.requestId !== null)
      error = new (error.constructor as typeof RimeError)(
        error.message,
        this.requestId,
        { cause: error.cause },
      );
    this.failure = error;
    if (this.timer) clearInterval(this.timer);
    this.queue.fail(error);
    this.controller.abort(error);
    this.call?.cancel();
  }
  private async produce() {
    const source = this.source!;
    let iterator: AsyncIterator<string> | undefined;
    const buffer = new SentenceBuffer(policy.sentenceBytes);
    let meaningful = false;
    try {
      try {
        iterator = source[Symbol.asyncIterator]();
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
            await this.call!.write(sentence);
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
        await this.call!.write(sentence);
      }
      this.call!.finishInput();
      this.inputDone = true;
    } catch (error) {
      if (error instanceof RimeError || this.controller.signal.aborted)
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
  private async read() {
    const converter = new Converter(this.format);
    for await (const data of this.call!.audio()) {
      this.progressAt = performance.now();
      if (data.length) {
        this.received = true;
        this.bytesReceived += data.length;
        await this.queue.put(converter.process(data));
      }
    }
    if (!this.inputDone)
      throw new RimeStreamError(
        "The service completed before input finished",
        this.requestId,
      );
    await this.queue.put(converter.process(new Uint8Array(), true));
  }
  private async run() {
    let tasks: Promise<void>[] = [];
    try {
      await abortable(ready, this.controller.signal);
      const prepared = await this.owner.prepare(this.controller.signal);
      this.call = new SynthesisCall(prepared, this.controller.signal);
      await this.call.start(this.voice, this.language);
      tasks = [this.produce(), this.read()];
      await Promise.all(tasks);
      this.queue.finish();
    } catch (error) {
      if (error instanceof RimeError) this.fail(error);
      else
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
