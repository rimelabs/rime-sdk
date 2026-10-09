import type * as grpc from "@grpc/grpc-js";
import { abortable, scope } from "../cancellation.js";
import {
  RimeError,
  RimeInputError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeStreamError,
} from "../errors.js";
import { InputAudio } from "./audio.js";
import { TranscriptQueue } from "./queue.js";
import { TranscriptState } from "./protocol.js";
import { TranscriptionCall } from "./transport.js";
import { policy } from "./policy.js";
import type {
  AudioSource,
  TranscriptionOptions,
  TranscriptionUpdate,
} from "./types.js";

interface StreamOwner {
  prepare(
    signal: AbortSignal,
  ): Promise<{ client: grpc.Client; metadata: grpc.Metadata }>;
  checkOpen(): void;
  forget(stream: TranscriptStream): void;
}

/** Lazy, single-consumer stream. Source exhaustion commits input; return/cancel aborts it. */
export class TranscriptStream implements AsyncIterableIterator<TranscriptionUpdate> {
  private readonly controller = new AbortController();
  private readonly queue = new TranscriptQueue(policy.queuedUpdates);
  private readonly timers = new Set<ReturnType<typeof scope>>();
  private worker: Promise<void> | null = null;
  private call: TranscriptionCall | null = null;
  private failure: RimeError | null = null;
  private reading = false;
  private finished = false;
  private inputDone = false;
  private accepted!: () => void;
  private readonly acceptance = new Promise<void>((resolve) => {
    this.accepted = resolve;
  });
  private admission: ReturnType<typeof scope> | null = null;
  private readonly externalAbort = () =>
    this.fail(new RimeCancelledError("Transcription cancelled"));

  constructor(
    private readonly owner: StreamOwner,
    private source: AudioSource | null,
    private readonly options: TranscriptionOptions,
  ) {}

  /** Server request identity, available once response headers arrive. */
  get requestId(): string | null {
    return this.call?.requestId ?? null;
  }

  private deadline(seconds: number, message: string) {
    const timer = scope([], seconds, new RimeTimeoutError(message));
    timer.signal.addEventListener(
      "abort",
      () => this.fail(timer.signal.reason),
      { once: true },
    );
    if (timer.signal.aborted) this.fail(timer.signal.reason);
    this.timers.add(timer);
    return timer;
  }
  private start() {
    if (this.failure) throw this.failure;
    if (this.finished || this.worker) return;
    this.owner.checkOpen();
    if (this.options.signal?.aborted) {
      this.externalAbort();
      throw this.failure;
    }
    this.options.signal?.addEventListener("abort", this.externalAbort, {
      once: true,
    });
    if (this.options.timeout != null)
      this.deadline(
        this.options.timeout,
        "Overall transcription deadline expired",
      );
    this.worker = this.run();
  }
  private fail(error: RimeError) {
    if (this.failure || this.finished) return;
    if (error.requestId === null && this.requestId !== null)
      error = new (error.constructor as typeof RimeError)(
        error.message,
        this.requestId,
        { cause: error.cause },
      );
    this.failure = error;
    this.queue.fail(error);
    for (const timer of this.timers) timer.dispose();
    this.controller.abort(error);
    this.call?.cancel();
    this.options.signal?.removeEventListener("abort", this.externalAbort);
    if (!this.worker) {
      this.source = null;
      this.owner.forget(this);
    }
  }
  private async produce() {
    const signal = this.controller.signal;
    await abortable(this.acceptance, signal);
    const audio = new InputAudio(this.options.inputFormat);
    let iterator: AsyncIterator<Uint8Array> | undefined;
    try {
      try {
        iterator = this.source![Symbol.asyncIterator]();
      } catch (error) {
        throw new RimeInputError("The audio source failed", this.requestId, {
          cause: error,
        });
      }
      for (;;) {
        let result: IteratorResult<Uint8Array>;
        try {
          result = await abortable(iterator.next(), signal);
        } catch (error) {
          if (signal.aborted) throw signal.reason;
          throw new RimeInputError("The audio source failed", this.requestId, {
            cause: error,
          });
        }
        if (result.done) break;
        for (const part of audio.feed(result.value))
          await this.call!.writeAudio(part);
      }
      audio.finish();
      this.inputDone = true;
      this.deadline(
        policy.completionTimeout,
        "Transcription completion timed out",
      );
      this.call!.finishInput();
    } finally {
      if (iterator?.return) {
        const cleanup = scope(
          [],
          policy.cleanupTimeout,
          new RimeTimeoutError("Audio source cleanup timed out"),
        );
        try {
          await abortable(
            Promise.resolve().then(() => iterator!.return!()),
            cleanup.signal,
          );
        } catch {
          /* Caller-owned source cleanup cannot replace the operation outcome. */
        } finally {
          cleanup.dispose();
        }
      }
    }
  }
  private async read() {
    const state = new TranscriptState(policy.transcriptBytes);
    for await (const message of this.call!.responses()) {
      const update = state.accept(message, this.inputDone);
      if (state.language !== null) {
        this.admission?.dispose();
        this.accepted();
      }
      if (update) await this.queue.put(update);
    }
    return state.finish();
  }
  private async run() {
    let tasks: Promise<unknown>[] = [];
    try {
      const prepared = await this.owner.prepare(this.controller.signal);
      this.controller.signal.throwIfAborted();
      this.call = new TranscriptionCall(
        prepared.client,
        prepared.metadata,
        this.controller.signal,
      );
      this.admission = this.deadline(
        policy.acceptanceTimeout,
        "Transcription acceptance timed out",
      );
      await this.call.start(
        this.options.language,
        this.options.mode ?? "written",
        this.options.contextTerms ?? [],
      );
      const producer = this.produce(),
        reader = this.read();
      tasks = [producer, reader];
      const [, final] = await Promise.all([producer, reader]);
      await this.queue.put(final);
      this.queue.finish();
    } catch (error) {
      this.fail(
        error instanceof RimeError
          ? error
          : new RimeStreamError("Transcription failed", this.requestId, {
              cause: error,
            }),
      );
    } finally {
      if (this.failure) this.controller.abort(this.failure);
      this.call?.cancel();
      await Promise.allSettled(tasks);
      this.source = null;
      if (this.failure) this.owner.forget(this);
    }
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  async next(): Promise<IteratorResult<TranscriptionUpdate>> {
    if (this.reading)
      throw new RimeInputError(
        "TranscriptStream permits only one concurrent reader",
      );
    this.reading = true;
    try {
      this.start();
      if (this.finished) return { done: true, value: undefined };
      const result = await this.queue.get();
      if (this.failure) throw this.failure;
      if (!result.done && result.value.kind === "final") {
        await this.cleanup();
        if (this.failure) throw this.failure;
        this.finished = true;
      }
      return result;
    } finally {
      this.reading = false;
    }
  }
  private async cleanup() {
    await this.worker;
    for (const timer of this.timers) timer.dispose();
    this.options.signal?.removeEventListener("abort", this.externalAbort);
    this.owner.forget(this);
  }
  /** Cancel unfinished work and release owned resources; safe to repeat. */
  async cancel(): Promise<void> {
    this.fail(new RimeCancelledError("Transcription cancelled"));
    await this.cleanup();
    this.source = null;
  }
  async return(): Promise<IteratorResult<TranscriptionUpdate>> {
    await this.cancel();
    return { done: true, value: undefined };
  }
  async [Symbol.asyncDispose]() {
    await this.cancel();
  }
}
