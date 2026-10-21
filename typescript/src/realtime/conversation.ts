import { abortable, scope } from "../cancellation.js";
import {
  RimeCancelledError,
  RimeInputError,
  RimeResourceLimitError,
  RimeTimeoutError,
} from "../errors.js";
import { Deferred } from "./async.js";
import type { RealtimeSession } from "./session.js";
import { isSessionCloseError } from "./session.js";
import { RimeRealtimeError } from "./types.js";
import type {
  AudioChunk,
  OutputRef,
  ResponseRef,
  SessionEvent,
} from "./types.js";

export interface ConversationSource {
  /** Return one chunk, or null at EOF. Abort must release a pending read. */
  read(signal: AbortSignal): Promise<AudioChunk | null>;
}
export interface ConversationOutput {
  /** Queue audio without blocking. Raise on overflow; never silently drop audio. */
  write(chunk: AudioChunk): void;
  /** Seal this output; completion still waits for the device to drain. */
  end(): void;
  /** Stop this output and discard unplayed audio. Also works after end().
   * Do not start later queued outputs as a side effect. */
  interrupt(): void;
  /** Measured played milliseconds. Reject on measurement failure or abort.
   * Abort stops the wait, without cancelling the device receipt. */
  completion(signal: AbortSignal): Promise<number>;
}
export interface ConversationPlayback {
  /** Queue an output in open order; audible playback must be FIFO.
   * Keep completion and interruption state separate for each output.
   * Do not mix outputs or play a later output ahead of an earlier one. */
  open(output: OutputRef): ConversationOutput;
  /** Confirm all playback has stopped and queued audio is discarded.
   * Caller resources stay open. Reject and release the wait on abort.
   * The runner supplies a separate signal with a five-second deadline and
   * waits for cancellation cleanup before returning. */
  stop(signal: AbortSignal): Promise<void>;
}
export interface ConversationOptions {
  readonly source: ConversationSource;
  readonly playback: ConversationPlayback;
  /** Called inline for every event. Must be synchronous and nonblocking.
   * Scheduled application work remains the caller's responsibility. */
  readonly onEvent?: (event: SessionEvent) => void;
  readonly signal?: AbortSignal;
}
interface Output {
  ref: OutputRef;
  stream: ConversationOutput;
  receipt: Promise<number>;
  sealed: boolean;
  report?: Promise<void>;
}
interface Reply {
  ref: ResponseRef;
  outputs: Map<string, Output>;
  interrupted: boolean;
  ended: boolean;
  finish?: AbortController;
  drained: Deferred<void>;
}

/** Run until input EOF, session closure, failure, or abort.
 * Owns the session's active lifetime and closes it on exit. The caller owns
 * adapter resource contexts. Do not consume session.events or report playback
 * elsewhere. Adapter operations must settle on abort. No input or callback
 * queue is added. Limits: 64 responses, 128 outputs, 256 pending operations.
 * Overflow fails the run. Callback and adapter failures reach the caller.
 */
export async function runConversation(
  session: RealtimeSession,
  { source, playback, onEvent, signal }: ConversationOptions,
): Promise<void> {
  const controller = new AbortController();
  const running = controller.signal;
  const receipts = new AbortController();
  const abort = () => controller.abort(signal?.reason);
  if (signal?.aborted) abort();
  else signal?.addEventListener("abort", abort, { once: true });
  const replies = new Map<string, Reply>();
  const tasks = new Set<Promise<unknown>>();
  let fail!: (error: unknown) => void;
  let failed = false;
  let failureReason: unknown;
  const failure = new Promise<never>((_, reject) => {
    fail = (error) => {
      if (!failed) {
        failed = true;
        failureReason = error;
        reject(error);
      }
    };
  });
  void failure.catch(() => {});
  function start<T>(work: () => Promise<T>, local?: AbortSignal): Promise<T> {
    if (tasks.size >= 256)
      throw new RimeResourceLimitError(
        "Conversation pending operation limit exceeded",
      );
    const task = work();
    tasks.add(task);
    void task.then(
      () => tasks.delete(task),
      (error) => {
        tasks.delete(task);
        const expected = [local, running, receipts.signal].some(
          (signal) =>
            signal?.aborted &&
            (error === signal.reason ||
              error instanceof RimeCancelledError ||
              (error instanceof DOMException && error.name === "AbortError")),
        );
        if (!expected && !isSessionCloseError(error)) fail(error);
      },
    );
    return task;
  }
  async function measure(stream: ConversationOutput) {
    const value = await stream.completion(receipts.signal);
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
      throw new RimeInputError(
        "Playback measurement must be finite nonnegative milliseconds",
      );
    return value;
  }
  function end(output: Output) {
    if (!output.sealed) {
      output.sealed = true;
      output.stream.end();
    }
  }
  function cut(output: Output) {
    if (output.report) return;
    output.stream.interrupt();
    end(output);
    output.report = start(async () => {
      const played = await output.receipt;
      running.throwIfAborted();
      try {
        await session.reportPlayback(
          {
            kind: "interrupted",
            output: output.ref,
            audioEndMs: Math.floor(played),
          },
          { signal: running },
        );
      } catch (error) {
        // The session also forwards the refusal to the application callback.
        if (
          !(error instanceof RimeRealtimeError) ||
          error.fault.scope !== "event" ||
          error.fault.code !== "truncate_not_current"
        )
          throw error;
      }
    });
  }
  function finish(reply: Reply) {
    const earlier: Promise<void>[] = [];
    for (const previous of replies.values()) {
      if (previous === reply) break;
      earlier.push(previous.drained.promise);
    }
    const finishing = new AbortController();
    reply.finish = finishing;
    const cancel = () => finishing.abort(running.reason);
    if (running.aborted) cancel();
    else running.addEventListener("abort", cancel, { once: true });
    start(async () => {
      try {
        let played = 0;
        for (const output of reply.outputs.values())
          played += await abortable(output.receipt, finishing.signal);
        reply.drained.resolve();
        if (reply.interrupted) {
          for (const output of reply.outputs.values())
            if (output.report) await abortable(output.report, finishing.signal);
        } else {
          // Prism's latest drain token clears all earlier playback too.
          for (const drained of earlier)
            await abortable(drained, finishing.signal);
          await session.reportPlayback(
            { kind: "finished", response: reply.ref, playedMs: played },
            { signal: finishing.signal },
          );
        }
        finishing.signal.throwIfAborted();
        replies.delete(reply.ref.responseId);
      } finally {
        running.removeEventListener("abort", cancel);
      }
    }, finishing.signal);
  }
  function interrupt(reply: Reply) {
    if (reply.interrupted) return;
    reply.interrupted = true;
    reply.finish?.abort();
    for (const output of reply.outputs.values()) cut(output);
    if (reply.ended) finish(reply);
  }
  function outputFor(ref: OutputRef) {
    const reply = replies.get(ref.response.responseId);
    if (!reply || reply.ended) return;
    const key = JSON.stringify([ref.itemId, ref.outputIndex, ref.contentIndex]);
    let output = reply.outputs.get(key);
    if (!output) {
      let count = 0;
      for (const value of replies.values()) count += value.outputs.size;
      if (count >= 128)
        throw new RimeResourceLimitError("Conversation output limit exceeded");
      const stream = playback.open(ref);
      output = {
        ref,
        stream,
        receipt: start(() => measure(stream)),
        sealed: false,
      };
      reply.outputs.set(key, output);
      if (reply.interrupted) cut(output);
    }
    return output;
  }
  async function send() {
    while (!running.aborted) {
      const chunk = await source.read(running);
      if (chunk === null) return;
      await session.sendAudio(chunk, { signal: running });
    }
    running.throwIfAborted();
  }
  async function read() {
    for await (const event of session.events) {
      running.throwIfAborted();
      const payload = event.payload;
      switch (payload.kind) {
        case "response.started":
          if (replies.size >= 64)
            throw new RimeResourceLimitError(
              "Conversation response limit exceeded",
            );
          replies.set(payload.response.responseId, {
            ref: payload.response,
            outputs: new Map(),
            interrupted: false,
            ended: false,
            drained: new Deferred<void>(),
          });
          break;
        case "message.started":
        case "audio.delta":
        case "audio.done": {
          const output = outputFor(payload.output);
          if (output) {
            const reply = replies.get(payload.output.response.responseId)!;
            if (payload.kind === "audio.delta" && !reply.interrupted)
              output.stream.write(payload.audio);
            else if (payload.kind === "audio.done") end(output);
          }
          break;
        }
        case "speech.started":
          if (session.info.interruptOnSpeech)
            for (const reply of replies.values()) interrupt(reply);
          break;
        case "response.abandoned": {
          const reply = replies.get(payload.response.responseId);
          if (reply) interrupt(reply);
          break;
        }
        case "response.ended": {
          const reply = replies.get(payload.response.responseId);
          if (reply) {
            reply.ended = true;
            for (const output of reply.outputs.values()) end(output);
            finish(reply);
          }
          break;
        }
      }
      const result: unknown = onEvent?.(event);
      if (
        result &&
        typeof (result as PromiseLike<unknown>).then === "function"
      ) {
        void Promise.resolve(result).catch(() => {});
        throw new RimeInputError("onEvent must be synchronous and nonblocking");
      }
    }
  }
  let original = false;
  try {
    running.throwIfAborted();
    await abortable(Promise.race([start(send), start(read), failure]), running);
    if (failed) throw failureReason;
  } catch (error) {
    if (!isSessionCloseError(error)) {
      original = true;
      throw error;
    }
  } finally {
    let cleanupError: unknown;
    // Stop input, event processing and reports, but keep receipt waits alive
    // until the adapter confirms playback has stopped or the deadline expires.
    controller.abort();
    const shutdown = scope(
      [],
      5,
      new RimeTimeoutError("Playback shutdown confirmation timed out"),
    );
    try {
      // Await the adapter itself, including cleanup after the deadline aborts.
      // Racing this promise against abort would leave that cleanup running.
      await playback.stop(shutdown.signal);
      shutdown.signal.throwIfAborted();
    } catch (error) {
      cleanupError = error;
    } finally {
      shutdown.dispose();
    }
    receipts.abort();
    try {
      await session.close();
    } catch (error) {
      cleanupError ??= error;
    }
    await Promise.allSettled(tasks);
    signal?.removeEventListener("abort", abort);
    if (!original) {
      if (failed) throw failureReason;
      signal?.throwIfAborted();
      if (cleanupError !== undefined) throw cleanupError;
    }
  }
}
