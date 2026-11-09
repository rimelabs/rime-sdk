import { randomUUID } from "node:crypto";
import {
  RimeError,
  RimeResourceLimitError,
  RimeStreamError,
  RimeCancelledError,
} from "../errors.js";
import { scope } from "./async.js";
import { reviewDecision } from "./settings.js";
import type { Review, ReviewDecision, ReviewRequest } from "./types.js";

/** Owns callback work independently of public event consumption and response creation. */
export class Reviews {
  private readonly active = new Map<
    string,
    { request: ReviewRequest; stop: AbortController }
  >();
  private readonly seen = new Set<string>();
  private readonly stopped = new Map<
    string,
    { index: number; timer: ReturnType<typeof setTimeout> }
  >();
  private readonly tasks = new Set<Promise<void>>();
  private readonly callback: Review["callback"];
  private readonly timeoutMs: number;

  constructor(
    review: Review,
    private readonly lifetime: AbortSignal,
    private readonly send: (
      event: Record<string, unknown>,
      signal: AbortSignal,
    ) => Promise<void>,
    private readonly fail: (error: RimeError) => void,
  ) {
    this.callback = review.callback;
    this.timeoutMs = review.timeoutMs ?? 1000;
  }

  request(value: ReviewRequest): void {
    if (
      this.lifetime.aborted ||
      this.active.has(value.reviewId) ||
      this.seen.has(value.reviewId)
    )
      return;
    if (this.active.size >= 32)
      throw new RimeResourceLimitError("Too many concurrent review callbacks");
    this.seen.add(value.reviewId);
    if (this.seen.size > 256)
      this.seen.delete(this.seen.values().next().value!);
    const limit = this.stopped.get(value.responseId);
    if (limit !== undefined && value.index > limit.index) return;
    const request = Object.freeze({
      reviewId: value.reviewId,
      responseId: value.responseId,
      itemId: value.itemId,
      index: value.index,
      text: value.text,
    });
    const stop = new AbortController();
    this.active.set(request.reviewId, { request, stop });
    // Schedule outside the socket reader, including callbacks that throw before returning.
    const task = Promise.resolve().then(() => this.run(request, stop.signal));
    this.tasks.add(task);
    task
      .finally(() => {
        this.active.delete(request.reviewId);
        this.tasks.delete(task);
      })
      .catch(() => {});
  }

  private async run(
    request: ReviewRequest,
    stopped: AbortSignal,
  ): Promise<void> {
    const deadline = scope(
      [this.lifetime, stopped],
      this.timeoutMs / 1000,
      new RimeCancelledError("Review callback expired"),
    );
    try {
      deadline.signal.throwIfAborted();
      let decision: ReviewDecision;
      try {
        decision = reviewDecision(
          await this.callback(request, deadline.signal),
        );
      } catch {
        if (deadline.signal.aborted) return;
        // Try to stop this chunk before reporting the callback failure.
        try {
          await this.result(request, { action: "stop" }, deadline.signal);
        } finally {
          this.fail(
            new RimeStreamError(
              "Review callback failed or returned an invalid decision",
            ),
          );
        }
        return;
      }
      if (deadline.signal.aborted) return;
      await this.result(request, decision, deadline.signal);
    } catch (error) {
      if (!deadline.signal.aborted)
        this.fail(
          error instanceof RimeError
            ? error
            : new RimeStreamError("Review result submission failed"),
        );
    } finally {
      deadline.dispose();
    }
  }

  private async result(
    request: ReviewRequest,
    decision: ReviewDecision,
    signal: AbortSignal,
  ): Promise<void> {
    // There is no successful acknowledgment. A submitted result is never replayed.
    await this.send(
      {
        type: "prism.review.result",
        event_id: `evt_${randomUUID().replaceAll("-", "")}`,
        review_id: request.reviewId,
        ...decision,
      },
      signal,
    );
    if (decision.action === "allow") return;
    if (!this.stopped.has(request.responseId) && this.stopped.size >= 128)
      throw new RimeResourceLimitError("Too many retained review replies");
    const limit = Math.min(
      request.index,
      this.stopped.get(request.responseId)?.index ?? Infinity,
    );
    clearTimeout(this.stopped.get(request.responseId)?.timer);
    const timer = setTimeout(
      () => this.stopped.delete(request.responseId),
      this.timeoutMs,
    );
    this.stopped.set(request.responseId, { index: limit, timer });
    for (const active of this.active.values())
      if (
        active.request.responseId === request.responseId &&
        active.request.index > limit
      )
        active.stop.abort(
          new RimeStreamError("A preceding review ended this reply"),
        );
  }

  end(responseId: string): void {
    clearTimeout(this.stopped.get(responseId)?.timer);
    this.stopped.delete(responseId);
    for (const active of this.active.values())
      if (active.request.responseId === responseId)
        active.stop.abort(new RimeStreamError("Review reply ended"));
  }

  async close(): Promise<void> {
    await Promise.allSettled(this.tasks);
    this.seen.clear();
    for (const value of this.stopped.values()) clearTimeout(value.timer);
    this.stopped.clear();
  }
}
