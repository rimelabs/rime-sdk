/** Application-owned tool work. The SDK runner does not execute tools. */
import {
  RimeInputError,
  RimeRealtimeError,
  type RealtimeSession,
  type ResponseRef,
  type SessionEvent,
  type ToolCall,
} from "@rimelabs/sdk";

interface ToolRound {
  calls: ToolCall[];
  abandoned: boolean;
  continuation?: AbortController;
}

export class ManualTools {
  private rounds = new Map<string, ToolRound>();
  private pending = new Set<Promise<void>>();
  private epoch = 0;
  constructor(
    private session: RealtimeSession,
    private controller: AbortController,
    private execute?: (call: ToolCall, signal: AbortSignal) => Promise<string>,
  ) {}
  onEvent({ payload }: SessionEvent) {
    if (
      payload.kind === "speech.started" ||
      payload.kind === "response.started"
    ) {
      this.epoch++;
      if (payload.kind === "response.started") {
        if (this.rounds.size >= 128)
          throw new Error("Too many pending application tool rounds");
        this.rounds.set(payload.response.responseId, {
          calls: [],
          abandoned: false,
        });
      }
    } else if (payload.kind === "response.abandoned") {
      const state = this.rounds.get(payload.response.responseId);
      if (state) {
        state.abandoned = true;
        state.continuation?.abort();
      }
    } else if (payload.kind === "tool.call") {
      if (
        [...this.rounds.values()].reduce(
          (n, state) => n + state.calls.length,
          0,
        ) >= 128
      )
        throw new Error("Too many pending application tool calls");
      this.rounds.get(payload.call.responseId)!.calls.push(payload);
    } else if (payload.kind === "response.ended") {
      const id = payload.response.responseId;
      const state = this.rounds.get(id);
      if (
        state?.calls.length &&
        !state.abandoned &&
        payload.status === "completed"
      ) {
        if (this.pending.size >= 64)
          throw new Error("Too many pending application tool tasks");
        const task = this.finish(payload.response, state, this.epoch);
        this.pending.add(task);
        void task.then(
          () => {
            this.pending.delete(task);
            this.rounds.delete(id);
          },
          (error) => {
            this.pending.delete(task);
            this.rounds.delete(id);
            this.controller.abort(error);
          },
        );
      } else this.rounds.delete(id);
    }
  }
  private async finish(response: ResponseRef, state: ToolRound, epoch: number) {
    const options = { signal: this.controller.signal };
    for (const call of state.calls) {
      if (state.abandoned) return;
      if (!this.execute) throw new Error("The example has no tool handler");
      const result = await this.execute(call, options.signal);
      await this.session.submitToolResult(call.call, result, options);
    }
    if (epoch === this.epoch && !state.abandoned) {
      state.continuation = new AbortController();
      try {
        await this.session.continueReply(response, {
          signal: AbortSignal.any([
            this.controller.signal,
            state.continuation.signal,
          ]),
        });
      } catch (error) {
        if (state.abandoned && error === state.continuation.signal.reason)
          return;
        if (
          !(error instanceof RimeInputError && epoch !== this.epoch) &&
          (!(error instanceof RimeRealtimeError) ||
            error.fault.scope !== "event" ||
            error.fault.code !== "tool_continuation_unavailable")
        )
          throw error;
      }
    }
  }
  async settled() {
    await Promise.allSettled(this.pending);
  }
}
