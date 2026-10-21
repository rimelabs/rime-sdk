/** Application-owned tool work. The SDK runner does not execute tools. */
import {
  RimeInputError,
  RimeRealtimeError,
  type RealtimeSession,
  type ResponseRef,
  type SessionEvent,
  type ToolCall,
} from "@rimelabs/sdk";

export class ManualTools {
  private calls = new Map<string, ToolCall[]>();
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
    )
      this.epoch++;
    else if (payload.kind === "tool.call") {
      if (
        [...this.calls.values()].reduce((n, calls) => n + calls.length, 0) >=
        128
      )
        throw new Error("Too many pending application tool calls");
      const id = payload.call.responseId;
      const calls = this.calls.get(id) ?? [];
      calls.push(payload);
      this.calls.set(id, calls);
    } else if (payload.kind === "response.ended") {
      const calls = this.calls.get(payload.response.responseId) ?? [];
      this.calls.delete(payload.response.responseId);
      if (calls.length && payload.status === "completed") {
        if (this.pending.size >= 64)
          throw new Error("Too many pending application tool tasks");
        const task = this.finish(payload.response, calls, this.epoch);
        this.pending.add(task);
        void task.then(
          () => this.pending.delete(task),
          (error) => {
            this.pending.delete(task);
            this.controller.abort(error);
          },
        );
      }
    }
  }
  private async finish(
    response: ResponseRef,
    calls: ToolCall[],
    epoch: number,
  ) {
    const options = { signal: this.controller.signal };
    for (const call of calls) {
      if (!this.execute) throw new Error("The example has no tool handler");
      const result = await this.execute(call, options.signal);
      await this.session.submitToolResult(call.call, result, options);
    }
    if (epoch === this.epoch) {
      try {
        await this.session.continueReply(response, options);
      } catch (error) {
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
