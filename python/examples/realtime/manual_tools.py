"""Application-owned tool execution. The SDK runner does not execute tools."""

import asyncio
from dataclasses import dataclass, field

from rimelabs_sdk import RimeInputError
from rimelabs_sdk import realtime as r


@dataclass
class ToolRound:
    calls: list = field(default_factory=list)
    abandoned: bool = False
    continuation: asyncio.Task | None = None


class ManualTools:
    def __init__(self, session, tasks, execute):
        self.session, self.tasks, self.execute = session, tasks, execute
        self.rounds = {}
        self.pending = set()
        self.epoch = 0

    def on_event(self, event):
        payload = event.payload
        if isinstance(payload, (r.SpeechStarted, r.ResponseStarted)):
            self.epoch += 1
            if isinstance(payload, r.ResponseStarted):
                if len(self.rounds) >= 128:
                    raise RuntimeError("Too many pending application tool rounds")
                self.rounds[payload.response.response_id] = ToolRound()
        elif isinstance(payload, r.ResponseAbandoned):
            if state := self.rounds.get(payload.response.response_id):
                state.abandoned = True
                if state.continuation:
                    state.continuation.cancel()
        elif isinstance(payload, r.ToolCall):
            if sum(len(state.calls) for state in self.rounds.values()) >= 128:
                raise RuntimeError("Too many pending application tool calls")
            self.rounds[payload.call.response_id].calls.append(payload)
        elif isinstance(payload, r.ResponseEnded):
            response_id = payload.response.response_id
            state = self.rounds.get(response_id)
            if state and state.calls and not state.abandoned and payload.status == "completed":
                if len(self.pending) >= 64:
                    raise RuntimeError("Too many pending application tool tasks")
                task = self.tasks.create_task(self.finish(payload.response, state, self.epoch))
                self.pending.add(task)
                task.add_done_callback(self.pending.discard)
                task.add_done_callback(lambda _: self.rounds.pop(response_id, None))
            else:
                self.rounds.pop(response_id, None)

    async def finish(self, response, state, epoch):
        for call in state.calls:
            if state.abandoned:
                return
            if self.execute is None:
                raise RuntimeError("The example has no tool handler")
            await self.session.submit_tool_result(call.call, await self.execute(call))
        if epoch == self.epoch and not state.abandoned:
            try:
                state.continuation = asyncio.create_task(self.session.continue_reply(response))
                await state.continuation
            except asyncio.CancelledError:
                # Abandonment cancels the continuation; caller cancellation must propagate.
                if not state.abandoned or asyncio.current_task().cancelling():
                    raise
            except RimeInputError:
                if epoch == self.epoch and not state.abandoned:
                    raise
            except r.RimeRealtimeError as error:
                if (
                    error.fault.scope != "event"
                    or error.fault.code != "tool_continuation_unavailable"
                ):
                    raise

    def cancel(self):
        for task in self.pending:
            task.cancel()
