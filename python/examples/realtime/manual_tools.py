"""Application-owned tool execution. The SDK runner does not execute tools."""

from rimelabs_sdk import RimeInputError
from rimelabs_sdk import realtime as r


class ManualTools:
    def __init__(self, session, tasks, execute):
        self.session, self.tasks, self.execute = session, tasks, execute
        self.calls = {}
        self.pending = set()
        self.epoch = 0

    def on_event(self, event):
        payload = event.payload
        if isinstance(payload, (r.SpeechStarted, r.ResponseStarted)):
            self.epoch += 1
        elif isinstance(payload, r.ToolCall):
            if sum(map(len, self.calls.values())) >= 128:
                raise RuntimeError("Too many pending application tool calls")
            self.calls.setdefault(payload.call.response_id, []).append(payload)
        elif isinstance(payload, r.ResponseEnded):
            calls = self.calls.pop(payload.response.response_id, [])
            if calls and payload.status == "completed":
                if len(self.pending) >= 64:
                    raise RuntimeError("Too many pending application tool tasks")
                task = self.tasks.create_task(self.finish(payload.response, calls, self.epoch))
                self.pending.add(task)
                task.add_done_callback(self.pending.discard)

    async def finish(self, response, calls, epoch):
        for call in calls:
            if self.execute is None:
                raise RuntimeError("The example has no tool handler")
            await self.session.submit_tool_result(call.call, await self.execute(call))
        if epoch == self.epoch:
            try:
                await self.session.continue_reply(response)
            except RimeInputError:
                if epoch == self.epoch:
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
