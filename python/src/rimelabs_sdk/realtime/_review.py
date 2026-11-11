"""Session-owned review callbacks, independent of response handles and event consumers."""

from __future__ import annotations

import asyncio
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable

from .._errors import RimeError, RimeInputError, RimeResourceLimitError, RimeStreamError
from . import _types as t


class Reviews:
    """Bound concurrent policy work and submit each retained review at most once."""

    def __init__(
        self,
        policy: t.Review,
        send: Callable[[dict[str, object], float], Awaitable[None]],
        fail: Callable[[RimeError], None],
    ):
        self._policy = policy
        self._closed = False
        self._send = send
        self._fail = fail
        self._tasks: dict[str, tuple[t.ReviewRequest, asyncio.Task[None]]] = {}
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._stopped: OrderedDict[str, int] = OrderedDict()

    def receive(self, request: t.ReviewRequest) -> None:
        if self._closed or request.review_id in self._seen or request.review_id in self._tasks:
            return
        cutoff = self._stopped.get(request.response_id)
        if cutoff is not None and request.index > cutoff:
            return
        if len(self._tasks) >= 128:
            raise RimeResourceLimitError("Too many pending speech reviews")
        self._seen[request.review_id] = None
        if len(self._seen) > 1024:
            self._seen.popitem(last=False)
        task = asyncio.create_task(self._run(request), name="rime:speech-review")
        self._tasks[request.review_id] = (request, task)

        def completed(done: asyncio.Task[None]) -> None:
            self._tasks.pop(request.review_id, None)
            if not done.cancelled():
                done.exception()  # The event queue can be closed after transport failure.

        task.add_done_callback(completed)

    def end(self, response_id: str, index: int = -1) -> None:
        cutoff = min(index, self._stopped.get(response_id, index))
        self._stopped[response_id] = cutoff
        if len(self._stopped) > 128:
            self._stopped.popitem(last=False)
        current = asyncio.current_task()
        for request, task in tuple(self._tasks.values()):
            if (
                request.response_id == response_id
                and request.index > cutoff
                and task is not current
            ):
                task.cancel()

    async def _run(self, request: t.ReviewRequest) -> None:
        failed = False
        deadline = asyncio.get_running_loop().time() + self._policy.timeout_ms / 1000
        try:
            timeout = asyncio.timeout_at(deadline)
            try:
                async with timeout:
                    decision = await self._policy.callback(request)
                if not isinstance(decision, (t.Allow, t.Replace, t.Stop)):
                    raise RimeInputError("Review callback must return Allow, Replace, or Stop")
                if isinstance(decision, t.Replace):
                    if not isinstance(decision.text, str) or not decision.text.strip(
                        t._FILLER_WHITESPACE
                    ):
                        raise RimeInputError("Replacement text must be nonblank")
                    decision.text.encode("utf-8")
            except Exception:  # noqa: BLE001 -- caller callbacks may raise any exception
                if timeout.expired():
                    return  # The server applies its timeout policy.
                decision = t.Stop()
                failed = True
            if (
                self._closed
                or asyncio.get_running_loop().time() >= deadline
                or request.index > self._stopped.get(request.response_id, request.index)
            ):
                return
            event: dict[str, object] = {
                "type": "prism.review.result",
                "event_id": "evt_" + uuid.uuid4().hex,
                "review_id": request.review_id,
                "action": "allow"
                if isinstance(decision, t.Allow)
                else "replace"
                if isinstance(decision, t.Replace)
                else "stop",
            }
            if isinstance(decision, t.Replace):
                event["text"] = decision.text
            await self._send(event, deadline)
            if not isinstance(decision, t.Allow):
                self.end(request.response_id, request.index)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- writer failures terminate the session
            self._fail(RimeStreamError("Speech review result could not be submitted"))
        finally:
            if failed:
                # An expired or cancelled queued Stop does not erase the policy
                # failure. The server still owns its configured timeout action.
                self._fail(RimeStreamError("Speech review callback failed; stop attempted"))

    def cancel(self) -> None:
        self._closed = True
        for _, task in tuple(self._tasks.values()):
            task.cancel()

    async def close(self) -> None:
        tasks = [task for _, task in self._tasks.values()]
        self.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._seen.clear()
        self._stopped.clear()
