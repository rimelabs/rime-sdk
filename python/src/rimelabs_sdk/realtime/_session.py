# SPDX-License-Identifier: Apache-2.0
"""Prism session state and request matching, independent of any audio player.

Protocol rules are adapted from rimelabs/sglang-omni clients/prism_realtime at
bc8a500249536f369d20bc094baa9169e5986758 (Apache-2.0).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import uuid
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

from websockets.exceptions import ConnectionClosed

from .._errors import (
    RimeAudioFormatError,
    RimeError,
    RimeInputError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
)
from . import _protocol as p
from . import _types as t

_ResultT = TypeVar("_ResultT", bound=p.Acknowledgment)

_NOT_READY = {"typed_turn_busy", "instructions_busy", "proactive_unavailable"}
_LIMIT = 128


@dataclass
class _Response:
    ref: t.ResponseRef
    done: asyncio.Event = field(default_factory=asyncio.Event)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    calls: dict[str, str | None] = field(default_factory=dict)
    outputs: set[t.OutputRef] = field(default_factory=set)
    token: str | None = None
    continued: bool = False
    abandoned: bool = False
    cancel_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _Pending:
    request: p.Request[p.Acknowledgment]
    future: asyncio.Future[p.Acknowledgment]
    target: str | None = None
    item_id: str | None = None


class RealtimeSession:
    """One conversation. Consume ``events`` concurrently with session operations.

    The session retains the last 128 responses, without retaining audio. References
    to older responses expire. A slow event consumer closes the session rather
    than dropping audio or blocking control messages.
    """

    def __init__(self, socket: Any, timeouts: t.RealtimeTimeouts):
        self._loop = asyncio.get_running_loop()
        self._pid = os.getpid()
        self._socket = socket
        self._timeouts = timeouts
        self._info: t.SessionInfo | None = None
        self._created: asyncio.Future[p.SessionView] = asyncio.get_running_loop().create_future()
        self._queue: asyncio.Queue[t.SessionEvent | None] = asyncio.Queue(256)
        self._failure: RimeError | None = None
        self._closed = False
        self._consuming = False
        self._ready = asyncio.Event()
        self._ready_epoch = 0
        self._admission = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._audio_lock = asyncio.Lock()
        self._clear_lock = asyncio.Lock()
        self._truncate_lock = asyncio.Lock()
        self._tool_lock = asyncio.Lock()
        self._pending: dict[str, _Pending] = {}
        self._responses: OrderedDict[str, _Response] = OrderedDict()
        self._latest: str | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._audio_format: t.PCMFormat | None = None
        self._resample_state: Any = None
        self._reader = asyncio.create_task(self._read(), name="rime:realtime-reader")
        self._close_task: asyncio.Task[None] | None = None

    @property
    def info(self) -> t.SessionInfo:
        if self._info is None:
            raise RimeInputError("The session has not initialized")
        return self._info

    @property
    def events(self) -> AsyncIterator[t.SessionEvent]:
        return self._events()

    async def _events(self) -> AsyncIterator[t.SessionEvent]:
        if self._consuming:
            raise RimeInputError("Only one consumer may read session events")
        self._consuming = True
        try:
            while True:
                if self._failure:
                    raise self._failure
                if self._closed and self._queue.empty():
                    return
                event = await self._queue.get()
                if event is None:
                    if self._failure:
                        raise self._failure
                    return
                yield event
        finally:
            self._consuming = False

    async def _initialize(self, settings: p.SessionSettings) -> None:
        try:
            created = await asyncio.wait_for(
                asyncio.shield(self._created), self._timeouts.connect_s
            )
            session = await self._request(p.UPDATE, {"session": settings})
            view = session.session
            self._info = t.SessionInfo(
                session_id=created.id,
                model="prism",
                voice=view.voice,
                interrupt_on_speech=view.interrupt_on_speech
                if view.interrupt_on_speech is not None
                else settings["turn_detection"]["interrupt_response"],
                tool_result_timeout_s=view.tool_result_timeout_s,
                tool_continuation_timeout_s=view.tool_continuation_timeout_s,
            )
        except TimeoutError:
            raise RimeTimeoutError("No session.created event arrived") from None

    def _check(self) -> None:
        if os.getpid() != self._pid or asyncio.get_running_loop() is not self._loop:
            raise RimeInputError("A realtime session belongs to one process and event loop")
        if self._failure:
            raise self._failure
        if self._closed:
            raise RimeInputError("The realtime session is closed")

    def _response(self, ref: t.ResponseRef | t.ToolCallRef) -> _Response:
        self._check()
        if ref.session_id != self.info.session_id:
            raise RimeInputError("Reference belongs to another session")
        response = self._responses.get(ref.response_id)
        if response is None:
            raise RimeInputError("Unknown or expired response reference")
        if isinstance(ref, t.ToolCallRef) and ref.call_id not in response.calls:
            raise RimeInputError("Unknown tool call reference")
        return response

    async def _send(
        self,
        event: dict[str, object],
        abandoned: asyncio.Event | None = None,
        *,
        submitted: asyncio.Event | None = None,
    ) -> None:
        self._check()
        try:
            async with asyncio.timeout(self._timeouts.request_s):
                async with self._write_lock:
                    self._check()
                    if abandoned is not None and abandoned.is_set():
                        raise asyncio.CancelledError
                    encoded = json.dumps(event, allow_nan=False)
                    if submitted is not None:
                        submitted.set()
                    await self._socket.send(encoded)
        except TimeoutError:
            error = RimeTimeoutError("Realtime write timed out; outcome unknown")
            self._fail(error)
            raise error from None
        except (OSError, ConnectionError, ConnectionClosed):
            failure = RimeStreamError("Realtime write failed; outcome unknown")
            self._fail(failure)
            raise failure from None

    async def _request(
        self,
        request: p.Request[_ResultT],
        body: dict[str, object],
        target: str | None = None,
        *,
        continuation: _Response | None = None,
    ) -> _ResultT:
        kind = request.kind
        self._check()
        if len(self._tasks) >= _LIMIT:
            raise RimeResourceLimitError("Too many pending realtime operations")
        # Reserve the turn only after local admission checks pass, and before
        # yielding so concurrent callers cannot reserve the same turn.
        if kind == "response.create":
            if continuation is not None:
                continuation.continued = True
            else:
                self._ready.clear()
        abandoned = asyncio.Event()
        task = asyncio.create_task(
            self._run_request(request, body, target, abandoned, self._ready_epoch, continuation)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        try:
            return request.result(await asyncio.shield(task))
        except asyncio.CancelledError:
            abandoned.set()
            if kind != "response.create":
                # Keep semantic-ack locks until the request has a known outcome.
                # Clear/truncate acknowledgments do not echo a request ID.
                try:
                    await asyncio.shield(task)
                except (RimeError, asyncio.CancelledError):
                    pass
            # A request may already have been accepted. Its task keeps matching the
            # answer and cancels a late generation, without replaying the request.
            if (
                task.done()
                and not task.cancelled()
                and task.exception() is None
                and kind == "response.create"
            ):
                accepted = task.result()
                if isinstance(accepted, t.ResponseStarted):
                    self._abandon_response(accepted.response)
            raise

    def _abandon_response(self, response: t.ResponseRef) -> None:
        state = self._responses[response.response_id]
        if state.abandoned or self._closed:
            return
        state.abandoned = True
        self._publish(t.ResponseAbandoned(response=response))
        self._background(self.cancel(response))

    def _background(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())

    async def _run_request(
        self,
        request: p.Request[p.Acknowledgment],
        body: dict[str, object],
        target: str | None,
        abandoned: asyncio.Event,
        ready_epoch: int,
        continuation: _Response | None,
    ) -> p.Acknowledgment:
        kind = request.kind
        event_id = "evt_" + uuid.uuid4().hex
        future: asyncio.Future[p.Acknowledgment] = asyncio.get_running_loop().create_future()
        self._pending[event_id] = _Pending(request, future, target)
        submitted = asyncio.Event()
        try:
            async with asyncio.timeout(self._timeouts.request_s):
                await self._send(
                    {"type": kind, "event_id": event_id, **body}, abandoned, submitted=submitted
                )
                result = await future
            if abandoned.is_set() and isinstance(result, t.ResponseStarted):
                self._abandon_response(result.response)
            return result
        except t.RimeRealtimeError as refusal:
            if (
                continuation is not None
                and refusal.fault.scope == "event"
                and refusal.fault.code == "tool_continuation_not_ready"
            ):
                # A correlated zero-effect refusal releases even an abandoned request.
                continuation.continued = False
            raise
        except TimeoutError:
            error = RimeTimeoutError(
                f"{kind}: no acknowledgment; outcome unknown", request_id=event_id
            )
            self._fail(error)
            raise error from None
        finally:
            # A send that started can have an unknown outcome. Release only
            # unsent reservations, without overriding newer server readiness.
            if kind == "response.create" and not submitted.is_set():
                if continuation is not None:
                    continuation.continued = False
                elif ready_epoch == self._ready_epoch:
                    self._ready.set()
            self._pending.pop(event_id, None)
            if future.done() and not future.cancelled():
                future.exception()

    @staticmethod
    def _text(value: str) -> None:
        if not isinstance(value, str) or not value.strip() or len(value) > 4000:
            raise RimeInputError("Text must be nonblank and at most 4000 characters")

    async def _create(
        self, body: dict[str, object], request: p.Request[t.ResponseStarted] = p.CREATE
    ) -> t.ResponseRef:
        deadline = self._loop.time() + self._timeouts.ready_s
        try:
            async with asyncio.timeout_at(deadline):
                await self._admission.acquire()
        except TimeoutError:
            raise t.RealtimeAdmissionTimeout("Prism admission lock exceeded ready_s") from None
        try:
            while True:
                self._check()
                try:
                    async with asyncio.timeout_at(deadline):
                        await self._ready.wait()
                except TimeoutError:
                    raise t.RealtimeAdmissionTimeout(
                        "Prism did not admit the turn before ready_s expired"
                    ) from None
                self._check()
                try:
                    # Once submitted, request_s governs the acknowledgment. An
                    # admission timeout must never hide an unknown wire outcome.
                    return (await self._request(request, {"response": body})).response
                except t.RimeRealtimeError as error:
                    if error.fault.scope != "event" or error.fault.code not in _NOT_READY:
                        raise
        finally:
            self._admission.release()

    async def send_text(self, text: str) -> t.ResponseRef:
        """Start a typed user turn. Wait for readiness and return at response creation."""
        self._text(text)
        return await self._create(
            {"prsm_input_text": text, "metadata": {"prsm_cause": "user_text"}}, p.TEXT
        )

    async def request_reply(
        self, *, instruction: str | None = None, tool_call: t.ToolCallRef | None = None
    ) -> t.ResponseRef:
        body: dict[str, object] = {"metadata": {"prsm_cause": "proactive"}}
        if instruction is not None:
            self._text(instruction)
            body["prsm_instruction"] = instruction
        if tool_call is not None:
            response = self._response(tool_call)
            if response.calls[tool_call.call_id] is None:
                raise RimeInputError("Record the tool result before requesting its report")
            body["prsm_call_id"] = tool_call.call_id
        return await self._create(body)

    async def add_message(self, role: Literal["user", "assistant"], text: str) -> t.ItemRef:
        if role not in ("user", "assistant"):
            raise RimeInputError("History role must be user or assistant")
        self._text(text)
        event = await self._request(
            p.ITEM, {"item": {"type": "message", "role": role, "text": text}}
        )
        return t.ItemRef(session_id=self.info.session_id, item_id=event.item_id)

    async def submit_tool_result(self, call: t.ToolCallRef, output: str) -> None:
        self._response(call)
        if len(self._tasks) >= _LIMIT:
            raise RimeResourceLimitError("Too many pending realtime operations")
        task = asyncio.create_task(self._submit_tool_result(call, output))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        await asyncio.shield(task)

    async def _submit_tool_result(self, call: t.ToolCallRef, output: str) -> None:
        async with self._tool_lock:
            response = self._response(call)
            if not isinstance(output, str):
                raise RimeInputError("Tool output must be a string")
            digest = hashlib.sha256(output.encode()).hexdigest()
            recorded = response.calls[call.call_id]
            if recorded is not None:
                if recorded != digest:
                    raise RimeInputError("A recorded tool result cannot change")
                return
            await self._request(
                p.TOOL_RESULT,
                {
                    "item": {
                        "type": "function_call_output",
                        "call_id": call.call_id,
                        "output": output,
                    }
                },
                call.call_id,
            )
            response.calls[call.call_id] = digest
            response.changed.set()

    async def continue_reply(self, parent: t.ResponseRef) -> t.ResponseRef:
        response = self._response(parent)
        try:
            async with asyncio.timeout(self._timeouts.ready_s):
                while True:
                    self._check()
                    if parent.response_id != self._latest or response.continued:
                        raise RimeInputError("The tool round is superseded or already continued")
                    if (
                        response.done.is_set()
                        and response.calls
                        and all(v is not None for v in response.calls.values())
                    ):
                        break
                    if response.done.is_set() and not response.calls:
                        raise RimeInputError("The response has no tool calls")
                    response.changed.clear()
                    await response.changed.wait()
        except TimeoutError:
            raise t.RealtimeAdmissionTimeout(
                "Tool results or parent completion did not arrive before ready_s expired"
            ) from None
        return (
            await self._request(
                p.CREATE,
                {
                    "response": {
                        "metadata": {
                            "prsm_cause": "tool_continuation",
                            "prsm_parent_response_id": parent.response_id,
                        }
                    }
                },
                continuation=response,
            )
        ).response

    async def cancel(self, response: t.ResponseRef) -> None:
        """Request a stop if needed; return when the response is terminal.

        A response can finish before cancellation takes effect. Read its
        ResponseEnded event for the actual status; returning does not imply a
        cancelled status. A refusal received before the terminal event raises.
        """
        state = self._response(response)
        async with state.cancel_lock:
            if not state.done.is_set():
                await self._request(
                    p.CANCEL, {"response_id": response.response_id}, response.response_id
                )

    async def clear_audio(self) -> None:
        async with self._audio_lock, self._clear_lock:
            await self._request(p.CLEAR, {})

    async def send_audio(self, chunk: t.AudioChunk) -> None:
        """Send one bounded chunk with transport backpressure.

        Format changes reset the resampler. Cancellation after submission starts
        closes the session because some of the chunk may already be buffered.
        """
        import audioop

        async with self._audio_lock:
            self._check()
            if len(chunk.data) > 192000:
                raise RimeAudioFormatError("Send audio in chunks of at most 192000 bytes")
            previous_format, previous_state = self._audio_format, self._resample_state
            submitted = asyncio.Event()
            if chunk.format != self._audio_format:
                self._resample_state = None
                self._audio_format = chunk.format
            data = chunk.data
            if chunk.format.channels == 2:
                data = audioop.tomono(data, 2, 0.5, 0.5)
            if chunk.format.sample_rate != 16000:
                data, self._resample_state = audioop.ratecv(
                    data, 2, 1, chunk.format.sample_rate, 16000, self._resample_state
                )
            # 40 ms per wire append; no unbounded queue of encoded microphone data.
            try:
                for offset in range(0, len(data), 1280):
                    await self._send(
                        {
                            "type": "input_audio_buffer.append",
                            "audio": base64.b64encode(data[offset : offset + 1280]).decode("ascii"),
                        },
                        submitted=submitted,
                    )
            except asyncio.CancelledError:
                if submitted.is_set():
                    self._fail(
                        RimeStreamError("Realtime audio submission cancelled; outcome unknown")
                    )
                else:
                    self._audio_format, self._resample_state = previous_format, previous_state
                raise

    async def report_playback(self, report: t.PlaybackReport) -> None:
        if isinstance(report, t.PlaybackInterrupted):
            state = self._response(report.output.response)
            if report.output not in state.outputs:
                raise RimeInputError("Unknown output reference")
            async with self._truncate_lock:
                await self._request(
                    p.TRUNCATE,
                    {
                        "item_id": report.output.item_id,
                        "content_index": report.output.content_index,
                        "audio_end_ms": report.audio_end_ms,
                    },
                    report.output.item_id,
                )
        else:
            state = self._response(report.response)
            try:
                await asyncio.wait_for(state.done.wait(), self._timeouts.ready_s)
            except TimeoutError:
                raise RimeTimeoutError(
                    "Response did not finish before playback report deadline"
                ) from None
            self._check()
            if state.token:
                await self._send(
                    {
                        "type": "prsm.playback.drained",
                        "response_id": report.response.response_id,
                        "drain_token": state.token,
                        "played_ms": report.played_ms,
                        "audible_tail_ms": report.audible_tail_ms,
                    }
                )

    def _settle(self, key: str | None, result: p.Acknowledgment) -> None:
        pending = self._pending.get(key or "")
        if pending is not None:
            pending.request.result(result)
            if not pending.future.done():
                pending.future.set_result(result)

    def _reject(self, key: str | None, error: RimeError) -> None:
        pending = self._pending.get(key or "")
        if pending is not None and not pending.future.done():
            pending.future.set_exception(error)

    def _fail(self, error: RimeError) -> None:
        if self._closed:
            return
        self._failure = error
        self._closed = True
        self._ready.set()
        for state in self._responses.values():
            state.done.set()
            state.changed.set()
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.set_exception(error)
        if not self._created.done():
            self._created.set_exception(error)
        if self._queue.full():
            self._queue.get_nowait()
        self._queue.put_nowait(None)
        self._background(self._socket.close())

    async def _read(self) -> None:
        try:
            async for raw in self._socket:
                event = p.decode(raw, self._created.result().id if self._created.done() else None)
                if event is not None:
                    self._dispatch(event)
            if not self._closed:
                self._fail(
                    RimeStreamError("Realtime connection closed; reconnect is not automatic")
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 -- reader failures terminate the public session
            self._fail(
                error
                if isinstance(error, RimeError)
                else RimeStreamError("Invalid event or lost realtime connection")
            )
        finally:
            await self._socket.close()

    def _dispatch(self, event: p.ServerEvent) -> None:
        payload, echo = event.payload, event.request_id
        # Reject mismatched echoed acknowledgments before changing session state.
        if isinstance(
            payload, (p.SessionUpdated, p.ItemCreated, p.ToolResultCreated, t.ResponseStarted)
        ):
            pending = self._pending.get(echo or "")
            if pending and not (
                isinstance(payload, p.ItemCreated) and pending.request.allows_item_created
            ):
                pending.request.result(payload)
            if (
                pending
                and isinstance(payload, p.ToolResultCreated)
                and pending.target != payload.call_id
            ):
                raise RimeStreamError("Tool result acknowledgment names another call")
        if isinstance(payload, p.SessionCreated):
            if not self._created.done():
                self._created.set_result(payload.session)
            return
        if isinstance(payload, (p.SessionUpdated, p.ToolResultCreated)):
            self._settle(echo, payload)
            return
        if isinstance(payload, p.ItemCreated):
            pending = self._pending.get(echo or "")
            if pending and pending.request.allows_item_created:
                pending.item_id = payload.item_id
            else:
                self._settle(echo, payload)
            return
        if isinstance(payload, p.AudioCleared):
            for key, pending in self._pending.items():
                if pending.request.kind == "input_audio_buffer.clear":
                    self._resample_state = None
                    self._settle(key, payload)
                    break
            return
        if isinstance(payload, p.ItemTruncated):
            for key, pending in self._pending.items():
                if (
                    pending.request.kind == "conversation.item.truncate"
                    and pending.target == payload.item_id
                    and payload.content_index == 0
                ):
                    self._settle(key, payload)
            return
        if isinstance(payload, t.InputReady):
            self._ready_epoch += 1
            self._ready.set()
        elif isinstance(payload, t.SpeechStarted):
            self._ready_epoch += 1
            self._ready.clear()
            for state in self._responses.values():
                state.changed.set()
            self._latest = None
        elif isinstance(payload, t.ResponseStarted):
            ref = payload.response
            if ref.response_id in self._responses:
                raise RimeStreamError("Duplicate response.created")
            self._responses[ref.response_id] = _Response(ref)
            for state in self._responses.values():
                state.changed.set()
            self._latest = ref.response_id
            self._ready_epoch += 1
            while len(self._responses) > _LIMIT:
                oldest = next(iter(self._responses.values()))
                if not oldest.done.is_set():
                    raise RimeResourceLimitError("Too many retained active responses")
                self._responses.popitem(last=False)
            self._ready.clear()
            self._settle(echo, payload)
        elif isinstance(payload, p.ResponseEnded):
            state = self._responses[payload.response.response_id]
            if state.done.is_set():
                return
            state.token = payload.drain_token
            state.done.set()
            state.changed.set()
            for key, pending in self._pending.items():
                if (
                    pending.request.kind == "response.cancel"
                    and pending.target == state.ref.response_id
                ):
                    self._settle(key, payload)
            self._publish(
                t.ResponseEnded(response=state.ref, status=payload.status, reason=payload.reason),
                event_id=event.event_id,
                request_id=echo,
            )
            return
        elif isinstance(payload, t.ToolCall):
            state = self._responses[payload.call.response_id]
            call_id = payload.call.call_id
            if call_id in state.calls:
                return
            if any(call_id in other.calls for other in self._responses.values()):
                raise RimeStreamError("Tool call ID reused by another response")
            if len(state.calls) >= 128:
                raise RimeResourceLimitError("Too many tool calls in one response")
            state.calls[call_id] = None
        elif isinstance(payload, t.MessageStarted):
            state = self._responses[payload.output.response.response_id]
            if len(state.outputs) >= 128:
                raise RimeResourceLimitError("Too many messages in one response")
            state.outputs.add(payload.output)
        elif isinstance(payload, (t.TextDelta, t.TextDone, t.AudioDelta, t.AudioDone)):
            if payload.output.response.response_id not in self._responses:
                raise RimeStreamError("Event refers to an unknown response")
        elif isinstance(payload, t.FaultEvent):
            fault = payload.error
            error = t.RimeRealtimeError(fault)
            if fault.code in _NOT_READY:
                self._ready.clear()
            self._reject(fault.request_id, error)
            if fault.code == "typed_turn_superseded":
                for key, pending in self._pending.items():
                    if pending.item_id == fault.item_id and fault.item_id:
                        self._reject(key, error)
            if fault.scope == "session":
                self._fail(error)
                return
        self._publish(payload, event_id=event.event_id, request_id=echo)

    def _publish(
        self,
        payload: t.InputEvent | t.ResponseEvent | t.FaultEvent,
        *,
        event_id: str = "",
        request_id: str | None = None,
    ) -> None:
        try:
            self._queue.put_nowait(
                t.SessionEvent(
                    session_id=self._session_id(),
                    event_id=event_id,
                    request_id=request_id,
                    payload=payload,
                )
            )
        except asyncio.QueueFull:
            error = RimeResourceLimitError("Realtime event consumer is too slow")
            self._fail(error)
            raise error from None

    def _session_id(self) -> str:
        return self._created.result().id

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._shutdown())
        await asyncio.shield(self._close_task)

    async def _shutdown(self) -> None:
        if not self._closed:
            self._fail(RimeStreamError("Realtime session closed"))
            self._failure = None  # explicit close ends the iterator normally
        try:
            await self._socket.close()
        finally:
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
            tasks = list(self._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self._created.done() and not self._created.cancelled():
                self._created.exception()
