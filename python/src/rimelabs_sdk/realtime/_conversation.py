"""Audio coordination through caller-owned input and playback adapters."""

from __future__ import annotations

import asyncio
import inspect
import math
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from .._errors import RimeInputError, RimeResourceLimitError, RimeStreamError, RimeTimeoutError
from . import _types as t
from ._session import RealtimeSession, _is_session_close_error, _response_interrupts_speech


class ConversationSource(Protocol):
    async def read(self) -> t.AudioChunk | None:
        """Read one chunk, or None at EOF. Must release a pending read on cancellation."""
        ...


class ConversationOutput(Protocol):
    def write(self, chunk: t.AudioChunk) -> None:
        """Queue audio without blocking. Raise on overflow; never discard audio silently."""
        ...

    def end(self) -> None:
        """Seal this output. Completion waits for queued and device audio to drain."""
        ...

    async def completion(self) -> float:
        """Return measured played milliseconds, or raise if measurement fails.

        Cancellation must stop the wait. It must not cancel the device receipt.
        """
        ...


class ConversationPlayback(Protocol):
    def open(self, output: t.OutputRef) -> ConversationOutput:
        """Queue an output in open order; audible playback must be FIFO.

        Keep completion and interruption state separate for each output.
        Do not mix outputs or play a later output ahead of an earlier one.
        """
        ...

    def interrupt(self, outputs: Sequence[t.OutputRef]) -> None:
        """Stop all selected outputs as one batch, including outputs after end().

        Discard their unplayed audio before starting any remaining output.
        Resume remaining outputs in FIFO order without waiting for new audio.
        Repeated interruptions are harmless; each receipt remains independent.
        """
        ...

    async def stop(self) -> None:
        """Confirm all playback has stopped and queued audio is discarded.

        Keep caller resources open. Release the wait on task cancellation;
        the runner allows five seconds for shutdown confirmation.
        """
        ...


@dataclass
class _Output:
    ref: t.OutputRef
    stream: ConversationOutput
    receipt: asyncio.Task[float]
    sealed: bool = False
    report: asyncio.Task[None] | None = None

    def end(self) -> None:
        if not self.sealed:
            self.sealed = True
            self.stream.end()


@dataclass
class _Reply:
    ref: t.ResponseRef
    interrupt_on_speech: bool = True
    outputs: dict[t.OutputRef, _Output] = field(default_factory=dict)
    interrupted: bool = False
    ended: bool = False
    finish: asyncio.Task[None] | None = None
    drained: asyncio.Event = field(default_factory=asyncio.Event)


async def run_conversation(
    session: RealtimeSession,
    *,
    source: ConversationSource,
    playback: ConversationPlayback,
    on_event: Callable[[t.SessionEvent], None] | None = None,
) -> None:
    """Run a live conversation until input EOF, session closure, or cancellation.

    This function owns the session's active lifetime and closes it on exit.
    The caller owns adapter resource contexts. Adapter operations must support
    task cancellation. Do not read session.events or report playback elsewhere.
    on_event runs inline for every event; it must be synchronous and nonblocking.
    Callback and adapter failures reach the caller. Application work scheduled by
    the callback remains the application's responsibility.

    Input is sent one chunk at a time through session.send_audio. No input or
    callback queue is added. At most 64 responses, 128 outputs, and 256 pending
    operations are retained; excess work fails instead of dropping audio.
    """
    replies: dict[str, _Reply] = {}
    parents: OrderedDict[str, str | None] = OrderedDict()
    interrupted: set[str] = set()
    tasks: set[asyncio.Task[Any]] = set()
    cancelled_tasks: set[asyncio.Task[Any]] = set()
    receipts: set[asyncio.Task[Any]] = set()
    failure: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def completed(task: asyncio.Task[Any]) -> None:
        tasks.discard(task)
        expected_cancellation = task in cancelled_tasks
        cancelled_tasks.discard(task)
        if task.cancelled():
            if not expected_cancellation and not failure.done():
                cancellation_error = RimeStreamError(
                    "Conversation operation was cancelled unexpectedly"
                )
                try:
                    task.result()
                except asyncio.CancelledError as cause:
                    cancellation_error.__cause__ = cause
                failure.set_exception(cancellation_error)
            return
        if (
            (error := task.exception()) is not None
            and not _is_session_close_error(error)
            and not failure.done()
        ):
            failure.set_exception(error)

    def cancel(task: asyncio.Task[Any]) -> None:
        if task.cancel():
            cancelled_tasks.add(task)

    def start(work: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        if len(tasks) >= 256:
            work.close()
            raise RimeResourceLimitError("Conversation pending operation limit exceeded")
        task = asyncio.create_task(work, name="rime-conversation")
        tasks.add(task)
        task.add_done_callback(completed)
        return task

    async def measure(stream: ConversationOutput) -> float:
        value = await stream.completion()
        try:
            invalid = (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            )
        except OverflowError:
            invalid = True
        if invalid:
            raise RimeInputError("Playback measurement must be finite nonnegative milliseconds")
        return value

    async def report_cut(output: _Output) -> None:
        played = await asyncio.shield(output.receipt)
        try:
            await session.report_playback(
                t.PlaybackInterrupted(output=output.ref, audio_end_ms=int(played))
            )
        except t.RimeRealtimeError as error:
            # The session also forwards this event to the application callback.
            if error.fault.scope != "event" or error.fault.code != "truncate_not_current":
                raise

    def cut(output: _Output) -> None:
        if output.report is None:
            output.end()
            output.report = start(report_cut(output))

    async def finish(reply: _Reply) -> None:
        earlier = []
        for previous in replies.values():
            if previous is reply:
                break
            earlier.append(previous.drained)
        played = 0.0
        for output in reply.outputs.values():
            played += await asyncio.shield(output.receipt)
        reply.drained.set()
        if reply.interrupted:
            for output in reply.outputs.values():
                if output.report:
                    await asyncio.shield(output.report)
        else:
            # Prism's latest drain token clears all earlier playback too.
            for drained in earlier:
                await drained.wait()
            await session.report_playback(t.PlaybackFinished(response=reply.ref, played_ms=played))
        replies.pop(reply.ref.response_id, None)

    def was_interrupted(response_id: str) -> bool:
        current: str | None = response_id
        # The session retains at most 128 response references.
        for _ in range(128):
            if current is None:
                return False
            if current in interrupted:
                return True
            current = parents.get(current)
        return False

    def interrupt(selected: Sequence[_Reply]) -> None:
        affected = [reply for reply in selected if not reply.interrupted]
        outputs = [output for reply in affected for output in reply.outputs.values()]
        for reply in affected:
            reply.interrupted = True
            if reply.ref.response_id in parents:
                interrupted.add(reply.ref.response_id)
            if reply.finish:
                cancel(reply.finish)
        if outputs:
            playback.interrupt([output.ref for output in outputs])
        for output in outputs:
            cut(output)
        for reply in affected:
            if reply.ended:
                reply.finish = start(finish(reply))

    def output_for(ref: t.OutputRef) -> _Output | None:
        reply = replies.get(ref.response.response_id)
        # Late events for a response whose playback was already released cannot
        # create another device stream. Protocol validation belongs to session.
        if reply is None or reply.ended:
            return None
        if ref not in reply.outputs:
            if sum(len(value.outputs) for value in replies.values()) >= 128:
                raise RimeResourceLimitError("Conversation output limit exceeded")
            stream = playback.open(ref)
            output = _Output(ref, stream, start(measure(stream)))
            receipts.add(output.receipt)
            output.receipt.add_done_callback(receipts.discard)
            reply.outputs[ref] = output
            if reply.interrupted:
                playback.interrupt([ref])
                cut(output)
        return reply.outputs[ref]

    async def send() -> None:
        while (chunk := await source.read()) is not None:
            await session.send_audio(chunk)

    async def read() -> None:
        reply: _Reply | None
        async for event in session.events:
            payload = event.payload
            if isinstance(payload, t.ResponseStarted):
                if len(replies) >= 64:
                    raise RimeResourceLimitError("Conversation response limit exceeded")
                response_id = payload.response.response_id
                parents[response_id] = payload.parent.response_id if payload.parent else None
                if len(parents) > 128:
                    expired, _ = parents.popitem(last=False)
                    interrupted.discard(expired)
                replies[response_id] = _Reply(
                    payload.response,
                    interrupt_on_speech=_response_interrupts_speech(session, payload.response),
                    interrupted=was_interrupted(response_id),
                )
                if replies[response_id].interrupted:
                    interrupted.add(response_id)
            elif isinstance(payload, (t.MessageStarted, t.AudioDelta, t.AudioDone)):
                output = output_for(payload.output)
                if output is not None:
                    reply = replies[payload.output.response.response_id]
                    if isinstance(payload, t.AudioDelta) and not reply.interrupted:
                        output.stream.write(payload.audio)
                    elif isinstance(payload, t.AudioDone):
                        output.end()
            elif isinstance(payload, t.SpeechStarted):
                floor = next(
                    (
                        reply
                        for reply in reversed(tuple(replies.values()))
                        if not reply.interrupted
                        and (
                            not reply.ended
                            or any(not output.receipt.done() for output in reply.outputs.values())
                        )
                    ),
                    None,
                )
                enabled = floor.interrupt_on_speech if floor else session.info.interrupt_on_speech
                if enabled:
                    interrupt(list(replies.values()))
            elif isinstance(payload, t.ResponseAbandoned):
                if payload.response.response_id in parents:
                    interrupted.add(payload.response.response_id)
                interrupt(
                    [reply for reply in replies.values() if was_interrupted(reply.ref.response_id)]
                )
            elif isinstance(payload, t.ResponseEnded):
                reply = replies.get(payload.response.response_id)
                if reply:
                    reply.ended = True
                    for output in reply.outputs.values():
                        output.end()
                    reply.finish = start(finish(reply))
            if on_event is not None:
                result = on_event(event)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise RimeInputError("on_event must be synchronous and nonblocking")

    async def cleanup() -> BaseException | None:
        cleanup_error: BaseException | None = None
        # Prevent new writes and reports while the adapter confirms shutdown.
        # Receipt waits remain live until stop() completes or times out.
        for task in tasks - receipts:
            cancel(task)
        try:
            async with asyncio.timeout(5):
                await playback.stop()
        except TimeoutError:
            cleanup_error = RimeTimeoutError("Playback shutdown confirmation timed out")
        except BaseException as error:  # noqa: BLE001 -- finish cleanup before propagating
            cleanup_error = error
        pending = list(tasks)
        for task in pending:
            if not task.cancelling():
                cancel(task)
        try:
            await session.close()
        except BaseException as error:  # noqa: BLE001 -- finish cleanup before propagating
            cleanup_error = cleanup_error or error
        await asyncio.gather(*pending, return_exceptions=True)
        if failure.done():
            task_error = failure.exception()
            cleanup_error = task_error or cleanup_error
        else:
            failure.cancel()
        return cleanup_error

    original: BaseException | None = None
    try:
        sending, reading = start(send()), start(read())
        done, _ = await asyncio.wait(
            [sending, reading, failure], return_when=asyncio.FIRST_COMPLETED
        )
        # A failure wins over simultaneous normal EOF.
        if failure.done():
            await failure
        for task in done:
            try:
                await task
            except Exception as error:
                if not _is_session_close_error(error):
                    raise
    except BaseException as error:
        original = error
        raise
    finally:
        # Cancellation must not interrupt shutdown confirmation or reset its deadline.
        # Retain the task and keep awaiting it, including after repeated cancellation.
        cleaning = asyncio.create_task(cleanup(), name="rime-conversation-cleanup")
        cancelled: asyncio.CancelledError | None = None
        while not cleaning.done():
            try:
                await asyncio.shield(cleaning)
            except asyncio.CancelledError as error:
                cancelled = cancelled or error
        cleanup_error = cleaning.result()
        if original is None:
            if cancelled is not None:
                raise cancelled
            if cleanup_error is not None:
                raise cleanup_error
