"""Test the public runner with controlled adapters and the local Prism peer."""

import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from test_realtime import Peer

from rimelabs_sdk import Rime, RimeInputError, RimeResourceLimitError, RimeStreamError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


class Source:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.waiting = 0
        self.release_error = None

    async def read(self):
        self.waiting += 1
        try:
            value = await self.queue.get()
            if isinstance(value, Exception):
                raise value
            return value
        finally:
            self.waiting -= 1
            if self.release_error:
                raise self.release_error


class Output:
    def __init__(self):
        self.done = asyncio.get_running_loop().create_future()
        self.writes = []
        self.ended = False
        self.interrupted = False
        self.waiting = 0
        self.write_error = None

    def write(self, chunk):
        if self.write_error:
            raise self.write_error
        assert not self.ended and not self.interrupted
        self.writes.append(chunk)

    def end(self):
        self.ended = True

    def interrupt(self):
        self.interrupted = True
        self.writes.clear()

    async def completion(self):
        self.waiting += 1
        try:
            return await asyncio.shield(self.done)
        finally:
            self.waiting -= 1


class Playback:
    def __init__(self):
        self.outputs = {}
        self.batches = []
        self.stopped = False
        self.stop_error = None

    def open(self, output):
        result = Output()
        self.outputs[output.item_id] = result
        return result

    def interrupt(self, outputs):
        self.batches.append(list(outputs))
        for ref in outputs:
            self.outputs[ref.item_id].interrupt()

    async def stop(self):
        self.stopped = True
        for output in self.outputs.values():
            output.interrupt()
        if self.stop_error:
            raise self.stop_error


@pytest.mark.parametrize("trigger", ["speech", "abandoned"])
async def test_interruption_batches_dependencies_and_preserves_other_turns(trigger):
    refs = {
        name: r.ResponseRef(session_id="s", response_id=name)
        for name in ("root", "child", "other", "grandchild", "late", "new")
    }
    source, playback = Source(), Playback()
    consumed = asyncio.Event()

    async def events():
        for name, parent in [
            ("root", None),
            ("child", "root"),
            ("other", None),
            ("grandchild", "child"),
        ]:
            yield SimpleNamespace(
                payload=r.ResponseStarted(
                    response=refs[name],
                    cause="tools" if parent else "text",
                    parent=refs[parent] if parent else None,
                )
            )
            yield SimpleNamespace(
                payload=r.MessageStarted(
                    output=r.OutputRef(
                        response=refs[name], item_id=name, output_index=0, content_index=0
                    )
                )
            )
        yield SimpleNamespace(
            payload=(
                r.SpeechStarted(item_id="user", audio_start_ms=0)
                if trigger == "speech"
                else r.ResponseAbandoned(response=refs["root"])
            )
        )
        # Late descendants must stay silent, while a new independent turn can play.
        for name, parent in [("late", "grandchild"), ("new", None)]:
            yield SimpleNamespace(
                payload=r.ResponseStarted(
                    response=refs[name],
                    cause="tools" if parent else "text",
                    parent=refs[parent] if parent else None,
                )
            )
            output = r.OutputRef(response=refs[name], item_id=name, output_index=0, content_index=0)
            yield SimpleNamespace(payload=r.MessageStarted(output=output))
            yield SimpleNamespace(
                payload=r.AudioDelta(output=output, audio=r.AudioChunk(data=bytes(960)))
            )
        consumed.set()
        await asyncio.Event().wait()

    async def close():
        pass

    session = SimpleNamespace(
        info=SimpleNamespace(interrupt_on_speech=True), events=events(), close=close
    )
    task = asyncio.create_task(r.run_conversation(session, source=source, playback=playback))
    try:
        await asyncio.wait_for(consumed.wait(), 1)
        expected = (
            ["root", "child", "other", "grandchild"]
            if trigger == "speech"
            else ["root", "child", "grandchild"]
        )
        assert [[ref.item_id for ref in batch] for batch in playback.batches] == [
            expected,
            ["late"],
        ]
        assert playback.outputs["other"].interrupted is (trigger == "speech")
        assert not playback.outputs["late"].writes
        assert len(playback.outputs["new"].writes) == 1
    finally:
        source.queue.put_nowait(None)
        await task


async def until(condition):
    async with asyncio.timeout(2):
        while not condition():
            await asyncio.sleep(0)


@asynccontextmanager
async def run(monkeypatch, *, enabled=True, callback=None, revision=None, before_run=None):
    peer = Peer()
    emit = peer.emit

    def configured(kind, **body):
        if kind == "session.updated":
            body["session"].setdefault("prism_effective_interrupt_response", enabled)
            if revision is not None:
                body["session"].setdefault("prism_settings_revision", revision)
        emit(kind, **body)

    peer.emit = configured

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(endpoint="ws://localhost/v1/realtime") as current,
    ):
        if before_run is not None:
            await before_run(current, peer)
        source, playback = Source(), Playback()
        events = []

        def on_event(event):
            events.append(event.payload)
            if callback:
                return callback(event)

        task = asyncio.create_task(
            r.run_conversation(current, source=source, playback=playback, on_event=on_event)
        )
        try:
            yield current, peer, source, playback, task, events
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert peer.closed and playback.stopped
            assert source.waiting == 0
            assert all(output.waiting == 0 for output in playback.outputs.values())
            assert not [t for t in asyncio.all_tasks() if t.get_name() == "rime-conversation"]


def message(peer, response="first", item=None, index=0):
    peer.emit(
        "response.output_item.added",
        response_id=response,
        output_index=index,
        item={"type": "message", "id": item or response},
    )


def audio(peer, response="first", item=None, index=0):
    peer.emit(
        "response.audio.delta",
        response_id=response,
        item_id=item or response,
        output_index=index,
        content_index=0,
        delta=base64.b64encode(bytes(960)).decode(),
    )


def begin(peer, response="first", *, with_audio=True):
    peer.emit("response.created", response={"id": response})
    message(peer, response)
    if with_audio:
        audio(peer, response)


def speech(peer):
    peer.emit("input_audio_buffer.speech_started", item_id="user", audio_start_ms=0)


def ack_cut(peer, request):
    peer.emit(
        "conversation.item.truncated",
        item_id=request["item_id"],
        content_index=0,
        audio_end_ms=request["audio_end_ms"],
        prism_request_event_id=request["event_id"],
    )


async def test_input_conversion_and_events_continue_until_measured_drain(monkeypatch):
    async with run(monkeypatch) as (_, peer, source, playback, task, events):
        begin(peer)
        peer.ended("first")
        await until(lambda: "first" in playback.outputs and playback.outputs["first"].ended)
        output = playback.outputs["first"]
        assert not peer.sent.qsize()
        source.queue.put_nowait(
            r.AudioChunk(data=bytes(3840), format=r.PCMFormat(sample_rate=48000))
        )
        request = await peer.next("input_audio_buffer.append")
        assert len(base64.b64decode(request["audio"])) == 1280
        peer.emit(
            "conversation.item.input_audio_transcription.completed",
            item_id="user",
            transcript="hello",
        )
        await until(lambda: any(isinstance(event, r.TranscriptFinal) for event in events))
        assert not peer.sent.qsize() and not task.done()
        output.done.set_result(13.5)  # Measured playback differs from queued 20 ms.
        report = await peer.next("prism.playback.drained")
        assert report["played_ms"] == 13.5
        source.queue.put_nowait(None)
        await task


@pytest.mark.parametrize("stage", ["before_message", "before_audio", "draining"])
async def test_interruption_late_audio_and_old_receipt_cannot_stop_new_output(monkeypatch, stage):
    async with run(monkeypatch) as (_, peer, _, playback, task, events):
        peer.emit("response.created", response={"id": "first"})
        if stage != "before_message":
            message(peer)
        if stage == "draining":
            audio(peer)
            peer.ended("first")
        speech(peer)
        if stage == "before_message":
            message(peer)
        audio(peer)  # Late data must not reach the output.
        if stage != "draining":
            peer.ended("first", "cancelled")
        begin(peer, "second")
        peer.ended("second")
        await until(lambda: "second" in playback.outputs and playback.outputs["second"].ended)
        first, second = playback.outputs["first"], playback.outputs["second"]
        assert first.interrupted and not first.writes
        assert not second.interrupted and len(second.writes) == 1
        second.done.set_result(19)
        await asyncio.sleep(0)
        assert peer.sent.empty()
        first.done.set_result(7 if stage == "draining" else 0)
        requests = [await asyncio.wait_for(peer.sent.get(), 1) for _ in range(2)]
        drained = next(value for value in requests if value["type"] == "prism.playback.drained")
        assert drained["response_id"] == "second" and drained["played_ms"] == 19
        report = next(value for value in requests if value["type"] == "conversation.item.truncate")
        assert report["item_id"] == "first"
        assert report["audio_end_ms"] == (7 if stage == "draining" else 0)
        ack_cut(peer, report)
        audio(peer)  # Even after cleanup, old output cannot reopen.
        await until(
            lambda: (
                len([e for e in events if isinstance(e, r.AudioDelta)])
                >= (4 if stage == "draining" else 3)
            )
        )
        assert not second.interrupted and not task.done()


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("revision", [None, 1])
async def test_effective_setting_and_hint(monkeypatch, enabled, revision):
    async with run(monkeypatch, enabled=enabled, revision=revision) as (
        current,
        peer,
        _,
        playback,
        _,
        events,
    ):
        assert current.info.interrupt_on_speech is enabled
        begin(peer)
        peer.emit("prism.input_audio_buffer.speech_hint", audio_start_ms=0)
        await until(lambda: any(isinstance(e, r.SpeechHint) for e in events))
        output = playback.outputs["first"]
        assert not output.interrupted
        speech(peer)
        await until(lambda: any(isinstance(e, r.SpeechStarted) for e in events))
        assert output.interrupted is enabled
        peer.ended("first")
        output.done.set_result(17)
        report = await peer.next(
            "conversation.item.truncate" if enabled else "prism.playback.drained"
        )
        if enabled:
            ack_cut(peer, report)
        else:
            assert report["played_ms"] == 17


async def test_each_output_has_its_own_receipt(monkeypatch):
    async with run(monkeypatch) as (_, peer, _, playback, _, _):
        begin(peer)
        message(peer, item="another", index=1)
        audio(peer, item="another", index=1)
        peer.ended("first")
        await until(lambda: "another" in playback.outputs)
        playback.outputs["first"].done.set_result(5)
        await asyncio.sleep(0)
        assert peer.sent.empty()
        playback.outputs["another"].done.set_result(8)
        assert (await peer.next("prism.playback.drained"))["played_ms"] == 13


@pytest.mark.parametrize("stage", ["input", "output", "drain", "report"])
async def test_cancellation_releases_all_operations(monkeypatch, stage):
    async with run(monkeypatch) as (_, peer, source, playback, task, _):
        await until(lambda: source.waiting)
        if stage != "input":
            begin(peer)
            await until(lambda: "first" in playback.outputs)
            if stage == "drain":
                peer.ended("first")
                await until(lambda: playback.outputs["first"].ended)
            if stage == "report":
                speech(peer)
                playback.outputs["first"].done.set_result(3)
                await peer.next("conversation.item.truncate")  # Leave acknowledgment pending.
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)


@pytest.mark.parametrize("mode", ["eof", "local_close", "remote_close"])
async def test_shutdown_stops_pending_input_and_output(monkeypatch, mode):
    async with run(monkeypatch) as (current, peer, source, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs)
        if mode == "eof":
            source.queue.put_nowait(None)
        elif mode == "local_close":
            await current.close()
        else:
            await peer.close()
        if mode == "remote_close":
            with pytest.raises(RimeStreamError):
                await task
        else:
            await task


@pytest.mark.parametrize(
    "mode", ["input", "write", "receipt", "invalid", "huge", "callback", "overflow"]
)
async def test_failures_reach_caller_and_cleanup_does_not_replace_them(monkeypatch, mode):
    original = RuntimeError("original failure")

    def callback(event):
        if mode == "callback":
            raise original

    async with run(monkeypatch, callback=callback) as (_, peer, source, playback, task, _):
        playback.stop_error = RuntimeError("cleanup failure")
        if mode == "input":
            source.queue.put_nowait(original)
        elif mode == "overflow":
            for index in range(65):
                peer.emit("response.created", response={"id": str(index)})
        else:
            begin(peer, with_audio=False)
            if mode != "callback":
                await until(lambda: "first" in playback.outputs)
                output = playback.outputs["first"]
                if mode == "write":
                    output.write_error = original
                    audio(peer)
                elif mode == "receipt":
                    output.done.set_exception(original)
                else:
                    output.done.set_result(10**1000 if mode == "huge" else float("nan"))
        expected = (
            RimeResourceLimitError
            if mode == "overflow"
            else RimeInputError
            if mode in ("invalid", "huge")
            else RuntimeError
        )
        with pytest.raises(expected) as caught:
            await asyncio.wait_for(task, 2)
        if mode not in ("invalid", "huge", "overflow"):
            assert caught.value is original
        assert peer.sent.empty()  # Never manufacture a playback measurement.


async def test_callback_can_handle_tools_with_public_session_methods(monkeypatch):
    tools = []
    async with run(monkeypatch, callback=lambda event: tools.append(event.payload)) as (
        current,
        peer,
        _,
        _,
        _,
        _,
    ):
        peer.emit("response.created", response={"id": "tool"})
        peer.emit(
            "response.function_call_arguments.done",
            response_id="tool",
            item_id="call-item",
            call_id="call",
            name="lookup",
            arguments="{}",
        )
        peer.ended("tool")
        await peer.next("prism.playback.drained")
        call = next(event for event in tools if isinstance(event, r.ToolCall))
        submitting = asyncio.create_task(current.submit_tool_result(call.call, '{"ok":true}'))
        request = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=request["event_id"],
            item={"type": "function_call_output", "call_id": "call"},
        )
        await submitting
        continuing = asyncio.create_task(current.continue_reply(call.call))
        request = await peer.next("response.create")
        peer.accepted(request, "continued")
        await continuing


async def test_cancel_while_transport_send_is_pending(monkeypatch):
    async with run(monkeypatch) as (_, peer, source, _, task, _):
        sending = asyncio.Event()
        released = asyncio.Event()
        original_send = peer.send

        async def blocked(raw):
            sending.set()
            try:
                await asyncio.Event().wait()
                await original_send(raw)
            finally:
                released.set()

        peer.send = blocked
        source.queue.put_nowait(r.AudioChunk(data=bytes(1280)))
        await sending.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert released.is_set()


async def test_audio_done_seals_output_without_blocking_events(monkeypatch):
    async with run(monkeypatch) as (_, peer, _, playback, _, events):
        begin(peer)
        peer.emit(
            "response.audio.done",
            response_id="first",
            item_id="first",
            output_index=0,
            content_index=0,
        )
        await until(lambda: playback.outputs.get("first") and playback.outputs["first"].ended)
        playback.outputs["first"].done.set_result(4)
        peer.emit(
            "conversation.item.input_audio_transcription.completed",
            item_id="user",
            transcript="yes",
        )
        await until(lambda: any(isinstance(e, r.TranscriptFinal) for e in events))
        assert peer.sent.empty()
        peer.ended("first")
        assert (await peer.next("prism.playback.drained"))["played_ms"] == 4


async def test_stale_interruption_refusal_is_forwarded(monkeypatch):
    async with run(monkeypatch) as (_, peer, _, playback, task, events):
        begin(peer)
        speech(peer)
        await until(lambda: playback.outputs.get("first") and playback.outputs["first"].interrupted)
        playback.outputs["first"].done.set_result(0)
        request = await peer.next("conversation.item.truncate")
        peer.emit(
            "error",
            error={
                "code": "truncate_not_current",
                "message": "Old output",
                "scope": "event",
                "owner": {"event_id": request["event_id"]},
            },
        )
        peer.ended("first", "cancelled")
        begin(peer, "second")
        peer.ended("second")
        await until(lambda: playback.outputs.get("second") and playback.outputs["second"].ended)
        playback.outputs["second"].done.set_result(9)
        assert (await peer.next("prism.playback.drained"))["response_id"] == "second"
        assert any(
            isinstance(e, r.FaultEvent) and e.error.code == "truncate_not_current" for e in events
        )
        assert not task.done()


async def test_async_callback_is_rejected(monkeypatch):
    async def callback(event):
        pass

    async with run(monkeypatch, callback=callback) as (_, peer, _, _, task, _):
        begin(peer)
        with pytest.raises(RimeInputError, match="synchronous"):
            await task


async def test_receipt_failure_wins_over_simultaneous_input_eof(monkeypatch):
    async with run(monkeypatch) as (_, peer, source, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs)
        failure = RuntimeError("Playback disconnected")
        source.queue.put_nowait(None)
        playback.outputs["first"].done.set_exception(failure)
        with pytest.raises(RuntimeError) as caught:
            await task
        assert caught.value is failure


@pytest.mark.parametrize("earlier_ended", [False, True])
async def test_newer_token_waits_for_earlier_playback_but_not_later_playback(
    monkeypatch, earlier_ended
):
    async with run(monkeypatch, enabled=False) as (_, peer, source, playback, _, events):
        begin(peer, "first")
        if earlier_ended:
            peer.ended("first")
        begin(peer, "second")
        peer.ended("second")
        begin(peer, "third")
        await until(lambda: "third" in playback.outputs)
        playback.outputs["second"].done.set_result(8)
        await until(lambda: playback.outputs["second"].waiting == 0)
        # A later event and input send must still work while the report waits.
        peer.emit(
            "conversation.item.input_audio_transcription.completed",
            item_id="user",
            transcript="yes",
        )
        await until(lambda: any(isinstance(e, r.TranscriptFinal) for e in events))
        source.queue.put_nowait(r.AudioChunk(data=bytes(1280)))
        await peer.next("input_audio_buffer.append")
        assert peer.sent.empty()
        if not earlier_ended:
            peer.ended("first")
        playback.outputs["first"].done.set_result(12)
        reports = [await peer.next("prism.playback.drained") for _ in range(2)]
        assert {value["response_id"]: value["played_ms"] for value in reports} == {
            "first": 12,
            "second": 8,
        }
        assert playback.outputs["third"].waiting == 1


@pytest.mark.parametrize("original_failure", [False, True])
async def test_input_release_failure_is_preserved(monkeypatch, original_failure):
    original = RuntimeError("callback failed")

    def callback(event):
        if original_failure:
            raise original

    async with run(monkeypatch, callback=callback) as (current, peer, source, _, task, _):
        await until(lambda: source.waiting == 1)
        released = RuntimeError("microphone release failed")
        source.release_error = released
        if original_failure:
            begin(peer)
        else:
            await current.close()
        with pytest.raises(RuntimeError) as caught:
            await task
        assert caught.value is (original if original_failure else released)


@pytest.mark.parametrize("reason", ["eof", "closed", "cancelled"])
async def test_shutdown_waits_for_adapter_confirmation_before_cancelling_receipts(
    monkeypatch, reason
):
    async with run(monkeypatch) as (current, peer, source, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs and playback.outputs["first"].waiting == 1)
        stopping, confirmed = asyncio.Event(), asyncio.Event()
        stop = playback.stop

        async def delayed_stop():
            stopping.set()
            await confirmed.wait()
            await stop()

        playback.stop = delayed_stop
        if reason == "eof":
            source.queue.put_nowait(None)
        elif reason == "closed":
            await current.close()
        else:
            task.cancel()
        try:
            await stopping.wait()
            assert not task.done()
            assert not playback.stopped
            assert playback.outputs["first"].waiting == 1
            await until(lambda: source.waiting == 0)
        finally:
            confirmed.set()
        if reason == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
        assert playback.stopped and playback.outputs["first"].waiting == 0


@pytest.mark.parametrize("cancel_during_shutdown", [False, True])
async def test_shutdown_confirmation_has_a_deadline(monkeypatch, cancel_during_shutdown):
    from rimelabs_sdk import RimeTimeoutError

    async with run(monkeypatch) as (_, peer, source, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs)
        released = asyncio.Event()
        stopping = asyncio.Event()

        async def missing_ack():
            playback.stopped = True
            stopping.set()
            try:
                await asyncio.Event().wait()
            finally:
                released.set()

        playback.stop = missing_ack
        source.queue.put_nowait(None)
        await stopping.wait()
        if cancel_during_shutdown:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel_during_shutdown else RimeTimeoutError):
            await asyncio.wait_for(task, 7)
        assert released.is_set()


@pytest.mark.parametrize("reason", ["eof", "closed", "cancelled", "failure"])
async def test_cancellation_during_shutdown_waits_for_confirmation(monkeypatch, reason):
    original = RuntimeError("callback failed")

    def callback(event):
        if reason == "failure" and isinstance(event.payload, r.TranscriptFinal):
            raise original

    async with run(monkeypatch, callback=callback) as (session, peer, source, playback, task, _):
        begin(peer)
        await until(lambda: playback.outputs.get("first") and playback.outputs["first"].writes)
        stopping, confirmed = asyncio.Event(), asyncio.Event()
        stop = playback.stop

        async def delayed_stop():
            stopping.set()
            await confirmed.wait()
            await stop()

        playback.stop = delayed_stop
        if reason == "eof":
            source.queue.put_nowait(None)
        elif reason == "closed":
            await session.close()
        elif reason == "cancelled":
            task.cancel("original cancellation")
        else:
            peer.emit(
                "conversation.item.input_audio_transcription.completed",
                item_id="user",
                transcript="hello",
            )
        try:
            await stopping.wait()
            for _ in range(2):
                task.cancel("cancellation during cleanup")
                await asyncio.sleep(0)
                assert not task.done()
                assert not playback.stopped
                assert playback.outputs["first"].waiting == 1
        finally:
            confirmed.set()
        with pytest.raises(
            RuntimeError if reason == "failure" else asyncio.CancelledError
        ) as caught:
            await task
        if reason == "failure":
            assert caught.value is original
        elif reason == "cancelled":
            assert str(caught.value) == "original cancellation"
        assert playback.stopped
        assert playback.outputs["first"].interrupted
        assert not playback.outputs["first"].writes
        assert playback.outputs["first"].waiting == 0


@pytest.mark.parametrize("reason", ["local_close", "remote_close", "refused"])
async def test_close_with_pending_interruption_report(monkeypatch, reason):
    async with run(monkeypatch) as (session, peer, _, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs)
        speech(peer)
        playback.outputs["first"].done.set_result(3)
        request = await peer.next("conversation.item.truncate")
        if reason == "local_close":
            await session.close()
            await task
        elif reason == "remote_close":
            await peer.close()
            with pytest.raises(RimeStreamError):
                await task
        else:
            peer.emit(
                "error",
                error={
                    "code": "invalid_request",
                    "message": "Realtime session closed",
                    "scope": "event",
                    "owner": {"event_id": request["event_id"]},
                },
            )
            with pytest.raises(r.RimeRealtimeError, match="Realtime session closed"):
                await task


async def test_unrelated_stream_error_during_close_is_preserved(monkeypatch):
    async with run(monkeypatch) as (session, peer, _, playback, task, _):
        begin(peer)
        await until(lambda: "first" in playback.outputs)
        error = RimeStreamError("Realtime session closed")
        playback.stop_error = error
        await session.close()
        with pytest.raises(RimeStreamError) as caught:
            await task
        assert caught.value is error


@pytest.mark.parametrize("ticks", [0, 1, 2, 3])
async def test_close_concurrent_with_playback_completion(monkeypatch, ticks):
    async with run(monkeypatch) as (session, peer, _, playback, task, _):
        begin(peer)
        peer.ended("first")
        await until(lambda: "first" in playback.outputs and playback.outputs["first"].ended)
        closing = asyncio.create_task(session.close())
        # Exercise completion before, during, and after session shutdown starts.
        for _ in range(ticks):
            await asyncio.sleep(0)
        playback.outputs["first"].done.set_result(8)
        await closing
        await task


@pytest.mark.parametrize("mode", ["receipt", "callback", "input"])
async def test_unhashable_adapter_and_callback_errors_reach_caller(monkeypatch, mode):
    @dataclass
    class DeviceError(Exception):
        message: str

    error = DeviceError("device failed")

    def callback(event):
        if mode == "callback":
            raise error

    async with run(monkeypatch, callback=callback) as (_, peer, source, playback, task, _):
        playback.stop_error = RuntimeError("cleanup failed")
        if mode == "input":
            source.queue.put_nowait(error)
        else:
            begin(peer)
            if mode == "receipt":
                await until(lambda: "first" in playback.outputs)
                playback.outputs["first"].done.set_exception(error)
        with pytest.raises(DeviceError) as caught:
            await asyncio.wait_for(asyncio.shield(task), 1)
        assert caught.value is error


async def test_measured_reply_keeps_finished_report_while_earlier_playback_is_cut(monkeypatch):
    async with run(monkeypatch) as (_, peer, _, playback, _, events):
        begin(peer, "a")
        peer.ended("a")
        begin(peer, "b")
        peer.ended("b")
        await until(lambda: "b" in playback.outputs and playback.outputs["b"].ended)
        playback.outputs["b"].done.set_result(8)
        await until(lambda: playback.outputs["b"].waiting == 0)
        assert peer.sent.empty()
        speech(peer)
        await until(lambda: any(isinstance(event, r.SpeechStarted) for event in events))
        assert [[ref.item_id for ref in batch] for batch in playback.batches] == [["a"]]
        assert not playback.outputs["b"].interrupted
        assert peer.sent.empty()
        playback.outputs["a"].done.set_result(4)
        requests = [await asyncio.wait_for(peer.sent.get(), 1) for _ in range(2)]
        cut_a = next(
            request for request in requests if request["type"] == "conversation.item.truncate"
        )
        drain_b = next(
            request for request in requests if request["type"] == "prism.playback.drained"
        )
        assert cut_a["item_id"] == "a" and cut_a["audio_end_ms"] == 4
        assert drain_b["response_id"] == "b" and drain_b["played_ms"] == 8
        ack_cut(peer, cut_a)


@pytest.mark.parametrize("newer_reply", [False, True])
async def test_speech_preserves_pending_finished_report(monkeypatch, newer_reply):
    async with run(monkeypatch) as (session, peer, _, playback, _, events):
        reporting, release = asyncio.Event(), asyncio.Event()
        report_playback = session.report_playback

        async def hold_finished(report):
            if isinstance(report, r.PlaybackFinished):
                reporting.set()
                await release.wait()
            await report_playback(report)

        monkeypatch.setattr(session, "report_playback", hold_finished)
        begin(peer, "first")
        peer.ended("first")
        await until(lambda: "first" in playback.outputs and playback.outputs["first"].ended)
        playback.outputs["first"].done.set_result(12)
        await asyncio.wait_for(reporting.wait(), 1)
        if newer_reply:
            begin(peer, "second")
            await until(lambda: "second" in playback.outputs)
        speech(peer)
        await until(lambda: any(isinstance(event, r.SpeechStarted) for event in events))
        assert not playback.outputs["first"].interrupted
        assert [[ref.item_id for ref in batch] for batch in playback.batches] == (
            [["second"]] if newer_reply else []
        )
        if newer_reply:
            playback.outputs["second"].done.set_result(3)
            cut = await peer.next("conversation.item.truncate")
            assert cut["item_id"] == "second"
            ack_cut(peer, cut)
        release.set()
        drain = await peer.next("prism.playback.drained")
        assert drain["response_id"] == "first" and drain["played_ms"] == 12


async def test_unrelated_input_error_during_close_is_preserved(monkeypatch):
    async with run(monkeypatch) as (session, _, source, _, task, _):
        await until(lambda: source.waiting)
        error = RimeInputError("The realtime session is closed")
        source.release_error = error
        await session.close()
        with pytest.raises(RimeInputError) as caught:
            await task
        assert caught.value is error


@pytest.mark.parametrize("stage", ["generating", "draining", "interrupted", "eof"])
async def test_unexpected_receipt_cancellation_fails_the_run(monkeypatch, stage):
    async with run(monkeypatch) as (_, peer, source, playback, task, _):
        begin(peer)
        if stage != "generating":
            peer.ended("first")
        await until(lambda: playback.outputs.get("first") and playback.outputs["first"].waiting)
        if stage == "interrupted":
            speech(peer)
            await until(lambda: playback.outputs["first"].interrupted)
        elif stage == "eof":
            source.queue.put_nowait(None)
        playback.outputs["first"].done.cancel()
        with pytest.raises(RimeStreamError, match="cancelled unexpectedly") as caught:
            await asyncio.wait_for(asyncio.shield(task), 1)
        assert isinstance(caught.value.__cause__, asyncio.CancelledError)
        assert playback.stopped and peer.closed
        assert playback.outputs["first"].waiting == 0
        assert peer.sent.empty()


async def test_unexpected_source_cancellation_fails_the_run(monkeypatch):
    async with run(monkeypatch) as (_, peer, source, playback, task, _):

        async def cancelled_read():
            raise asyncio.CancelledError("input adapter disconnected")

        source.read = cancelled_read
        # Release the already pending read so the next one uses the adapter above.
        source.queue.put_nowait(r.AudioChunk(data=bytes(1280)))
        with pytest.raises(RimeStreamError, match="cancelled unexpectedly") as caught:
            await asyncio.wait_for(asyncio.shield(task), 1)
        assert isinstance(caught.value.__cause__, asyncio.CancelledError)
        assert playback.stopped and peer.closed


@pytest.mark.parametrize("ended", [False, True])
@pytest.mark.parametrize(
    "initial,updated,late,reply_revision",
    [
        (True, False, False, 1),
        (False, True, False, 1),
        (True, False, True, 1),
        (False, True, True, 1),
        (True, False, True, 2),
        (False, True, True, 2),
        (False, False, True, 1),
    ],
)
async def test_confirmed_speech_uses_reply_policy(
    monkeypatch, initial, updated, late, reply_revision, ended
):
    async with run(monkeypatch, enabled=initial, revision=1) as (
        current,
        peer,
        _,
        playback,
        _,
        events,
    ):

        def begin_reply():
            peer.emit(
                "response.created",
                response={"id": "first", "prism_settings_revision": reply_revision},
            )
            message(peer)
            audio(peer)

        if not late:
            begin_reply()
            await until(lambda: "first" in playback.outputs)
        update = asyncio.create_task(current.update(interrupt_on_speech=updated))
        q = await peer.next("session.update")
        peer.emit(
            "session.updated",
            prism_request_event_id=q["event_id"],
            session={
                "id": "session-1",
                "voice": "test",
                "prism_effective_interrupt_response": updated,
                "prism_settings_revision": 2,
            },
        )
        await update
        if late:
            begin_reply()
            await until(lambda: "first" in playback.outputs)
        if ended:
            # The server's playback estimate has expired, but the physical receipt is pending.
            peer.ended("first")
            await until(lambda: any(isinstance(event, r.ResponseEnded) for event in events))
        speech(peer)
        await until(lambda: any(isinstance(event, r.SpeechStarted) for event in events))
        expected = initial if reply_revision == 1 else updated
        output = playback.outputs["first"]
        assert output.interrupted is expected
        if not ended:
            peer.ended("first")
        output.done.set_result(17)
        report = await peer.next(
            "conversation.item.truncate" if expected else "prism.playback.drained"
        )
        if expected:
            ack_cut(peer, report)


@pytest.mark.parametrize("change", [False, True])
async def test_interruption_policy_history_is_bounded_by_changes(monkeypatch, change):
    from test_realtime import session
    from test_realtime_updates import acknowledge

    from rimelabs_sdk import RimeResourceLimitError

    async with session(monkeypatch) as (current, peer, _):
        for revision in range(1, 131):
            enabled = bool(revision % 2) if change else False
            update = asyncio.create_task(current.update(interrupt_on_speech=enabled))
            q = await peer.next("session.update")
            acknowledge(peer, q, revision, interrupt=enabled)
            await update
        peer.emit("response.created", response={"id": "old-guess", "prism_settings_revision": 1})
        events = current.events
        if change:
            with pytest.raises(RimeResourceLimitError):
                await anext(events)
        else:
            assert (await anext(events)).payload.settings_revision == 1
        await events.aclose()


async def test_queued_reply_keeps_policy_after_history_eviction(monkeypatch):
    from test_realtime_updates import acknowledge

    async def queue_before_runner(current, peer):
        peer.emit("response.created", response={"id": "first", "prism_settings_revision": 1})
        for revision in range(2, 131):
            enabled = bool(revision % 2)
            update = asyncio.create_task(current.update(interrupt_on_speech=enabled))
            q = await peer.next("session.update")
            acknowledge(peer, q, revision, interrupt=enabled)
            await update
        message(peer)
        audio(peer)

    async with run(monkeypatch, revision=1, before_run=queue_before_runner) as (
        current,
        peer,
        _,
        playback,
        _,
        events,
    ):
        assert not current.info.interrupt_on_speech
        await until(lambda: "first" in playback.outputs)
        speech(peer)
        await until(lambda: any(isinstance(event, r.SpeechStarted) for event in events))
        assert playback.outputs["first"].interrupted
        peer.ended("first")
        playback.outputs["first"].done.set_result(17)
        ack_cut(peer, await peer.next("conversation.item.truncate"))


@pytest.mark.parametrize("latest_known", [1, 5])
@pytest.mark.parametrize("reply_age", ["old", "unknown", "new", "omitted"])
async def test_missing_update_revision_does_not_guess_reply_policy(
    monkeypatch, latest_known, reply_age
):
    from test_realtime_updates import acknowledge

    async def updates_before_runner(current, peer):
        if latest_known != 1:
            update = asyncio.create_task(current.update(instructions="Same policy"))
            request = await peer.next("session.update")
            acknowledge(peer, request, latest_known, interrupt=True)
            await update
        for enabled in (False, True, False):
            update = asyncio.create_task(current.update(interrupt_on_speech=enabled))
            request = await peer.next("session.update")
            # No revision was published for any of these policy changes.
            peer.emit(
                "session.updated",
                prism_request_event_id=request["event_id"],
                session={
                    "id": "session-1",
                    "voice": "test",
                    "prism_effective_interrupt_response": enabled,
                },
            )
            await update
        update = asyncio.create_task(current.update(instructions="Published again"))
        request = await peer.next("session.update")
        acknowledge(peer, request, latest_known + 4, interrupt=False)
        await update

    # The configured peer adds initialization's revision to every acknowledgment;
    # bypass that behavior for the deliberately revisionless updates above.
    async with run(monkeypatch, revision=1) as (current, peer, _, playback, task, events):
        configured = peer.emit
        peer.emit = Peer.emit.__get__(peer, Peer)
        await updates_before_runner(current, peer)
        peer.emit = configured
        response = {"id": "first"}
        if reply_age != "omitted":
            response["prism_settings_revision"] = {
                "old": latest_known,
                "unknown": latest_known + 1,
                "new": latest_known + 4,
            }[reply_age]
        peer.emit("response.created", response=response)
        if reply_age == "unknown":
            with pytest.raises(RimeResourceLimitError, match="revision is unknown"):
                await asyncio.wait_for(asyncio.shield(task), 1)
            return
        message(peer)
        audio(peer)
        await until(lambda: "first" in playback.outputs)
        speech(peer)
        await until(lambda: any(isinstance(event, r.SpeechStarted) for event in events))
        expected = reply_age == "old"
        assert playback.outputs["first"].interrupted is expected
        peer.ended("first")
        playback.outputs["first"].done.set_result(7)
        report = await peer.next(
            "conversation.item.truncate" if expected else "prism.playback.drained"
        )
        if expected:
            ack_cut(peer, report)
