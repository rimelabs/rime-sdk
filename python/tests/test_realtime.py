"""Public Prism interface tests with a controlled WebSocket peer."""

import asyncio
import base64
import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from rimelabs_sdk import (
    Rime,
    RimeAudioFormatError,
    RimeCancelledError,
    RimeInputError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
)
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client, _session

_CONTRACT = yaml.safe_load(
    (Path(__file__).parents[2] / "conformance/prism/speech_to_speech.asyncapi.yaml").read_text()
)
_VALIDATORS = {
    message["name"]: Draft7Validator({**message["payload"], "components": _CONTRACT["components"]})
    for message in _CONTRACT["components"]["messages"].values()
}


class Peer:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.closed = False
        self.settings = None
        self.emit("session.created", session={"id": "session-1"})

    def emit(self, kind, **body):
        self.incoming.put_nowait(json.dumps({"type": kind, "event_id": "server-event", **body}))

    async def send(self, raw):
        event = json.loads(raw)
        _VALIDATORS[event["type"]].validate(event)
        if event["type"] == "session.update":
            self.settings = event["session"]
            self.emit(
                "session.updated",
                prsm_request_event_id=event["event_id"],
                session={"id": "session-1", "voice": "test"},
            )
        else:
            await self.sent.put(event)

    async def close(self):
        if not self.closed:
            self.closed = True
            self.incoming.put_nowait(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.incoming.get()
        if value is None:
            raise StopAsyncIteration
        return value

    async def next(self, kind):
        event = await asyncio.wait_for(self.sent.get(), 1)
        assert event["type"] == kind
        return event

    def accepted(self, request, response_id="reply-1", **metadata):
        self.emit(
            "response.created",
            prsm_request_event_id=request["event_id"],
            response={
                "id": response_id,
                "metadata": {**request.get("response", {}).get("metadata", {}), **metadata},
            },
        )

    def ended(self, response_id="reply-1", status="completed"):
        self.emit(
            "response.done",
            response={
                "id": response_id,
                "status": status,
                "metadata": {"prsm_drain_token": "drain-1"},
            },
        )


@asynccontextmanager
async def session(monkeypatch, *, timeouts=None):
    peer = Peer()

    async def connect(endpoint, **kwargs):
        assert kwargs["additional_headers"] == {"Authorization": "Bearer test"}
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(
            endpoint="ws://localhost/v1/realtime",
            tools=[
                r.ToolDefinition(name="lookup", parameters={"type": "object", "properties": {}})
            ],
            timeouts=timeouts or r.RealtimeTimeouts(request_s=0.2, ready_s=0.4),
        ) as current,
    ):
        yield current, peer, client


async def accepted_turn(current, peer):
    peer.emit("prsm.typed_input.ready")
    task = asyncio.create_task(current.send_text("hello"))
    request = await peer.next("response.create")
    peer.accepted(request)
    return await task


async def test_client_close_stops_tts_and_realtime(setup, monkeypatch):
    _, client = setup
    peer = Peer()

    async def connect(endpoint, **kwargs):
        assert kwargs["additional_headers"] == {"Authorization": "Bearer test-key"}
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    source_waiting = asyncio.Event()
    source_closed = asyncio.Event()

    async def text():
        try:
            source_waiting.set()
            await asyncio.Event().wait()
            yield "Never sent."
        finally:
            source_closed.set()

    async with (
        client.realtime.connect(endpoint="ws://localhost/v1/realtime") as current,
        client.tts.stream(text()) as audio,
    ):
        reading = asyncio.create_task(anext(audio))
        await asyncio.wait_for(source_waiting.wait(), 1)
        clearing = asyncio.create_task(current.clear_audio())
        await peer.next("input_audio_buffer.clear")
        await client.close()
        with pytest.raises(RimeCancelledError):
            await reading
        with pytest.raises(RimeStreamError, match="closed"):
            await clearing
        assert source_closed.is_set()
        assert peer.closed
        with pytest.raises(RimeInputError, match="closed"):
            client.tts.stream("After close.")
        with pytest.raises(RimeInputError, match="closed"):
            await client.voices.list()
        with pytest.raises(RimeInputError, match="closed"):
            await current.send_text("After close.")


async def test_configuration_and_typed_events(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        assert current.info.voice == "test"
        assert "tool_choice" not in peer.settings
        ref = await accepted_turn(current, peer)
        peer.emit(
            "response.output_item.added",
            response_id=ref.response_id,
            output_index=0,
            item={"id": "msg", "type": "message"},
        )
        peer.emit(
            "response.audio.delta",
            response_id=ref.response_id,
            item_id="msg",
            output_index=0,
            content_index=0,
            delta=base64.b64encode(b"\0\0" * 20).decode(),
        )
        peer.ended()
        iterator = current.events
        assert isinstance((await anext(iterator)).payload, r.InputReady)
        assert isinstance((await anext(iterator)).payload, r.ResponseStarted)
        output = (await anext(iterator)).payload.output
        assert output.response == ref
        assert (await anext(iterator)).payload.audio.format.sample_rate == 24000
        assert (await anext(iterator)).payload.status == "completed"
        await iterator.aclose()


async def test_received_audio_can_exceed_outgoing_chunk_limit(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        data = b"\x01\x00" * 96001
        event = {
            "type": "response.audio.delta",
            "event_id": "audio-event",
            "response_id": ref.response_id,
            "item_id": "msg",
            "output_index": 0,
            "content_index": 0,
            "delta": base64.b64encode(data).decode(),
        }
        _VALIDATORS[event["type"]].validate(event)
        assert len(json.dumps(event).encode()) < 1024 * 1024
        peer.incoming.put_nowait(json.dumps(event))
        peer.ended()
        audio = []
        async for event in current.events:
            if isinstance(event.payload, r.AudioDelta):
                audio.append(event.payload.audio)
            if isinstance(event.payload, r.ResponseEnded):
                break
        assert audio == [r.AudioChunk(data=data, format=r.PCMFormat(sample_rate=24000))]
        assert not peer.closed


async def test_outgoing_audio_limit_preserves_conversion_state(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        await current.send_audio(
            r.AudioChunk(data=b"\x01\x00" * 5, format=r.PCMFormat(sample_rate=24000))
        )
        await peer.next("input_audio_buffer.append")
        previous = current._audio_format, current._resample_state
        chunk = r.AudioChunk(data=b"\x02\x00" * 96001)
        with pytest.raises(RimeAudioFormatError, match="at most 192000 bytes"):
            await current.send_audio(chunk)
        assert (current._audio_format, current._resample_state) == previous
        assert peer.sent.empty()
        await current.send_audio(r.AudioChunk(data=b"\x00\x00" * 96000))
        audio = b""
        while not peer.sent.empty():
            audio += base64.b64decode((await peer.next("input_audio_buffer.append"))["audio"])
        assert audio == b"\x00\x00" * 96000
        assert not peer.closed


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [({}, "speech"), ({"prsm_cause": "future_cause"}, "unknown")],
)
async def test_unknown_server_enum_values_are_explicit(monkeypatch, metadata, expected):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("response.created", response={"id": "future", "metadata": metadata})
        peer.emit(
            "error",
            error={
                "code": "future_code",
                "scope": "future_scope",
                "message": "An extension event",
                "owner": {"kind": "future_owner"},
            },
        )
        peer.ended("future", status="future_status")
        iterator = current.events
        started = (await anext(iterator)).payload
        fault = (await anext(iterator)).payload
        ended = (await anext(iterator)).payload
        assert isinstance(started, r.ResponseStarted) and started.cause == expected
        assert isinstance(fault, r.FaultEvent) and fault.error.scope == "unknown"
        assert fault.error.code == "future_code"
        assert isinstance(ended, r.ResponseEnded) and ended.status == "unknown"
        assert ended.response == started.response
        assert not peer.closed
        await iterator.aclose()


async def test_one_ready_admits_one_turn_and_busy_needs_next_ready(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        first = asyncio.create_task(current.send_text("one"))
        request = await peer.next("response.create")
        second = asyncio.create_task(current.send_text("two"))
        peer.emit(
            "error",
            error={
                "code": "typed_turn_busy",
                "scope": "event",
                "message": "busy",
                "owner": {"kind": "event", "event_id": request["event_id"]},
            },
        )
        await asyncio.sleep(0.01)
        assert peer.sent.empty()
        peer.emit("prsm.typed_input.ready")
        retry = await peer.next("response.create")
        assert retry["event_id"] != request["event_id"]
        peer.accepted(retry)
        await first
        assert not second.done()
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)


async def test_cancel_before_ready_never_sends(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        task = asyncio.create_task(current.send_text("abandoned"))
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        peer.emit("prsm.typed_input.ready")
        await asyncio.sleep(0.01)
        assert peer.sent.empty()


async def test_cancel_after_send_cancels_late_response(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        task = asyncio.create_task(current.request_reply(instruction="hello"))
        request = await peer.next("response.create")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        peer.accepted(request)
        cancel = await peer.next("response.cancel")
        assert cancel["response_id"] == "reply-1"
        peer.ended(status="cancelled")


async def test_typed_turn_superseded_after_item_ack_matches_item_identity(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        pending = asyncio.create_task(current.send_text("superseded before generation"))
        request = await peer.next("response.create")
        peer.emit(
            "conversation.item.created",
            prsm_request_event_id=request["event_id"],
            item={"id": "typed-user-item", "type": "message", "role": "user"},
        )
        peer.emit(
            "error",
            error={
                "code": "typed_turn_superseded",
                "scope": "utterance",
                "message": "A new user turn superseded this typed turn",
                "owner": {"kind": "utterance", "item_id": "typed-user-item"},
            },
        )
        with pytest.raises(r.RimeRealtimeError) as error:
            await pending
        assert error.value.fault.code == "typed_turn_superseded"
        assert error.value.fault.item_id == "typed-user-item"
        assert error.value.fault.request_id is None
        assert (await accepted_turn(current, peer)).response_id == "reply-1"
        assert not peer.closed


async def test_unknown_outcome_closes_without_replay(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        task = asyncio.create_task(current.send_text("once"))
        await peer.next("response.create")
        with pytest.raises(RimeTimeoutError, match="outcome unknown"):
            await task
        with pytest.raises(RimeTimeoutError):
            await anext(current.events)
        assert peer.sent.empty()


async def test_clear_serializes_and_matches_ack_without_echo(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        one = asyncio.create_task(current.clear_audio())
        await peer.next("input_audio_buffer.clear")
        two = asyncio.create_task(current.clear_audio())
        await asyncio.sleep(0)
        assert peer.sent.empty()
        peer.emit("input_audio_buffer.cleared")
        await one
        await peer.next("input_audio_buffer.clear")
        assert not two.done()
        peer.emit("input_audio_buffer.cleared")
        await two


async def test_named_clear_refusal_is_matched(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        task = asyncio.create_task(current.clear_audio())
        request = await peer.next("input_audio_buffer.clear")
        peer.emit(
            "error",
            error={
                "code": "conflict",
                "scope": "event",
                "message": "no",
                "owner": {"kind": "event", "event_id": request["event_id"]},
            },
        )
        with pytest.raises(r.RimeRealtimeError) as caught:
            await task
        assert caught.value.fault.request_id == request["event_id"]


async def test_playback_uses_output_identity_and_waits_for_done(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        peer.emit(
            "response.output_item.added",
            response_id="reply-1",
            output_index=0,
            item={"id": "msg", "type": "message"},
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        output = event.payload.output
        truncate = asyncio.create_task(
            current.report_playback(r.PlaybackInterrupted(output=output, audio_end_ms=123))
        )
        request = await peer.next("conversation.item.truncate")
        assert request["content_index"] == 0
        peer.emit("conversation.item.truncated", item_id="msg", content_index=0, audio_end_ms=123)
        await truncate
        receipt = asyncio.create_task(
            current.report_playback(r.PlaybackFinished(response=ref, played_ms=123))
        )
        await asyncio.sleep(0)
        assert peer.sent.empty()
        peer.ended()
        await receipt
        assert (await peer.next("prsm.playback.drained"))["response_id"] == ref.response_id
        await iterator.aclose()


async def test_tools_wait_for_result_ack_and_parent_done(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id="reply-1",
            call_id="call-1",
            item_id="tool-1",
            name="lookup",
            arguments="{}",
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        call = event.payload.call
        continuation = asyncio.create_task(current.continue_reply(ref))
        result = asyncio.create_task(current.submit_tool_result(call, "answer"))
        request = await peer.next("conversation.item.create")
        peer.ended()
        await asyncio.sleep(0.01)
        assert peer.sent.empty()
        peer.emit(
            "conversation.item.created",
            prsm_request_event_id=request["event_id"],
            item={"id": "result"},
        )
        await result
        request = await peer.next("response.create")
        assert request["response"]["metadata"]["prsm_parent_response_id"] == ref.response_id
        peer.accepted(request, "reply-2")
        await continuation
        with pytest.raises(RimeInputError):
            await current.continue_reply(ref)
        await current.submit_tool_result(call, "answer")  # identical result needs no new request
        with pytest.raises(RimeInputError):
            await current.submit_tool_result(call, "different")
        await iterator.aclose()


async def test_refs_reject_other_session(monkeypatch):
    async with session(monkeypatch) as (current, _, _):
        with pytest.raises(RimeInputError, match="another session"):
            await current.cancel(r.ResponseRef(session_id="another", response_id="reply-1"))


@pytest.mark.parametrize(
    "refusal", ["tool_continuation_not_ready", "tool_continuation_unavailable", None]
)
async def test_continuation_retries_only_after_confirmed_not_ready(monkeypatch, refusal):
    async with session(monkeypatch) as (current, peer, _):
        parent = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            call_id="call",
            item_id="tool",
            name="lookup",
            arguments="{}",
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        result = asyncio.create_task(current.submit_tool_result(event.payload.call, "answer"))
        request = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prsm_request_event_id=request["event_id"],
            item={"id": "result"},
        )
        await result
        peer.ended()
        continuation = asyncio.create_task(current.continue_reply(parent))
        request = await peer.next("response.create")
        if refusal is None:
            with pytest.raises(RimeTimeoutError, match="outcome unknown"):
                await continuation
            with pytest.raises(RimeTimeoutError):
                await current.continue_reply(parent)
        else:
            peer.emit(
                "error",
                error={
                    "code": refusal,
                    "scope": "event",
                    "message": "continuation refused",
                    "owner": {"kind": "event", "event_id": request["event_id"]},
                },
            )
            with pytest.raises(r.RimeRealtimeError) as error:
                await continuation
            assert error.value.fault.code == refusal
            if refusal == "tool_continuation_not_ready":
                retry = asyncio.create_task(current.continue_reply(parent))
                retried = await peer.next("response.create")
                assert retried["event_id"] != request["event_id"]
                assert retried["response"] == request["response"]
                peer.accepted(retried, "continued")
                assert (await retry).response_id == "continued"
            else:
                with pytest.raises(RimeInputError, match="already continued"):
                    await current.continue_reply(parent)
        assert peer.sent.empty()
        await iterator.aclose()


async def completed_tool_round(current, peer):
    parent = await accepted_turn(current, peer)
    peer.emit(
        "response.function_call_arguments.done",
        response_id=parent.response_id,
        call_id="call",
        item_id="tool",
        name="lookup",
        arguments="{}",
    )
    iterator = current.events
    for _ in range(3):
        event = await anext(iterator)
    result = asyncio.create_task(current.submit_tool_result(event.payload.call, "answer"))
    request = await peer.next("conversation.item.create")
    peer.emit(
        "conversation.item.created",
        prsm_request_event_id=request["event_id"],
        item={"id": "result"},
    )
    await result
    peer.ended()
    async for event in iterator:
        if isinstance(event.payload, r.ResponseEnded):
            break
    await iterator.aclose()
    return parent


@pytest.mark.parametrize("turn_kind", ["text", "tools"])
@pytest.mark.parametrize("failure", ["operation_limit", "cancel_before_send"])
async def test_unsent_turn_can_be_retried(monkeypatch, turn_kind, failure):
    async with session(monkeypatch) as (current, peer, _):
        if turn_kind == "tools":
            parent = await completed_tool_round(current, peer)

            def start_turn():
                return current.continue_reply(parent)

        else:
            peer.emit("prsm.typed_input.ready")
            await current._ready.wait()

            def start_turn():
                return current.send_text("hello")

        if failure == "operation_limit":
            monkeypatch.setattr(_session, "_LIMIT", 1)
            clearing = asyncio.create_task(current.clear_audio())
            await peer.next("input_audio_buffer.clear")
            with pytest.raises(RimeResourceLimitError, match="Too many pending"):
                await start_turn()
            assert peer.sent.empty()
            peer.emit("input_audio_buffer.cleared")
            await clearing
        else:
            async with current._write_lock:
                waiting = asyncio.create_task(start_turn())
                async with asyncio.timeout(1):
                    while not current._pending:
                        await asyncio.sleep(0)
                waiting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiting
                assert peer.sent.empty()
            await asyncio.gather(*current._tasks, return_exceptions=True)

        retry = asyncio.create_task(start_turn())
        request = await peer.next("response.create")
        peer.accepted(request, "retried")
        assert (await retry).response_id == "retried"
        assert peer.sent.empty()
        assert not peer.closed


async def test_unsent_turn_does_not_restore_readiness_after_speech(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        async with current._write_lock:
            waiting = asyncio.create_task(current.send_text("cancelled"))
            async with asyncio.timeout(1):
                while not current._pending:
                    await asyncio.sleep(0)
            peer.emit("input_audio_buffer.speech_started", item_id="speech", audio_start_ms=0)
            async for event in current.events:
                if isinstance(event.payload, r.SpeechStarted):
                    break
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
        await asyncio.gather(*current._tasks, return_exceptions=True)
        assert not current._ready.is_set()
        assert peer.sent.empty()
        await accepted_turn(current, peer)


async def test_cancelled_submitted_continuation_cannot_be_retried(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent = await completed_tool_round(current, peer)
        continuation = asyncio.create_task(current.continue_reply(parent))
        request = await peer.next("response.create")
        continuation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await continuation
        with pytest.raises(RimeInputError, match="already continued"):
            await current.continue_reply(parent)
        assert peer.sent.empty()
        peer.accepted(request, "continued")
        cancel = await peer.next("response.cancel")
        assert cancel["response_id"] == "continued"
        peer.ended("continued", status="cancelled")


async def test_audio_conversion_is_stateful_and_chunked(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        chunk = r.AudioChunk(
            data=b"\x01\x00" * 4800, format=r.PCMFormat(sample_rate=48000, channels=2)
        )
        await current.send_audio(chunk)
        audio = b""
        while not peer.sent.empty():
            event = await peer.next("input_audio_buffer.append")
            data = base64.b64decode(event["audio"])
            assert len(data) <= 1280
            audio += data
        assert len(audio) == 1600
        assert audio == b"\x01\x00" * 800


async def test_resampling_preserves_audio_across_chunk_boundaries(monkeypatch):
    data = b"".join(value.to_bytes(2, "little", signed=True) for value in range(-500, 500))
    converted = []
    for chunks in ([data], [data[:2], data[2:438], data[438:]]):
        async with session(monkeypatch) as (current, peer, _):
            for chunk in chunks:
                await current.send_audio(
                    r.AudioChunk(data=chunk, format=r.PCMFormat(sample_rate=24000))
                )
            output = b""
            while not peer.sent.empty():
                event = await peer.next("input_audio_buffer.append")
                output += base64.b64decode(event["audio"])
            converted.append(output)
    assert converted[0] == converted[1]


async def test_cancelled_partial_audio_send_closes_without_replay(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        original_send = peer.send
        second_append = asyncio.Event()
        appends = 0

        async def blocked_send(raw):
            nonlocal appends
            appends += 1
            if appends == 2:
                second_append.set()
                await asyncio.Event().wait()
            await original_send(raw)

        monkeypatch.setattr(peer, "send", blocked_send)
        chunk = r.AudioChunk(data=b"\x01\x00" * 2400, format=r.PCMFormat(sample_rate=24000))
        sending = asyncio.create_task(current.send_audio(chunk))
        await asyncio.wait_for(second_append.wait(), 1)
        assert (
            len(base64.b64decode((await peer.next("input_audio_buffer.append"))["audio"])) == 1280
        )
        sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        with pytest.raises(RimeStreamError, match="audio submission cancelled; outcome unknown"):
            await anext(current.events)
        with pytest.raises(RimeStreamError):
            await current.send_audio(chunk)
        assert peer.sent.empty()
        await current.close()
        assert peer.closed


async def test_cancelling_audio_waiter_preserves_the_active_send(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        original_send = peer.send
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_send(raw):
            started.set()
            await release.wait()
            await original_send(raw)

        monkeypatch.setattr(peer, "send", blocked_send)
        first = asyncio.create_task(current.send_audio(r.AudioChunk(data=b"\x01\x00")))
        await started.wait()
        waiting = asyncio.create_task(current.send_audio(r.AudioChunk(data=b"\x02\x00")))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        release.set()
        await first
        await current.send_audio(r.AudioChunk(data=b"\x03\x00"))
        audio = [
            base64.b64decode((await peer.next("input_audio_buffer.append"))["audio"])
            for _ in range(2)
        ]
        assert audio == [b"\x01\x00", b"\x03\x00"]
        assert peer.sent.empty()
        assert not peer.closed


@pytest.mark.parametrize("status", ["completed", "cancelled", "failed"])
async def test_tools_and_terminal_are_once_and_output_identity_is_preserved(monkeypatch, status):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        for index in (2, 4):
            peer.emit(
                "response.output_item.added",
                response_id=ref.response_id,
                output_index=index,
                item={"id": f"message-{index}", "type": "message"},
            )
            peer.emit(
                "response.text.delta",
                response_id=ref.response_id,
                item_id=f"message-{index}",
                output_index=index,
                content_index=0,
                delta=str(index),
            )
            for call_id in ("first", "second", "first"):
                peer.emit(
                    "response.function_call_arguments.done",
                    response_id=ref.response_id,
                    item_id=f"tool-{call_id}",
                    output_index=5 if call_id == "first" else 6,
                    call_id=call_id,
                    name="lookup",
                    arguments='{"value": 7}',
                )
        peer.ended(status=status)
        peer.ended(status=status)
        # This event also proves the connection remains usable after completion.
        peer.emit("prsm.typed_input.ready")
        iterator = current.events
        events = []
        while True:
            payload = (await anext(iterator)).payload
            events.append(payload)
            if len(events) > 1 and isinstance(payload, r.InputReady):
                break
        calls = [event for event in events if isinstance(event, r.ToolCall)]
        assert [event.call.call_id for event in calls] == ["first", "second"]
        assert all(event.call.response_id == ref.response_id for event in calls)
        assert all(event.arguments == {"value": 7} for event in calls)
        terminals = [event for event in events if isinstance(event, r.ResponseEnded)]
        assert len(terminals) == 1
        assert terminals[0].response == ref
        assert terminals[0].status == status
        messages = [event.output for event in events if isinstance(event, r.MessageStarted)]
        deltas = [event.output for event in events if isinstance(event, r.TextDelta)]
        assert messages == deltas
        assert [(output.item_id, output.output_index) for output in messages] == [
            ("message-2", 2),
            ("message-4", 4),
        ]
        assert all(output.response == ref and output.content_index == 0 for output in messages)
        await iterator.aclose()


async def test_slow_consumer_fails_instead_of_dropping(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        for _ in range(257):
            peer.emit("prsm.typed_input.ready")
        await asyncio.sleep(0.02)
        with pytest.raises(RimeResourceLimitError):
            await anext(current.events)


async def test_transport_loss_fails_pending_and_iterator(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        task = asyncio.create_task(current.clear_audio())
        await peer.next("input_audio_buffer.clear")
        await peer.close()
        with pytest.raises(RimeStreamError):
            await task
        with pytest.raises(RimeStreamError):
            await anext(current.events)


async def test_owner_close_closes_sessions(monkeypatch):
    async with session(monkeypatch) as (current, peer, client):
        await client.close()
        assert peer.closed
        await current.close()
        with pytest.raises(RimeInputError):
            await current.send_text("closed")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1])
def test_invalid_playback_times(value):
    with pytest.raises(RimeInputError):
        r.PlaybackFinished(response=r.ResponseRef(session_id="s", response_id="r"), played_ms=value)


async def test_cancelled_clear_keeps_its_ack_slot(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        first = asyncio.create_task(current.clear_audio())
        await peer.next("input_audio_buffer.clear")
        first.cancel()
        second = asyncio.create_task(current.clear_audio())
        await asyncio.sleep(0.01)
        assert peer.sent.empty()
        peer.emit("input_audio_buffer.cleared")
        await asyncio.gather(first, return_exceptions=True)
        await peer.next("input_audio_buffer.clear")
        assert not second.done()
        peer.emit("input_audio_buffer.cleared")
        await second


@pytest.mark.parametrize("outcome", ["acknowledged", "refused", "unsent"])
async def test_cancelled_clear_resets_conversion_only_after_ack(monkeypatch, outcome):
    prefix = r.AudioChunk(data=b"\x01\x00" * 2, format=r.PCMFormat(sample_rate=24000))
    following = r.AudioChunk(
        data=b"".join(value.to_bytes(2, "little") for value in range(100, 108)),
        format=prefix.format,
    )
    async with session(monkeypatch) as (current, peer, _):
        await current.send_audio(prefix)
        await peer.next("input_audio_buffer.append")
        if outcome == "unsent":
            original_send = peer.send
            release = asyncio.Event()

            async def blocked_send(raw):
                await original_send(raw)
                await release.wait()

            monkeypatch.setattr(peer, "send", blocked_send)
            history = asyncio.create_task(current.add_message("user", "history"))
            request = await peer.next("conversation.item.create")
            clearing = asyncio.create_task(current.clear_audio())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            clearing.cancel()
            await asyncio.sleep(0)
            release.set()
            peer.emit(
                "conversation.item.created",
                prsm_request_event_id=request["event_id"],
                item={"id": "history-item"},
            )
            await history
        else:
            clearing = asyncio.create_task(current.clear_audio())
            request = await peer.next("input_audio_buffer.clear")
            clearing.cancel()
            await asyncio.sleep(0)
            if outcome == "acknowledged":
                peer.emit("input_audio_buffer.cleared")
            else:
                peer.emit(
                    "error",
                    error={
                        "code": "clear_refused",
                        "scope": "event",
                        "message": "No change",
                        "owner": {"kind": "event", "event_id": request["event_id"]},
                    },
                )
        with pytest.raises(asyncio.CancelledError):
            await clearing
        assert peer.sent.empty()
        await current.send_audio(following)
        actual = (await peer.next("input_audio_buffer.append"))["audio"]
        assert not peer.closed
    async with session(monkeypatch) as (reference, peer, _):
        if outcome != "acknowledged":
            await reference.send_audio(prefix)
            await peer.next("input_audio_buffer.append")
        await reference.send_audio(following)
        assert actual == (await peer.next("input_audio_buffer.append"))["audio"]


async def test_late_tool_result_is_recorded_and_reported_by_call_id(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        old = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=old.response_id,
            call_id="late",
            item_id="tool",
            name="lookup",
            arguments="{}",
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        call = event.payload.call
        peer.ended()
        peer.emit("input_audio_buffer.speech_started", item_id="new-user", audio_start_ms=10)
        peer.emit("response.created", response={"id": "new-response", "metadata": {}})
        peer.ended("new-response")
        result = asyncio.create_task(current.submit_tool_result(call, "late answer"))
        request = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prsm_request_event_id=request["event_id"],
            item={"id": "result"},
        )
        await result
        with pytest.raises(RimeInputError, match="superseded"):
            await current.continue_reply(old)
        peer.emit("prsm.typed_input.ready")
        report = asyncio.create_task(current.request_reply(tool_call=call))
        request = await peer.next("response.create")
        assert request["response"]["prsm_call_id"] == "late"
        peer.accepted(request, "report")
        await report
        await iterator.aclose()


async def test_cancelled_tool_result_still_updates_round_after_ack(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            call_id="call",
            item_id="tool",
            name="lookup",
            arguments="{}",
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        call = event.payload.call
        result = asyncio.create_task(current.submit_tool_result(call, "answer"))
        request = await peer.next("conversation.item.create")
        result.cancel()
        await asyncio.gather(result, return_exceptions=True)
        peer.emit(
            "conversation.item.created",
            prsm_request_event_id=request["event_id"],
            item={"id": "result"},
        )
        peer.ended()
        continuation = asyncio.create_task(current.continue_reply(parent))
        request = await peer.next("response.create")
        peer.accepted(request, "continued")
        await continuation
        await iterator.aclose()


async def test_response_error_does_not_finish_generation(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        peer.emit(
            "error",
            error={
                "code": "no_audio",
                "message": "failed",
                "scope": "response",
                "owner": {"kind": "response", "response_id": ref.response_id},
            },
        )
        iterator = current.events
        for _ in range(3):
            event = await anext(iterator)
        assert isinstance(event.payload, r.FaultEvent)
        next_event = asyncio.create_task(anext(iterator))
        await asyncio.sleep(0)
        assert not next_event.done()
        peer.ended(status="failed")
        assert isinstance((await next_event).payload, r.ResponseEnded)
        await iterator.aclose()


async def test_transcription_failure_is_scoped_to_its_utterance(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit(
            "conversation.item.input_audio_transcription.failed",
            item_id="user-without-transcript",
            content_index=0,
            error={
                "type": "transcription_failed",
                "code": "no_transcript",
                "message": "No transcript was produced",
            },
        )
        iterator = current.events
        failed = (await anext(iterator)).payload
        assert isinstance(failed, r.TranscriptFailed)
        assert failed.error.scope == "utterance"
        assert failed.error.item_id == failed.item_id == "user-without-transcript"
        assert failed.error.code == "no_transcript"
        assert failed.error.message == "No transcript was produced"
        assert (await accepted_turn(current, peer)).response_id == "reply-1"
        await iterator.aclose()


async def test_second_event_consumer_is_rejected(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        first = current.events
        await anext(first)
        with pytest.raises(RimeInputError, match="one consumer"):
            await anext(current.events)
        await first.aclose()


async def test_ready_timeout_does_not_close_idle_session(monkeypatch):
    async with session(monkeypatch, timeouts=r.RealtimeTimeouts(ready_s=0.01)) as (
        current,
        peer,
        _,
    ):
        with pytest.raises(r.RealtimeAdmissionTimeout, match="ready_s"):
            await current.send_text("wait")
        assert not peer.closed
        await accepted_turn(current, peer)


async def test_client_close_during_connect_does_not_cancel_application_owner(monkeypatch):
    started = asyncio.Event()

    async def connect(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(_client, "connect", connect)
    client = Rime(api_key="test")

    async def application():
        async with client, client.realtime.connect(endpoint="ws://localhost/v1/realtime"):
            pytest.fail("Connection must not finish")

    owner = asyncio.create_task(application())
    await started.wait()
    await asyncio.wait_for(client.close(), 1)
    result = await asyncio.wait_for(asyncio.gather(owner, return_exceptions=True), 1)
    assert isinstance(result[0], asyncio.CancelledError)


async def test_concurrent_cancel_and_playback_stop_share_one_cancel(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        first = asyncio.create_task(current.cancel(ref))
        await peer.next("response.cancel")

        async def stopped():
            await current.cancel(ref)
            await current.report_playback(r.PlaybackFinished(response=ref, played_ms=0))

        playback = asyncio.create_task(stopped())
        await asyncio.sleep(0.01)
        assert peer.sent.empty()
        peer.ended(status="cancelled")
        await asyncio.gather(first, playback)
        receipt = await peer.next("prsm.playback.drained")
        assert receipt["response_id"] == ref.response_id
        assert peer.sent.empty()


async def test_cancel_refusal_is_correlated_and_keeps_session_usable(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        cancelling = asyncio.create_task(current.cancel(ref))
        request = await peer.next("response.cancel")
        peer.emit(
            "error",
            error={
                "code": "response_cancel_not_active",
                "scope": "event",
                "message": "The response already finished",
                "owner": {"kind": "event", "event_id": request["event_id"]},
            },
        )
        with pytest.raises(r.RimeRealtimeError) as error:
            await cancelling
        assert error.value.fault.request_id == request["event_id"]
        assert error.value.fault.code == "response_cancel_not_active"
        peer.ended()
        iterator = current.events
        while not isinstance((await anext(iterator)).payload, r.ResponseEnded):
            pass
        await current.cancel(ref)
        assert peer.sent.empty()
        assert not peer.closed
        await iterator.aclose()


async def test_completion_wins_cancel_race_without_changing_terminal_status(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        ref = await accepted_turn(current, peer)
        cancelling = asyncio.create_task(current.cancel(ref))
        request = await peer.next("response.cancel")
        peer.ended(status="completed")
        await cancelling
        peer.emit(
            "error",
            error={
                "code": "response_cancel_not_active",
                "scope": "event",
                "message": "The response already finished",
                "owner": {"kind": "event", "event_id": request["event_id"]},
            },
        )
        peer.emit("prsm.typed_input.ready")
        next_turn = asyncio.create_task(current.send_text("next turn"))
        next_request = await peer.next("response.create")
        peer.accepted(next_request, "reply-2")
        assert (await next_turn).response_id == "reply-2"
        iterator = current.events
        events = []
        while True:
            payload = (await anext(iterator)).payload
            events.append(payload)
            if isinstance(payload, r.ResponseStarted) and payload.response.response_id == "reply-2":
                break
        terminals = [event for event in events if isinstance(event, r.ResponseEnded)]
        assert len(terminals) == 1
        assert terminals[0].response == ref
        assert terminals[0].status == "completed"
        refusals = [event for event in events if isinstance(event, r.FaultEvent)]
        assert len(refusals) == 1
        assert refusals[0].error.request_id == request["event_id"]
        assert refusals[0].error.code == "response_cancel_not_active"
        assert not peer.closed
        await iterator.aclose()


@pytest.mark.parametrize("changed_format", [False, True])
async def test_audio_cancel_before_write_restores_conversion(monkeypatch, changed_format):
    pcm = r.PCMFormat(sample_rate=24000)
    before = r.AudioChunk(data=b"\x01\x00" * 5, format=pcm)
    after = r.AudioChunk(data=b"\x03\x00" * 7, format=pcm)
    outputs = []
    for cancel_chunk in (False, True):
        async with session(monkeypatch) as (current, peer, _):
            await current.send_audio(before)
            if cancel_chunk:
                await current._write_lock.acquire()
                try:
                    waiting = asyncio.create_task(
                        current.send_audio(
                            r.AudioChunk(
                                data=b"\x02\x00" * 17,
                                format=r.PCMFormat(sample_rate=48000) if changed_format else pcm,
                            )
                        )
                    )
                    await asyncio.sleep(0)
                    waiting.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiting
                finally:
                    current._write_lock.release()
            await current.send_audio(after)
            audio = b""
            while not peer.sent.empty():
                audio += base64.b64decode((await peer.next("input_audio_buffer.append"))["audio"])
            outputs.append(audio)
            assert not peer.closed
            await accepted_turn(current, peer)
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize("accepted_before_cancel", [False, True])
async def test_abandoned_response_notice_is_once_and_does_not_report_playback(
    monkeypatch, accepted_before_cancel
):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prsm.typed_input.ready")
        task = asyncio.create_task(current.send_text("hello"))
        request = await peer.next("response.create")
        if accepted_before_cancel:
            # Dispatch acceptance synchronously, then cancel before the caller
            # receives the reference. This exercises the completion race.
            current._dispatch(
                {
                    "type": "response.created",
                    "prsm_request_event_id": request["event_id"],
                    "response": {"id": "reply-1", "metadata": {}},
                }
            )
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if not accepted_before_cancel:
            peer.accepted(request)
        await peer.next("response.cancel")
        peer.ended(status="cancelled")
        notices = []
        async for event in current.events:
            if isinstance(event.payload, r.ResponseAbandoned):
                notices.append(event.payload)
            if isinstance(event.payload, r.ResponseEnded):
                break
        assert notices == [
            r.ResponseAbandoned(
                response=r.ResponseRef(session_id="session-1", response_id="reply-1")
            )
        ]
        assert peer.sent.empty()  # The SDK does not decide whether audio was played.


async def test_request_ack_deadline_is_independent_of_admission_deadline(monkeypatch):
    async with session(monkeypatch, timeouts=r.RealtimeTimeouts(ready_s=0.01, request_s=0.2)) as (
        current,
        peer,
        _,
    ):
        peer.emit("prsm.typed_input.ready")
        task = asyncio.create_task(current.send_text("hello"))
        request = await peer.next("response.create")
        await asyncio.sleep(0.03)
        assert not task.done()
        peer.accepted(request)
        assert (await task).response_id == "reply-1"


async def test_unknown_request_outcome_is_not_an_admission_timeout(monkeypatch):
    async with session(monkeypatch, timeouts=r.RealtimeTimeouts(ready_s=0.01, request_s=0.04)) as (
        current,
        peer,
        _,
    ):
        peer.emit("prsm.typed_input.ready")
        task = asyncio.create_task(current.send_text("hello"))
        await peer.next("response.create")
        with pytest.raises(RimeTimeoutError) as raised:
            await task
        assert not isinstance(raised.value, r.RealtimeAdmissionTimeout)
        with pytest.raises(RimeTimeoutError):
            await current.send_text("next")
