"""Cleanup and cancellation at realtime ownership boundaries."""

import asyncio

import grpc
import pytest
from test_realtime import Peer, completed_tool_round, session

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk.realtime import _client, _session


@pytest.mark.parametrize("initialization_fails", [False, True])
async def test_cancelled_context_cleanup_remains_tracked(monkeypatch, initialization_fails):
    peer = Peer()
    close_started = asyncio.Event()
    release_close = asyncio.Event()
    close_socket = peer.close
    initialize = _session.RealtimeSession._initialize

    async def connect(*args, **kwargs):
        return peer

    async def slow_close():
        close_started.set()
        await release_close.wait()
        await close_socket()

    async def failing_initialize(current, settings):
        await initialize(current, settings)
        raise ValueError("initialization failed")

    monkeypatch.setattr(_client, "connect", connect)
    monkeypatch.setattr(peer, "close", slow_close)
    if initialization_fails:
        monkeypatch.setattr(_session.RealtimeSession, "_initialize", failing_initialize)
    client = Rime(api_key="test")

    async def application():
        async with client.realtime.connect(endpoint="ws://localhost/v1/realtime"):
            pass

    owner = asyncio.create_task(application())
    try:
        await asyncio.wait_for(close_started.wait(), 1)
        current = next(iter(client.realtime._sessions))
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert current in client.realtime._sessions
        assert not current._close_task.done()

        rejoined = asyncio.Event()
        close_session = current.close

        async def observed_close():
            rejoined.set()
            await close_session()

        monkeypatch.setattr(current, "close", observed_close)
        closing = asyncio.create_task(client.close())
        await asyncio.wait_for(rejoined.wait(), 1)
        assert not closing.done()
        release_close.set()
        await asyncio.wait_for(closing, 1)
        assert peer.closed
        assert current._reader.done()
        assert not client.realtime._sessions
    finally:
        release_close.set()
        await client.close()
        await asyncio.gather(owner, return_exceptions=True)


async def test_cancel_after_opening_closes_undelivered_session(monkeypatch):
    peer = Peer()
    initialize = _session.RealtimeSession._initialize

    async def connect(*args, **kwargs):
        return peer

    async def cancel_after_initialization(current, settings):
        await initialize(current, settings)
        # Runs after opening succeeds, before its result reaches the caller.
        asyncio.get_running_loop().call_soon(owner.cancel)

    monkeypatch.setattr(_client, "connect", connect)
    monkeypatch.setattr(_session.RealtimeSession, "_initialize", cancel_after_initialization)
    async with Rime(api_key="test") as client:

        async def application():
            async with client.realtime.connect(endpoint="ws://localhost/v1/realtime"):
                pytest.fail("A cancelled caller must not receive the session")

        owner = asyncio.create_task(application())
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert peer.closed
        assert not client.realtime._sessions
        assert not client.realtime._opening


@pytest.mark.parametrize(
    "refusal", ["tool_continuation_not_ready", "tool_continuation_unavailable"]
)
async def test_cancelled_continuation_handles_late_refusal(monkeypatch, refusal):
    async with session(monkeypatch) as (current, peer, _):
        parent = await completed_tool_round(current, peer)
        continuation = asyncio.create_task(current.continue_reply(parent))
        request = await peer.next("response.create")
        continuation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await continuation
        background = list(current._tasks)
        peer.emit(
            "error",
            error={
                "code": refusal,
                "scope": "event",
                "message": "continuation refused",
                "owner": {"kind": "event", "event_id": request["event_id"]},
            },
        )
        await asyncio.gather(*background, return_exceptions=True)
        if refusal == "tool_continuation_not_ready":
            retry = asyncio.create_task(current.continue_reply(parent))
            try:
                retried = await peer.next("response.create")
                assert retried["event_id"] != request["event_id"]
                peer.accepted(retried, "reply-2")
                assert (await retry).response_id == "reply-2"
            finally:
                await asyncio.gather(retry, return_exceptions=True)
        else:
            with pytest.raises(RimeInputError, match="already continued"):
                await current.continue_reply(parent)


async def test_socket_close_error_still_cleans_tts_and_credentials(setup, monkeypatch):
    _, client = setup
    peer = Peer()
    failure = OSError("socket close failed")

    async def connect(*args, **kwargs):
        return peer

    close_socket = peer.close

    async def failing_close():
        await close_socket()
        raise failure

    monkeypatch.setattr(_client, "connect", connect)
    context = client.realtime.connect(endpoint="ws://localhost/v1/realtime")
    current = await context.__aenter__()
    source_waiting = asyncio.Event()
    source_closed = asyncio.Event()

    async def source():
        try:
            source_waiting.set()
            await asyncio.Event().wait()
            yield "never"
        finally:
            source_closed.set()

    audio = client.tts.stream(source())
    reading = asyncio.create_task(anext(audio))
    await asyncio.wait_for(source_waiting.wait(), 1)
    monkeypatch.setattr(peer, "close", failing_close)
    try:
        with pytest.raises(OSError) as caught:
            await client.close()
        assert caught.value is failure
        assert source_closed.is_set()
        assert current._reader.done()
        assert client.tts._channel.get_state() == grpc.ChannelConnectivity.SHUTDOWN
        assert client._credentials._key == ""
        assert client._credentials._token is None
    finally:
        # Restore teardown after the intentionally failed, cached close.
        monkeypatch.setattr(peer, "close", close_socket)
        await current._shutdown()
        await client.tts._close()
        await client._credentials.close()
        await asyncio.gather(reading, return_exceptions=True)
        with pytest.raises(OSError):
            await context.__aexit__(None, None, None)
        client._close_task = None


async def test_tts_close_error_still_clears_credentials(monkeypatch):
    client = Rime(api_key="test")
    close_tts = client.tts._close

    async def failing_close():
        await close_tts()
        raise OSError("TTS close failed")

    monkeypatch.setattr(client.tts, "_close", failing_close)
    with pytest.raises(OSError, match="TTS close failed"):
        await client.close()
    assert client._credentials._closed
    assert client._credentials._key == ""


async def _observed_response(current, peer, number, *, tool=False):
    from rimelabs_sdk import realtime as r

    response_id = f"retained-{number}"
    peer.emit("response.created", response={"id": response_id})
    if tool:
        peer.emit(
            "response.function_call_arguments.done",
            response_id=response_id,
            item_id=f"item-{number}",
            call_id=f"call-{number}",
            name="lookup",
            arguments="{}",
        )
    peer.ended(response_id)
    ref = call = None
    from contextlib import aclosing

    async with aclosing(current.events) as events:
        async for event in events:
            if isinstance(event.payload, r.ResponseStarted):
                ref = event.payload.response
            if isinstance(event.payload, r.ToolCall):
                call = event.payload.call
            if (
                isinstance(event.payload, r.ResponseEnded)
                and event.payload.response.response_id == response_id
            ):
                return ref, call


async def test_public_holds_keep_playback_after_ordinary_cache_expiry(monkeypatch):
    from rimelabs_sdk import realtime as r

    async with session(monkeypatch) as (current, peer, _):
        old, _ = await _observed_response(current, peer, 0)
        first = current.hold_response(old)
        with current.hold_response(old):
            first.release()
            first.release()
            for number in range(1, 140):
                await _observed_response(current, peer, number)
            await current.report_playback(r.PlaybackFinished(response=old, played_ms=0))
            receipt = await peer.next("prism.playback.drained")
            assert receipt["response_id"] == old.response_id
        with pytest.raises(RimeInputError, match="expired"):
            current.hold_response(old)


async def test_duplicate_end_after_active_response_becomes_expired(monkeypatch):
    from contextlib import aclosing

    from rimelabs_sdk import realtime as r

    async with session(monkeypatch) as (current, peer, _):
        peer.emit("response.created", response={"id": "long-running"})
        async with aclosing(current.events) as events:
            started = await anext(events)
            assert isinstance(started.payload, r.ResponseStarted)
            old = started.payload.response
        for number in range(128):
            await _observed_response(current, peer, number)
        peer.ended(old.response_id)
        async with aclosing(current.events) as events:
            ended = await anext(events)
            assert isinstance(ended.payload, r.ResponseEnded)
            assert ended.payload.response == old
        with pytest.raises(RimeInputError, match="expired"):
            current.hold_response(old)
        peer.ended(old.response_id)
        # The duplicate must not fail the stream or produce a second completion.
        peer.emit("prism.typed_input.ready")
        async with aclosing(current.events) as events:
            ready = await anext(events)
            assert isinstance(ready.payload, r.InputReady)
        reply = asyncio.create_task(current.request_reply())
        request = await peer.next("response.create")
        peer.accepted(request, "still-usable")
        assert (await reply).response_id == "still-usable"


async def test_pending_tool_owner_survives_then_can_report_once(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent, call = await _observed_response(current, peer, 0, tool=True)
        for number in range(1, 140):
            await _observed_response(current, peer, number)
        with current.hold_response(parent):
            submitted = asyncio.create_task(current.submit_tool_result(call, "done"))
            request = await peer.next("conversation.item.create")
            peer.emit(
                "conversation.item.created",
                prism_request_event_id=request["event_id"],
                item={"type": "function_call_output", "call_id": call.call_id},
            )
            await submitted
            await current.submit_tool_result(call, "done")
            assert peer.sent.empty()
            peer.emit("prism.typed_input.ready")
            reply = asyncio.create_task(current.request_reply(tool_call=call))
            request = await peer.next("response.create")
            assert request["response"]["prism_call_id"] == call.call_id
            peer.accepted(request, "report")
            await reply
        with pytest.raises(RimeInputError, match="expired"):
            current.hold_response(parent)


@pytest.mark.parametrize("overflow", ["response", "late_tool"])
async def test_protected_limit_nested_release_and_close(monkeypatch, overflow):
    from rimelabs_sdk import RimeResourceLimitError

    async with session(monkeypatch) as (current, peer, _):
        ordinary, _ = await _observed_response(current, peer, -1)
        holds = []
        # Leave space for the next active generation as well as existing holds.
        for number in range(127):
            ref, _ = await _observed_response(current, peer, number)
            holds.append(current.hold_response(ref))
        last = current.hold_response(ordinary)
        with current.hold_response(ordinary):
            last.release()
            assert len(current._responses) == 128
        await current.close()
        for hold in holds:
            hold.release()
            hold.release()
        assert not current._responses
        assert not current._response_interruptions
        with pytest.raises(RimeInputError, match="closed"):
            current.hold_response(ordinary)

    async with session(monkeypatch) as (current, peer, _):
        ordinary, _ = await _observed_response(current, peer, -1)
        for number in range(128):
            protected, _ = await _observed_response(current, peer, number, tool=True)
        with pytest.raises(RimeResourceLimitError, match="protected"):
            current.hold_response(ordinary)
        # A nested hold on an already-protected response needs no extra slot.
        nested = current.hold_response(protected)
        nested.release()
        nested.release()
        assert peer.sent.empty()
        # A local hold refusal leaves the connection and old ordinary reference usable.
        from rimelabs_sdk import realtime as r

        await current.report_playback(r.PlaybackFinished(response=ordinary, played_ms=0))
        receipt = await peer.next("prism.playback.drained")
        assert receipt["response_id"] == ordinary.response_id
        if overflow == "response":
            peer.emit("response.created", response={"id": "overflow"})
        else:
            peer.emit(
                "response.function_call_arguments.done",
                response_id=ordinary.response_id,
                item_id="overflow-item",
                call_id="overflow-call",
                name="lookup",
                arguments="{}",
            )
        with pytest.raises(RimeResourceLimitError, match="protected"):
            await anext(current.events)
        assert "overflow" not in current._responses
        assert len(current._responses) == 129  # 128 protected plus the ordinary response
        assert len(current._response_interruptions) == 129
        assert not current._responses[ordinary.response_id].calls
