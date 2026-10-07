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
