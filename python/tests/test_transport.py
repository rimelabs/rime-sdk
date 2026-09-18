"""Transport behavior through real gRPC, without changing stream workers."""

import asyncio

import grpc
import pytest
from fake_service import FakeService

from rime_sdk import (
    RimeAudioFormatError,
    RimeAuthenticationError,
    RimePermissionError,
    RimeUnavailableError,
)
from rime_sdk._transport import SynthesisCall, discover


@pytest.fixture
async def connection():
    async with FakeService() as server, grpc.aio.insecure_channel(server.target) as channel:
        yield server, channel


@pytest.mark.parametrize(
    "status,error_type",
    [
        (grpc.StatusCode.UNAVAILABLE, RimeUnavailableError),
        (grpc.StatusCode.UNAUTHENTICATED, RimeAuthenticationError),
        (grpc.StatusCode.PERMISSION_DENIED, RimePermissionError),
    ],
)
async def test_write_after_rejection_without_reader(connection, status, error_type):
    server, channel = connection
    server.mode = "error_before_audio"
    server.rejection_status = status
    call = SynthesisCall(channel, ())
    try:
        await call.start("clementine", "en")
        async with asyncio.timeout(2):
            while not call.done():
                await asyncio.sleep(0.001)
        with pytest.raises(error_type) as caught:
            await call.write("Hello.")
        assert caught.value.request_id == "rejected-request"
        assert call.request_id == "rejected-request"
        assert len(server.calls) == 1
    finally:
        call.cancel()


@pytest.mark.parametrize("mode", ["odd_chunks", "headers_after_text"])
async def test_audio_and_wire_order(connection, mode):
    server, channel = connection
    server.mode = mode
    server.trailing_metadata = (("x-request-id", "trailer-id"),)
    call = SynthesisCall(channel, (("authorization", "Bearer test-token"),))
    try:
        async with asyncio.timeout(2):
            await call.start("voice", "de")
            await call.write("Hello.")
        await call.finish_input()
        chunks = [part async for part in call.audio()]
        assert b"".join(chunks) == server.payload
        if mode == "odd_chunks":
            assert chunks == [server.payload[:1], server.payload[1:]]
        assert call.request_id == "test-request"
        assert len(server.calls) == 1
        assert [m.WhichOneof("payload") for m in server.calls[0]] == ["header", "text_chunk"]
        assert server.calls[0][0].header.speaker == "voice"
        assert server.calls[0][0].header.language == "de"
        assert server.calls[0][0].header.audio_parameters.audio_format == "audio/pcm"
        assert server.metadata[0]["authorization"] == "Bearer test-token"
    finally:
        call.cancel()


@pytest.mark.parametrize("mode", ["empty_no_headers", "empty_audio"])
async def test_empty_success_requires_audio_metadata(connection, mode):
    server, channel = connection
    server.mode = mode
    server.response_metadata = ()
    call = SynthesisCall(channel, ())
    try:
        await call.start("voice", "en")
        await call.finish_input()
        with pytest.raises(RimeAudioFormatError):
            await anext(call.audio())
    finally:
        call.cancel()


async def test_discovery_transport_does_not_retry(connection):
    server, channel = connection
    server.discovery_failures = 1
    with pytest.raises(RimeUnavailableError):
        await discover(channel, (), "languages", None, 1)
    assert server.discovery_calls == 1
