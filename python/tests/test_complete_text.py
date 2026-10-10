"""Full-text synthesis must select the unary-input RPC, preserving SDK behavior."""

import asyncio
from dataclasses import replace

import grpc
import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import (
    AudioFormat,
    PronunciationEntry,
    Rime,
    RimeCancelledError,
    RimeInputError,
    RimeTimeoutError,
    RimeUnavailableError,
)


async def collect(stream):
    return b"".join([part async for part in stream])


async def test_full_text_is_one_request_with_lexicon_snapshot(setup):
    service, client = setup
    entries = [PronunciationEntry("read", '" r\\ E d')]
    stream = client.tts.stream(
        "Hello. Please read the pages.", complete_text=True, custom_lexicon=entries
    )
    entries.clear()
    assert b"".join([part async for part in stream]) == service.payload
    assert len(service.complete_calls) == 1
    request = service.complete_calls[0]
    assert request.text == "Hello. Please read the pages."
    assert request.custom_lexicon[0].spelling == "read"
    assert request.audio_parameters.sampling_rate == 24000
    assert stream.request_id == "test-request"
    assert service.metadata[0]["authorization"] == "Bearer test-token"


@pytest.mark.parametrize("mode", ["error_before_audio", "no_audio_error"])
async def test_full_text_pronunciation_rejection_and_recovery(setup, mode):
    service, client = setup
    service.mode = mode
    service.rejection_status = grpc.StatusCode.INVALID_ARGUMENT
    service.rejection_message = 'custom-lexicon entry "hello": no-primary-stress'
    service.release.set()
    with pytest.raises(RimeInputError, match="no-primary-stress") as caught:
        async for _ in client.tts.stream("Hello.", complete_text=True):
            pytest.fail("Unexpected audio")
    assert caught.value.request_id
    assert len(service.complete_calls) == 1
    service.mode = "normal"
    assert b"".join([part async for part in client.tts.stream("Hello.", complete_text=True)])


async def test_full_text_cancellation_and_input_validation(setup):
    service, client = setup

    async def source():
        yield "Hello."

    for text, flag in [(source(), True), ("é" * 32769, True), ("Hello.", "yes")]:
        with pytest.raises(RimeInputError):
            client.tts.stream(text, complete_text=flag)
    assert not service.calls
    service.mode = "silence"
    stream = client.tts.stream("Hello.", complete_text=True)
    reading = asyncio.create_task(anext(stream))
    await asyncio.wait_for(service.headers_sent.wait(), 1)
    await stream.cancel()
    with pytest.raises(RimeCancelledError):
        await reading


async def test_full_text_timestamps_share_audio_completion_contract(setup):
    service, _ = setup
    service.final_responses = [
        proto.SynthesisResponseStream(
            trailer={
                "timestamps": {
                    "status": {"code": 0},
                    "spans": [{"text": "Hello", "start": {}, "end": {"nanos": 100000000}}],
                }
            }
        )
    ]
    async with Rime(api_key="test-key", model="mistv3", endpoint=service.target) as client:
        stream = client.tts.stream("Hello.", complete_text=True, timestamps=True)
        with pytest.raises(RimeInputError, match="Consume all audio"):
            await stream.timestamps()
        assert b"".join([part async for part in stream]) == service.payload
        result = await stream.timestamps()
        assert result.spans[0].text == "Hello"
        assert service.complete_calls[0].timestamps.enable


async def test_full_text_partial_failure_is_not_completion_or_retried(setup):
    service, client = setup
    service.mode = "partial_error"
    async with client.tts.stream("Hello.", complete_text=True) as stream:
        assert await asyncio.wait_for(anext(stream), 1) == service.payload
        service.release.set()
        with pytest.raises(RimeUnavailableError, match="test disconnect") as caught:
            await collect(stream)
        assert caught.value.request_id == stream.request_id == "test-request"
    assert len(service.complete_calls) == 1
    service.mode = "normal"
    assert await collect(client.tts.stream("Again.", complete_text=True)) == service.payload


@pytest.mark.parametrize("after_audio", [False, True])
async def test_full_text_stall_without_overall_deadline_cancels_rpc(setup, after_audio):
    service, client = setup
    service.mode = "delayed_trailer" if after_audio else "silence"
    client.tts._policy = replace(
        client.tts._policy,
        first_audio_timeout=5 if after_audio else 0.1,
        progress_timeout=0.1 if after_audio else 5,
    )
    async with client.tts.stream("Hello.", complete_text=True, timeout=None) as stream:
        if after_audio:
            assert await asyncio.wait_for(anext(stream), 1) == service.payload
        with pytest.raises(RimeTimeoutError, match="stopped making progress") as caught:
            await asyncio.wait_for(collect(stream), 1)
        assert caught.value.request_id == "test-request"
    await asyncio.wait_for(service.cancelled.wait(), 1)
    assert not client.tts._streams


async def test_full_text_cancellation_keeps_an_active_sibling(setup):
    service, client = setup
    service.mode = "delayed_trailer"
    first = client.tts.stream("First.", complete_text=True)
    sibling = client.tts.stream("Sibling.", complete_text=True)
    assert await asyncio.wait_for(anext(first), 1) == service.payload
    assert await asyncio.wait_for(anext(sibling), 1) == service.payload
    pending = asyncio.create_task(anext(first))
    await asyncio.wait_for(first.cancel(), 1)
    with pytest.raises(RimeCancelledError):
        await pending
    await asyncio.wait_for(service.cancelled.wait(), 1)
    service.release.set()
    assert await asyncio.wait_for(collect(sibling), 1) == b""
    assert len(service.complete_calls) == 2
    assert not client.tts._streams


@pytest.mark.parametrize("after_audio", [False, True])
async def test_full_text_client_close_wakes_pending_reader(setup, after_audio):
    service, client = setup
    service.mode = "delayed_trailer" if after_audio else "silence"
    stream = client.tts.stream("Hello.", complete_text=True)
    if after_audio:
        assert await asyncio.wait_for(anext(stream), 1) == service.payload
    pending = asyncio.create_task(anext(stream))
    await asyncio.wait_for(service.headers_sent.wait(), 1)
    await asyncio.wait_for(client.close(), 1)
    with pytest.raises(RimeCancelledError):
        await pending
    await asyncio.wait_for(service.cancelled.wait(), 1)
    assert not client.tts._streams


@pytest.mark.parametrize("profile", list(AudioFormat))
async def test_full_text_slow_consumer_preserves_bounded_audio(setup, profile):
    service, client = setup
    client.tts._policy = replace(client.tts._policy, progress_timeout=0.05)
    # Keep the RPC open and fill the output queue in either audio profile.
    service.mode = "delayed_trailer"
    samples = 3 * (client.tts._policy.output_bytes + 1)
    service.payload = b"\0\0" * samples
    async with client.tts.stream(
        "Hello.", complete_text=True, audio_format=profile, timeout=None
    ) as stream:
        chunks = [await asyncio.wait_for(anext(stream), 1)]
        await asyncio.sleep(0.15)
        assert 0 < stream._queue.size <= client.tts._policy.output_bytes
        chunks.append(await asyncio.wait_for(anext(stream), 1))
        service.release.set()
        async with asyncio.timeout(2):
            chunks.extend([part async for part in stream])
    assert all(0 < len(part) <= client.tts._policy.output_chunk_bytes for part in chunks)
    expected = (
        service.payload if profile is AudioFormat.PCM_24000 else b"\xff" * ((samples + 2) // 3)
    )
    assert b"".join(chunks) == expected


async def test_full_text_overall_deadline_applies_while_consumer_is_paused(setup):
    service, client = setup
    service.mode = "delayed_trailer"
    service.payload *= 100
    async with client.tts.stream("Hello.", complete_text=True, timeout=0.15) as stream:
        assert await asyncio.wait_for(anext(stream), 1)
        await asyncio.sleep(0.25)
        with pytest.raises(RimeTimeoutError, match="Overall synthesis deadline"):
            await anext(stream)
    await asyncio.wait_for(service.cancelled.wait(), 1)
