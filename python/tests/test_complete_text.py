"""Full-text synthesis must select the unary-input RPC, preserving SDK behavior."""

import asyncio

import grpc
import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import PronunciationEntry, Rime, RimeCancelledError, RimeInputError


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
