import asyncio

import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import (
    AudioFormat,
    PronunciationEntry,
    Rime,
    RimeCancelledError,
    RimeInputError,
    RimeStreamError,
    RimeTimeoutError,
    RimeUnavailableError,
    TimestampResult,
    TimestampStatus,
    WordTimestamp,
)
from rimelabs_sdk.tts._audio import Converter


def trailer(code=0, spans=None):
    return proto.SynthesisResponseStream(
        trailer={
            "timestamps": {
                "status": {"code": code, "message": "" if code == 0 else "alignment unavailable"},
                "spans": spans if spans is not None else [],
            }
        }
    )


WORDS = [
    {"text": "twenty", "start": {"nanos": 90000000}, "end": {"nanos": 325000000}},
    {"text": "two", "start": {"seconds": 1, "nanos": 125000000}, "end": {"seconds": 2}},
]


async def collect(stream):
    return b"".join([chunk async for chunk in stream])


@pytest.fixture(params=["coda", "mistv3"])
async def tts(request, setup):
    service, _ = setup
    async with Rime(api_key="test-key", model=request.param, endpoint=service.target) as client:
        yield service, client


@pytest.mark.parametrize(
    "profile,mime",
    [
        (AudioFormat.PCM, "audio/pcm"),
        (AudioFormat.WAV, "audio/wav"),
        (AudioFormat.MP3, "audio/mpeg"),
        (AudioFormat.PCM_24000, "audio/pcm"),
        (AudioFormat.MULAW_8000, "audio/pcm"),
    ],
)
async def test_timestamps_preserve_audio_and_synthesis_offsets(tts, profile, mime):
    service, client = tts
    service.response_metadata = (
        ("x-rime-audio-content-type", mime),
        ("x-request-id", "test-request"),
    )
    if profile in (AudioFormat.WAV, AudioFormat.MP3):
        service.payload = b"odd-encoded"
    service.mode = "odd_chunks"
    service.final_responses = [trailer(spans=WORDS)]

    async def text():
        yield "First. "
        yield "Twenty two."

    stream = client.tts.stream(text(), timestamps=True, audio_format=profile)
    audio = await collect(stream)
    converter = Converter(profile)
    expected = converter.process(service.payload * (len(service.calls[0]) - 1), final=True)
    assert audio == expected
    assert service.calls[0][0].header.audio_parameters.audio_format == mime
    assert service.calls[0][0].header.timestamps.enable
    result = await stream.timestamps()
    assert result == TimestampResult(
        TimestampStatus(0, ""),
        (WordTimestamp("twenty", 0.09, 0.325), WordTimestamp("two", 1.125, 2)),
    )
    await stream.cancel()
    await client.close()
    assert await stream.timestamps() == result


@pytest.mark.parametrize("code", [0, 1, 3, 4, 8, 12, 13, 14])
async def test_alignment_status_keeps_successful_audio(tts, code):
    service, client = tts
    service.final_responses = [trailer(code)]
    stream = client.tts.synthesize("Hello.", timestamps=True)
    assert await collect(stream) == service.payload
    result = await stream.timestamps()
    assert result.status == TimestampStatus(code, "" if code == 0 else "alignment unavailable")
    assert result.spans == ()


@pytest.mark.parametrize(
    "responses",
    [
        [],
        [proto.SynthesisResponseStream(trailer={})],
        [proto.SynthesisResponseStream(trailer={"timestamps": {}})],
        [trailer(), trailer()],
        [trailer(), proto.SynthesisResponseStream(audio=b"\x00\x00")],
        [trailer(14, WORDS)],
        [trailer(spans=[{"text": "missing"}])],
        [trailer(spans=[{"text": "negative", "start": {"nanos": -1}, "end": {}}])],
        [trailer(spans=[{"text": "invalid", "start": {}, "end": {"nanos": 1000000000}}])],
        [trailer(spans=[{"text": "reversed", "start": {"seconds": 2}, "end": {"seconds": 1}}])],
    ],
)
async def test_bad_timestamp_result_does_not_fail_audio(tts, responses):
    service, client = tts
    service.final_responses = responses
    stream = client.tts.synthesize("Hello.", timestamps=True)
    assert (await collect(stream)).startswith(service.payload)
    with pytest.raises(RimeStreamError) as caught:
        await stream.timestamps()
    assert caught.value.request_id == "test-request"


async def test_opt_in_and_boolean_validation(tts):
    service, client = tts
    for invalid in [1, None, "true"]:
        with pytest.raises(RimeInputError, match="boolean"):
            client.tts.synthesize("Hello.", timestamps=invalid)
    assert service.calls == []
    for options in [{}, {"timestamps": False}]:
        stream = client.tts.synthesize("Hello.", **options)
        await collect(stream)
        assert not service.calls[-1][0].header.HasField("timestamps")
        with pytest.raises(RimeInputError, match="Enable"):
            await stream.timestamps()


async def test_audio_arrives_before_timestamps_and_early_access_never_waits(tts):
    service, client = tts
    service.mode = "delayed_trailer"
    service.final_responses = [trailer(spans=WORDS)]
    async with client.tts.synthesize("Hello.", timestamps=True) as stream:
        with pytest.raises(RimeInputError, match="Consume"):
            await stream.timestamps()
        assert await asyncio.wait_for(anext(stream), 1) == service.payload
        with pytest.raises(RimeInputError, match="Consume"):
            await stream.timestamps()
        service.release.set()
        await collect(stream)
        assert len((await stream.timestamps()).spans) == 2


@pytest.mark.parametrize("mode", ["stream", "client", "before_start"])
async def test_cancelled_timestamps_fail_promptly(tts, mode):
    service, client = tts
    service.mode = "delayed_trailer"
    stream = client.tts.synthesize("Hello.", timestamps=True)
    if mode != "before_start":
        await anext(stream)
    if mode == "client":
        await client.close()
    else:
        await stream.cancel()
    with pytest.raises(RimeCancelledError):
        await stream.timestamps()


async def test_deadline_fails_audio_and_timestamp_access(tts):
    service, client = tts
    service.mode = "delayed_trailer"
    stream = client.tts.synthesize("Hello.", timestamps=True, timeout=0.1)
    with pytest.raises(RimeTimeoutError):
        await collect(stream)
    with pytest.raises(RimeTimeoutError):
        await stream.timestamps()


async def test_final_rpc_failure_overrides_a_timestamp_trailer(tts):
    service, client = tts
    service.mode = "error_after_trailer"
    service.final_responses = [trailer(spans=WORDS)]
    stream = client.tts.synthesize("Hello.", timestamps=True)
    with pytest.raises(RimeUnavailableError):
        await collect(stream)
    with pytest.raises(RimeUnavailableError):
        await stream.timestamps()


@pytest.mark.parametrize("incremental", [False, True])
async def test_coda_preserves_request_words_and_delivered_audio_offsets(setup, incremental):
    service, client = setup
    service.final_responses = [
        trailer(
            spans=[
                {"text": "22", "start": {}, "end": {"seconds": 1}},
                {"text": "LiveKit", "start": {"seconds": 1}, "end": {"seconds": 2}},
                {"text": "to", "start": {"seconds": 2}, "end": {"seconds": 2}},
            ]
        )
    ]
    options = {
        "timestamps": True,
        "language": "de",
        "custom_lexicon": [PronunciationEntry("LiveKit", "l aI v k I t")],
        "audio_format": AudioFormat.MULAW_8000,
        "time_scale_factor": 2,
    }

    async def chunks():
        yield "22 Live"
        yield "Kit to missing."

    stream = (
        client.tts.stream(chunks(), **options)
        if incremental
        else client.tts.synthesize("22 LiveKit to missing.", **options)
    )
    assert await collect(stream)
    header = service.calls[0][0].header
    assert header.timestamps.enable
    assert header.language == "de"
    assert header.custom_lexicon[0].spelling == "LiveKit"
    assert header.audio_parameters.time_scale_factor == 2
    assert "".join(message.text_chunk for message in service.calls[0][1:]) == (
        "22 LiveKit to missing."
    )
    assert await stream.timestamps() == TimestampResult(
        TimestampStatus(0, ""),
        (WordTimestamp("22", 0, 1), WordTimestamp("LiveKit", 1, 2), WordTimestamp("to", 2, 2)),
    )
