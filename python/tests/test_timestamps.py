import asyncio

import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import (
    AudioFormat,
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


@pytest.fixture
async def mist(setup):
    service, _ = setup
    async with Rime(api_key="test-key", model="mistv3", endpoint=service.target) as client:
        yield service, client


@pytest.mark.parametrize("profile", list(AudioFormat))
async def test_timestamps_preserve_audio_and_synthesis_offsets(mist, profile):
    service, client = mist
    service.final_responses = [trailer(spans=WORDS)]

    async def text():
        yield "First. "
        yield "Twenty two."

    stream = client.tts.stream(text(), timestamps=True, audio_format=profile)
    audio = await collect(stream)
    converter = Converter(profile)
    expected = converter.process(service.payload * (len(service.calls[0]) - 1), final=True)
    assert audio == expected
    assert service.calls[0][0].header.timestamps.enable
    result = await stream.timestamps()
    assert result == TimestampResult(
        TimestampStatus(0, ""),
        (WordTimestamp("twenty", 0.09, 0.325), WordTimestamp("two", 1.125, 2)),
    )
    await stream.cancel()
    await client.close()
    assert await stream.timestamps() == result


@pytest.mark.parametrize("code", [1, 3, 4, 8, 12, 14])
async def test_alignment_failure_keeps_successful_audio(mist, code):
    service, client = mist
    service.final_responses = [trailer(code)]
    stream = client.tts.stream("Hello.", timestamps=True)
    assert await collect(stream) == service.payload
    result = await stream.timestamps()
    assert result.status == TimestampStatus(code, "alignment unavailable")
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
async def test_bad_timestamp_result_does_not_fail_audio(mist, responses):
    service, client = mist
    service.final_responses = responses
    stream = client.tts.stream("Hello.", timestamps=True)
    assert (await collect(stream)).startswith(service.payload)
    with pytest.raises(RimeStreamError) as caught:
        await stream.timestamps()
    assert caught.value.request_id == "test-request"


async def test_opt_in_and_model_validation(setup):
    service, coda = setup
    with pytest.raises(RimeInputError, match="mistv3"):
        coda.tts.stream("Hello.", timestamps=True)
    assert service.calls == []
    async with Rime(api_key="test-key", model="mistv3", endpoint=service.target) as client:
        for invalid in [1, None, "true"]:
            with pytest.raises(RimeInputError, match="boolean"):
                client.tts.stream("Hello.", timestamps=invalid)
        for options in [{}, {"timestamps": False}]:
            stream = client.tts.stream("Hello.", **options)
            await collect(stream)
            assert not service.calls[-1][0].header.HasField("timestamps")
            with pytest.raises(RimeInputError, match="Enable"):
                await stream.timestamps()


async def test_audio_arrives_before_timestamps_and_early_access_never_waits(mist):
    service, client = mist
    service.mode = "delayed_trailer"
    service.final_responses = [trailer(spans=WORDS)]
    async with client.tts.stream("Hello.", timestamps=True) as stream:
        with pytest.raises(RimeInputError, match="Consume"):
            await stream.timestamps()
        assert await asyncio.wait_for(anext(stream), 1) == service.payload
        with pytest.raises(RimeInputError, match="Consume"):
            await stream.timestamps()
        service.release.set()
        await collect(stream)
        assert len((await stream.timestamps()).spans) == 2


@pytest.mark.parametrize("mode", ["stream", "client", "before_start"])
async def test_cancelled_timestamps_fail_promptly(mist, mode):
    service, client = mist
    service.mode = "delayed_trailer"
    stream = client.tts.stream("Hello.", timestamps=True)
    if mode != "before_start":
        await anext(stream)
    if mode == "client":
        await client.close()
    else:
        await stream.cancel()
    with pytest.raises(RimeCancelledError):
        await stream.timestamps()


async def test_deadline_fails_audio_and_timestamp_access(mist):
    service, client = mist
    service.mode = "delayed_trailer"
    stream = client.tts.stream("Hello.", timestamps=True, timeout=0.1)
    with pytest.raises(RimeTimeoutError):
        await collect(stream)
    with pytest.raises(RimeTimeoutError):
        await stream.timestamps()


@pytest.mark.parametrize("complete_text", [False, True])
async def test_final_rpc_failure_overrides_a_timestamp_trailer(mist, complete_text):
    service, client = mist
    service.mode = "error_after_trailer"
    service.final_responses = [trailer(spans=WORDS)]
    stream = client.tts.stream("Hello.", timestamps=True, complete_text=complete_text)
    with pytest.raises(RimeUnavailableError):
        await collect(stream)
    with pytest.raises(RimeUnavailableError):
        await stream.timestamps()
