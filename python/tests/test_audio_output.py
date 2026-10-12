import asyncio
import io
import json
import wave
from pathlib import Path

import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import (
    AudioFormat,
    RimeAudioFormatError,
    RimeInputError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from rimelabs_sdk.tts._buffer import finalize_wav

CONTRACT = json.loads((Path(__file__).parents[2] / "conformance/contract.json").read_text())


@pytest.mark.parametrize("case", CONTRACT["output_cases"])
@pytest.mark.parametrize("incremental", [False, True])
async def test_output_headers_and_chunk_boundaries(setup, case, incremental):
    service, client = setup
    service.mode = "odd_chunks"
    service.response_metadata = (
        ("x-rime-audio-content-type", case["wire_format"]),
        ("x-request-id", "test-request"),
    )
    # Odd-length encoded bytes must not go through PCM alignment or conversion.
    service.payload = b"encoded" if case["format"] in ("WAV", "MP3") else b"\x01\x00" * 12
    options = {
        "audio_format": AudioFormat[case["format"]],
        "sample_rate": case["sample_rate"],
        "time_scale_factor": 0.8,
    }

    async def text():
        yield "Hello."

    audio = (
        client.tts.stream(text(), **options)
        if incremental
        else client.tts.synthesize("Hello.", **options)
    )
    assert audio.sample_rate == case["sample_rate"]
    chunks = [chunk async for chunk in audio]
    if case["format"] != "MULAW_8000":
        assert b"".join(chunks) == service.payload
    else:
        assert len(b"".join(chunks)) == 4
    params = service.calls[0][0].header.audio_parameters
    assert params.audio_format == case["wire_format"]
    assert params.sampling_rate == case["wire_rate"]
    assert params.time_scale_factor == pytest.approx(0.8)


@pytest.mark.parametrize("case", CONTRACT["wav_cases"], ids=lambda case: case["name"])
def test_wav_files(case):
    data = bytearray.fromhex(case["input_hex"])
    if case.get("error"):
        with pytest.raises(RimeAudioFormatError):
            finalize_wav(data, 16000)
    else:
        finalize_wav(data, 16000)
        assert data.hex() == case["output_hex"]
        with wave.open(io.BytesIO(data), "rb") as audio:
            assert audio.getframerate() == 16000
            assert audio.getsampwidth() == 2
            assert audio.getnchannels() == 1
            assert len(audio.readframes(audio.getnframes())) == audio.getnframes() * 2


async def test_buffered_formats_and_default_header(setup):
    service, client = setup
    for profile, mime, payload, expected in [
        (AudioFormat.PCM, "audio/pcm", b"\x01\x00", b"\x01\x00"),
        (AudioFormat.MP3, "audio/mpeg", b"odd-mp3", b"odd-mp3"),
        (
            AudioFormat.WAV,
            "audio/wav",
            bytes.fromhex(CONTRACT["wav_cases"][0]["input_hex"]),
            bytes.fromhex(CONTRACT["wav_cases"][0]["output_hex"]),
        ),
    ]:
        service.mode = "odd_chunks"
        service.payload = payload
        service.response_metadata = (
            ("x-rime-audio-content-type", mime),
            ("x-request-id", "test-request"),
        )
        assert (
            await client.tts.synthesize_to_buffer("Hello.", audio_format=profile, sample_rate=16000)
            == expected
        )
        assert not service.calls[-1][0].header.audio_parameters.HasField("time_scale_factor")
        assert not client.tts._streams


async def test_output_validation_is_local(setup):
    service, client = setup
    for rate in CONTRACT["invalid_sample_rates"] + [True, "8000", 8000.5]:
        with pytest.raises(RimeAudioFormatError):
            client.tts.synthesize("Hello.", sample_rate=rate)
    for factor in CONTRACT["invalid_time_scale_factors"] + [
        float("nan"),
        float("inf"),
        True,
        "1",
        10**1000,
    ]:
        with pytest.raises(RimeInputError):
            client.tts.synthesize("Hello.", time_scale_factor=factor)
    for profile, rate in [(AudioFormat.PCM_24000, 8000), (AudioFormat.MULAW_8000, 16000)]:
        with pytest.raises(RimeAudioFormatError):
            client.tts.synthesize("Hello.", audio_format=profile, sample_rate=rate)
    assert not service.calls


@pytest.mark.parametrize(
    "mode,error,options",
    [
        ("error_after_trailer", RimeUnavailableError, {}),
        ("hang_after_input", RimeTimeoutError, {"timeout": 0.1}),
        ("wrong_format", RimeAudioFormatError, {}),
    ],
)
async def test_buffer_failure_cleans_up_and_preserves_request_id(setup, mode, error, options):
    service, client = setup
    service.mode = mode
    with pytest.raises(error) as caught:
        await client.tts.synthesize_to_buffer("Hello.", **options)
    assert caught.value.request_id == "test-request"
    assert not client.tts._streams
    service.mode = "normal"
    assert await client.tts.synthesize_to_buffer("Still works.") == service.payload


async def test_buffer_task_cancellation(setup):
    service, client = setup
    service.mode = "silence"
    task = asyncio.create_task(client.tts.synthesize_to_buffer("Hello."))
    await asyncio.wait_for(service.received.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(service.cancelled.wait(), 2)
    assert not client.tts._streams


@pytest.mark.parametrize("rate,factor", [(8000, 0.4), (16000, 1.0), (24000, 2.5)])
async def test_sample_rate_without_format_and_factor_boundaries(setup, rate, factor):
    service, client = setup
    audio = client.tts.synthesize("Hello.", sample_rate=rate, time_scale_factor=factor)
    with pytest.raises(AttributeError):
        audio.sample_rate = 48000
    assert b"".join([chunk async for chunk in audio]) == service.payload
    params = service.calls[0][0].header.audio_parameters
    assert params.sampling_rate == rate
    assert params.time_scale_factor == pytest.approx(factor)


@pytest.mark.parametrize("extra_bytes", [0, 2])
async def test_fixed_buffer_limit_and_cleanup(setup, extra_bytes):
    service, client = setup
    service.payload = b"\x01\x00" * (512 * 1024)
    service.final_responses = [proto.SynthesisResponseStream(audio=service.payload)] * 63
    if extra_bytes:
        service.final_responses.append(proto.SynthesisResponseStream(audio=b"\0" * extra_bytes))
        service.mode = "hang_after_input"
    async with asyncio.timeout(10):
        if extra_bytes:
            with pytest.raises(RimeResourceLimitError, match="64 MiB") as caught:
                await client.tts.synthesize_to_buffer("Hello.")
            assert caught.value.request_id == "test-request"
            await asyncio.wait_for(service.cancelled.wait(), 2)
        else:
            data = await client.tts.synthesize_to_buffer("Hello.")
            assert len(data) == 64 * 1024 * 1024
    assert not client.tts._streams
    service.mode = "normal"
    service.final_responses = []
    service.payload = b"\x01\x00"
    assert await client.tts.synthesize_to_buffer("Still works.") == service.payload
