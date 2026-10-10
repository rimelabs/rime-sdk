"""STT framing against shared PCM samples, including refused inputs."""

import json
import struct
from pathlib import Path

import pytest

from rimelabs_sdk import PCMFormat, RimeAudioFormatError, RimeInputError, realtime
from rimelabs_sdk._pcm import InputConverter
from rimelabs_sdk.stt._audio import InputAudio

VECTORS = json.loads((Path(__file__).parents[2] / "conformance/pcm-input.json").read_text())[
    "vectors"
]


def pcm(samples):
    return struct.pack(f"<{len(samples)}h", *samples)


@pytest.mark.parametrize("vector", VECTORS)
@pytest.mark.parametrize("split", [None, 1, 3, 7])
def test_stt_conversion_matches_reference_across_arbitrary_byte_splits(vector, split):
    audio = InputAudio(PCMFormat(sample_rate=vector["sampleRate"], channels=vector["channels"]))
    source = pcm(vector["input"])
    chunks = []
    for offset in range(0, len(source), split or len(source)):
        chunks.extend(audio.feed(source[offset : offset + (split or len(source))]))
    audio.finish()
    assert b"".join(chunks) == pcm(vector["output"])
    assert all(0 < len(chunk) <= 65536 and len(chunk) % 2 == 0 for chunk in chunks)


@pytest.mark.parametrize("channels", [1, 2])
@pytest.mark.parametrize("sample_rate", [8000, 16000, 24000, 48000])
def test_large_source_has_bounded_complete_messages(sample_rate, channels):
    audio = InputAudio(PCMFormat(sample_rate=sample_rate, channels=channels))
    frames = 480000
    chunks = list(audio.feed(pcm([1234] * frames * channels)))
    audio.finish()
    # Linear interpolation of a constant has a known sample count and value.
    output_frames = ((frames - 1) * 16000) // sample_rate + 1
    assert b"".join(chunks) == pcm([1234] * output_frames)
    assert all(0 < len(chunk) <= 65536 and len(chunk) % 2 == 0 for chunk in chunks)


@pytest.mark.parametrize("channels", [1, 2])
def test_incomplete_frame_is_carried_but_refused_at_eof(channels):
    for length in range(1, 2 * channels):
        audio = InputAudio(PCMFormat(channels=channels))
        assert list(audio.feed(b"\0" * length)) == []
        with pytest.raises(RimeAudioFormatError, match="incomplete PCM16 frame"):
            audio.finish()
        with pytest.raises(RimeInputError, match="finished"):
            list(audio.feed(b"\0"))
    audio = InputAudio(PCMFormat(channels=channels))
    assert list(audio.feed(b"\0")) == []
    assert list(audio.feed(b"\0" * (2 * channels - 1))) == [b"\0\0"]
    audio.finish()


@pytest.mark.parametrize("value", [None, "audio", bytearray(b"\0\0"), [0, 0]])
def test_non_bytes_input_is_refused(value):
    with pytest.raises(RimeAudioFormatError, match="must yield"):
        list(InputAudio().feed(value))


@pytest.mark.parametrize(
    "format",
    [
        {"sample_rate": 44100},
        {"sample_rate": 16000.0},
        {"channels": 0},
        {"channels": True},
        {"channels": 1.0},
        {"encoding": "mulaw"},
    ],
)
def test_unsupported_format_is_refused(format):
    with pytest.raises(RimeAudioFormatError):
        PCMFormat(**format)


def test_empty_chunks_do_not_flush_pending_audio_or_add_samples():
    audio = InputAudio()
    assert list(audio.feed(b"")) == []
    assert list(audio.feed(b"\x01")) == []
    assert list(audio.feed(b"")) == []
    assert list(audio.feed(b"\0")) == [b"\x01\0"]
    audio.finish()
    audio.finish()
    with pytest.raises(RimeInputError, match="finished"):
        list(audio.feed(b""))
    InputAudio().finish()


@pytest.mark.parametrize("vector", VECTORS)
def test_interleaved_utterances_have_independent_conversion_and_pending_frames(
    vector,
):
    format = PCMFormat(sample_rate=vector["sampleRate"], channels=vector["channels"])
    first, second = InputAudio(format), InputAudio(format)
    source = pcm(vector["input"])
    first_output, second_output = [], []
    for offset in range(0, len(source), 3):
        first_output.extend(first.feed(source[offset : offset + 3]))
        second_output.extend(second.feed(b"\0" * len(source[offset : offset + 3])))
    first.finish()
    second.finish()
    assert b"".join(first_output) == pcm(vector["output"])
    assert b"".join(second_output) == b"\0" * (len(vector["output"]) * 2)


@pytest.mark.parametrize("vector", VECTORS)
def test_converter_snapshot_does_not_mutate_previous_state(vector):
    format = PCMFormat(sample_rate=vector["sampleRate"], channels=vector["channels"])
    source = pcm(vector["input"])
    boundary = 7 * 2 * format.channels
    previous = InputConverter(format)
    prefix = previous.process(source[:boundary])
    attempted = previous.clone()
    expected_tail = attempted.process(source[boundary:])
    assert previous.process(source[boundary:]) == expected_tail
    assert prefix + expected_tail == pcm(vector["output"])


def test_preparation_is_lazy_and_bounds_each_conversion(monkeypatch):
    original = InputConverter.process
    converted_bytes = []

    def record(self, data):
        converted_bytes.append(len(data))
        return original(self, data)

    monkeypatch.setattr(InputConverter, "process", record)
    audio = InputAudio()
    source = b"\0\0" * 500000
    output = audio.feed(source)
    assert converted_bytes == []
    assert next(output)
    assert 0 < sum(converted_bytes) < len(source)
    list(output)
    audio.finish()
    assert sum(converted_bytes) == len(source)
    assert max(converted_bytes) <= 16384


def test_pcm_format_public_import_is_the_existing_realtime_value():
    assert PCMFormat is realtime.PCMFormat
    assert realtime.AudioChunk(data=b"\0\0", format=PCMFormat()).format == realtime.PCMFormat()
