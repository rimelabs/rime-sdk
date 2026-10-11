"""The engine owns sentence boundaries; the SDK preserves and transports text."""

import asyncio
import json
from pathlib import Path

import pytest
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import RimeInputError

CASES = json.loads((Path(__file__).parents[2] / "conformance/text-input.json").read_text())


async def collect(stream):
    return b"".join([chunk async for chunk in stream])


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
@pytest.mark.parametrize("size", [1, 7, 1024])
async def test_text_preservation_across_source_boundaries(setup, case, size):
    service, client = setup

    async def source():
        for offset in range(0, len(case["text"]), size):
            yield case["text"][offset : offset + size]
            yield ""

    await collect(client.tts.stream(source()))
    messages = service.calls[0]
    assert messages[0].header.split_strategy == proto.SPLIT_STRATEGY_ACCUMULATE_SENTENCE
    assert "".join(m.text_chunk for m in messages[1:]) == case["text"]
    assert all(0 < len(m.text_chunk.encode()) <= 4096 for m in messages[1:])


@pytest.mark.parametrize("text", ["x" * 70000, "🚀" * 20000, "word " + " " * 70000 + "end"])
async def test_large_text_is_bounded_without_a_sentence_limit(setup, text):
    service, client = setup
    await collect(client.tts.synthesize(text))
    messages = service.calls[0]
    assert messages[0].header.split_strategy == proto.SPLIT_STRATEGY_ACCUMULATE_SENTENCE
    assert "".join(m.text_chunk for m in messages[1:]) == text
    assert all(0 < len(m.text_chunk.encode()) <= 4096 for m in messages[1:])


async def test_fragment_sent_before_next_input_and_final_audio_drained(setup):
    service, client = setup
    service.mode = "empty_audio"
    service.final_responses = [proto.SynthesisResponseStream(audio=service.payload)]

    async def source():
        yield "The value is 1."
        # Receiving this fragment must not depend on a following sentence.
        await asyncio.wait_for(service.received.wait(), 2)
        # Engine buffering during a source pause is not an output stall.
        await asyncio.sleep(0.3)
        yield "7"
        yield " "
        yield "percent without final punctuation"

    async with asyncio.timeout(3):
        assert await collect(client.tts.stream(source())) == service.payload
    assert [m.text_chunk for m in service.calls[0][1:]] == [
        "The value is 1.",
        "7",
        " ",
        "percent without final punctuation",
    ]


@pytest.mark.parametrize("text", ["\ud800", "hello\udfff", "\ud800\udc00"])
async def test_malformed_unicode_is_rejected_and_client_recovers(setup, text):
    _, client = setup
    with pytest.raises(RimeInputError):
        await collect(client.tts.synthesize(text))
    assert await collect(client.tts.synthesize("valid"))
