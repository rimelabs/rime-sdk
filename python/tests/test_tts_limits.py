"""Request boundaries through local gRPC with the production channel limits."""

import asyncio

import pytest

from rimelabs_sdk import PronunciationEntry, RimeInputError, RimeResourceLimitError
from rimelabs_sdk.tts._transport import header


@pytest.mark.parametrize("character", ["a", "é", "🙂"])
async def test_complete_text_utf8_boundary_and_recovery(setup, character):
    service, client = setup
    text = character * (65536 // len(character.encode("utf-8")))
    stream = client.tts.stream(text, complete_text=True)
    assert b"".join([part async for part in stream]) == service.payload
    assert service.complete_calls[0].text == text
    with pytest.raises(RimeInputError, match="65536 UTF-8 bytes"):
        client.tts.stream(text + "a", complete_text=True)
    assert len(service.complete_calls) == 1
    assert b"".join([part async for part in client.tts.stream("Again.", complete_text=True)])


@pytest.mark.parametrize("complete_text", [False, True])
async def test_serialized_request_limit_and_recovery(setup, complete_text):
    service, client = setup
    text = "é" * 32768 if complete_text else "Hello."
    entries = [PronunciationEntry(f"word{i}", '" k { S') for i in range(500)]

    def request():
        message = header("clementine", "en", custom_lexicon=entries)
        if complete_text:
            message.header.text = text
            return message.header
        return message

    # Pad one spelling to exercise the byte limit independently of entry count.
    # The local service does not perform linguistic validation.
    while (difference := 131072 - request().ByteSize()) != 0:
        spelling = entries[-1].spelling
        spelling = spelling + "x" * difference if difference > 0 else spelling[:difference]
        entries[-1] = PronunciationEntry(spelling, entries[-1].pronunciation)
    async with asyncio.timeout(3):
        stream = client.tts.stream(text, complete_text=complete_text, custom_lexicon=entries)
        assert b"".join([part async for part in stream]) == service.payload
        entries[-1] = PronunciationEntry(entries[-1].spelling + "x", entries[-1].pronunciation)
        assert request().ByteSize() == 131073
        with pytest.raises(RimeResourceLimitError):
            async with client.tts.stream(
                text, complete_text=complete_text, custom_lexicon=entries
            ) as rejected:
                async for _ in rejected:
                    pytest.fail("Oversized request produced audio")
        recovered = client.tts.stream("Again.", complete_text=complete_text)
        assert b"".join([part async for part in recovered]) == service.payload
    accepted = [messages for messages in service.calls if messages]
    assert len(accepted) == 2
    assert len(accepted[0][0].header.custom_lexicon) == 500
    assert not client.tts._streams
