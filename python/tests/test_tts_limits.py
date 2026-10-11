"""Request boundaries through local gRPC with the production channel limits."""

import asyncio

import pytest

from rimelabs_sdk import PronunciationEntry, RimeResourceLimitError
from rimelabs_sdk.tts._transport import header


async def test_serialized_request_limit_and_recovery(setup):
    service, client = setup
    text = "Hello."
    entries = [PronunciationEntry(f"word{i}", '" k { S') for i in range(500)]

    def request():
        return header("clementine", "en", custom_lexicon=entries)

    # Pad one spelling to exercise the byte limit independently of entry count.
    # The local service does not perform linguistic validation.
    while (difference := 131072 - request().ByteSize()) != 0:
        spelling = entries[-1].spelling
        spelling = spelling + "x" * difference if difference > 0 else spelling[:difference]
        entries[-1] = PronunciationEntry(spelling, entries[-1].pronunciation)
    async with asyncio.timeout(3):
        stream = client.tts.synthesize(text, custom_lexicon=entries)
        assert b"".join([part async for part in stream]) == service.payload
        entries[-1] = PronunciationEntry(entries[-1].spelling + "x", entries[-1].pronunciation)
        assert request().ByteSize() == 131073
        with pytest.raises(RimeResourceLimitError):
            async with client.tts.synthesize(text, custom_lexicon=entries) as rejected:
                async for _ in rejected:
                    pytest.fail("Oversized request produced audio")
        recovered = client.tts.synthesize("Again.")
        assert b"".join([part async for part in recovered]) == service.payload
    accepted = [messages for messages in service.calls if messages]
    assert len(accepted) == 2
    assert len(accepted[0][0].header.custom_lexicon) == 500
    assert not client.tts._streams
