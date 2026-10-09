"""Pronunciation options and service diagnostics through the public SDK."""

import asyncio

import grpc
import pytest

from rimelabs_sdk import AudioFormat, PronunciationEntry, RimeInputError
from rimelabs_sdk._grpc import rpc_error


@pytest.mark.parametrize("incremental", [False, True])
@pytest.mark.parametrize("profile", [AudioFormat.PCM_24000, AudioFormat.MULAW_8000])
async def test_lexicon_is_snapshotted_once_per_stream(setup, incremental, profile):
    service, client = setup
    entries = [
        PronunciationEntry("Hello", 'h @ . " l oU'),
        PronunciationEntry("cafe\u0301 au lait", '" k { S'),
        PronunciationEntry("Hello", '" k { S'),
    ]
    expected = [(entry.spelling, entry.pronunciation) for entry in entries]

    async def text():
        yield "Hello. "
        yield "Hello again."

    stream = client.tts.stream(
        text() if incremental else "Hello. Hello again.",
        custom_lexicon=entries,
        audio_format=profile,
    )
    entries[0] = PronunciationEntry("changed", "changed")
    entries.clear()
    async with stream:
        audio = b"".join([part async for part in stream])
    assert audio
    assert len(service.calls) == 1
    messages = service.calls[0]
    assert [message.WhichOneof("payload") for message in messages] == [
        "header",
        "text_chunk",
        "text_chunk",
    ]
    assert [
        (entry.spelling, entry.pronunciation) for entry in messages[0].header.custom_lexicon
    ] == expected
    # Options never become client state or leak into the next request.
    async with client.tts.stream("Hello.") as plain:
        assert b"".join([part async for part in plain]) == service.payload
    assert not service.calls[1][0].header.custom_lexicon


@pytest.mark.parametrize(
    "entries",
    [
        None,
        "hello",
        {},
        [{"spelling": "hello", "pronunciation": "h"}],
        [None],
        [PronunciationEntry(1, "h")],
        [PronunciationEntry("hello", None)],
    ],
)
async def test_bad_lexicon_shape_fails_before_network(setup, entries):
    service, client = setup
    with pytest.raises(RimeInputError):
        client.tts.stream("Hello.", custom_lexicon=entries)
    assert not service.calls


REJECTIONS = [
    'custom-lexicon entry "hello": "h @ . l oU" is not well-formed (no-primary-stress)',
    'custom-lexicon entry "hello": "q" is not well-formed (unknown-phone); custom-lexicon entry "": "h" is not well-formed (empty-spelling)',
    "custom lexicon is not supported by this model",
    'custom lexicon is not supported for language "ja"',
    "custom lexicon has 501 entries; the maximum is 500",
]


@pytest.mark.parametrize("message", REJECTIONS)
@pytest.mark.parametrize("mode", ["error_before_audio", "no_audio_error"])
async def test_pronunciation_errors_preserve_message_and_request_id(setup, message, mode):
    service, client = setup
    service.mode = mode
    service.rejection_status = grpc.StatusCode.INVALID_ARGUMENT
    service.rejection_message = message
    service.release.set()
    stopped = asyncio.Event()

    async def text():
        try:
            await asyncio.Event().wait()
            yield "Hello."
        finally:
            stopped.set()

    stream = client.tts.stream(text(), custom_lexicon=[PronunciationEntry("hello", "h @ . l oU")])
    async with asyncio.timeout(2):
        with pytest.raises(RimeInputError) as caught:
            async with stream:
                async for _ in stream:
                    pytest.fail("Rejected lexicon produced audio")
        if mode == "no_audio_error":
            await stopped.wait()
    assert str(caught.value) == message
    assert caught.value.request_id == (
        "rejected-request" if mode == "error_before_audio" else "test-request"
    )
    assert len(service.calls) == 1
    assert service.calls[0][0].header.custom_lexicon[0].pronunciation == "h @ . l oU"


@pytest.mark.parametrize("details", [None, "", "   "])
def test_missing_service_message_has_status_fallback(details):
    error = rpc_error(grpc.StatusCode.INVALID_ARGUMENT, "id", details)
    assert isinstance(error, RimeInputError)
    assert str(error) == "Rime operation failed: INVALID_ARGUMENT"
    assert error.request_id == "id"
