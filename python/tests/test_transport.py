import pytest


@pytest.mark.parametrize("mode", ["odd_chunks", "headers_after_text"])
async def test_audio_and_wire_order(setup, mode):
    server, client = setup
    server.mode = mode
    stream = client.tts.stream("Hello.", voice="voice", language="de")
    assert b"".join([part async for part in stream]) == server.payload
    assert stream.request_id == "test-request"
    assert len(server.calls) == 1
    assert [m.WhichOneof("payload") for m in server.calls[0]] == ["header", "text_chunk"]
    header = server.calls[0][0].header
    assert (header.speaker, header.language, header.audio_parameters.audio_format) == (
        "voice",
        "de",
        "audio/pcm",
    )
