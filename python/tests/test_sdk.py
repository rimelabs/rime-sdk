import asyncio
import gc
import json
import math
import struct
import time
import weakref
from dataclasses import replace
from pathlib import Path

import grpc
import pytest

from rimelabs_sdk import (
    AudioFormat,
    Rime,
    RimeAudioFormatError,
    RimeAuthenticationError,
    RimeCancelledError,
    RimeInputError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
    _auth,
)
from rimelabs_sdk.tts._audio import Converter
from rimelabs_sdk.tts._sentences import SentenceBuffer
from rimelabs_sdk.tts._transport import rpc_error

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = json.loads((ROOT / "conformance/contract.json").read_text())


async def collect(stream):
    return b"".join([chunk async for chunk in stream])


@pytest.mark.parametrize(
    "case", json.loads((ROOT / "conformance/sentences.json").read_text()), ids=lambda c: c["id"]
)
@pytest.mark.parametrize("size", CONTRACT["chunk_sizes"])
def test_sentence_conformance(case, size):
    buffer = SentenceBuffer(65536)
    actual = []
    for offset in range(0, len(case["text"]), size):
        actual.extend(buffer.feed(case["text"][offset : offset + size]))
    actual.extend(buffer.feed("", final=True))
    assert actual == case["sentences"]
    assert "".join(actual) == case["text"]


def test_sentences_at_every_split_and_end_of_input():
    cases = json.loads((ROOT / "conformance/sentences.json").read_text())
    for case in cases:
        text = case["text"]
        for split in range(len(text) + 1):
            buffer = SentenceBuffer(65536)
            actual = list(buffer.feed(text[:split]))
            actual.extend(buffer.feed(""))
            # final=True must use the same scans, even with a nonempty fragment.
            actual.extend(buffer.feed(text[split:], final=True))
            assert actual == case["sentences"], (case["id"], split)
            assert list(buffer.feed("", final=True)) == []


def test_sentences_after_context_rotation():
    text = ("Hello 🚀 world. Dr. Smith paid 3.14 dollars. \u200fمرحبا بالعالم. " * 30) + "Done."
    whole = SentenceBuffer(65536)
    expected = list(whole.feed(text, final=True))
    for sizes in [[1], [2, 7, 31, 128, 3], [1024]]:
        buffer = SentenceBuffer(65536)
        actual = []
        offset = step = 0
        while offset < len(text):
            size = sizes[step % len(sizes)]
            actual.extend(buffer.feed(text[offset : offset + size]))
            offset += size
            step += 1
        actual.extend(buffer.feed("", final=True))
        assert actual == expected
    assert "".join(expected) == text


@pytest.mark.parametrize("spacing", [1, 2, 15, 16, 17, 31, 32, 100])
def test_sentence_delivery_at_lookahead_threshold(spacing):
    buffer = SentenceBuffer(65536)
    output = []
    for char in "First." + " " * spacing + "The next sentenc":
        output.extend(buffer.feed(char))
    assert output == ["First."]


def test_sentence_storage_does_not_grow_with_completed_text():
    buffer = SentenceBuffer(256)
    text = "Hello 🚀 world. This is a short sentence. " * 1000
    output = []
    for sentence in buffer.feed(text):
        output.append(sentence)
        assert len(buffer.pending.encode()) < 2048
    output.extend(buffer.feed("", final=True))
    assert "".join(output) == text
    assert buffer.pending == ""


def test_long_sentence_does_not_scan_once_per_source_chunk(monkeypatch):
    from rimelabs_sdk.tts import _sentences

    detector = _sentences.sentence_ends
    calls = 0

    def count_calls(text):
        nonlocal calls
        calls += 1
        return detector(text)

    monkeypatch.setattr(_sentences, "sentence_ends", count_calls)
    buffer = SentenceBuffer(65536)
    text = "a" * 65535 + "."
    output = []
    for char in text:
        output.extend(buffer.feed(char))
    output.extend(buffer.feed("", final=True))
    assert output == [text]
    assert calls < 128


async def test_sentence_messages_ignore_source_chunk_size(setup):
    service, client = setup
    text = "Hi! Dr. Smith agrees. Hello."
    for size in [None, 1, 7]:

        async def source(size=size):
            for offset in range(0, len(text), size):
                yield text[offset : offset + size]

        await collect(client.tts.stream(text if size is None else source()))
    messages = [[m.text_chunk for m in call[1:]] for call in service.calls]
    assert messages == [["Hi!", " Dr.", " Smith agrees.", " Hello."]] * 3


@pytest.mark.parametrize(
    "value",
    CONTRACT["invalid_timeouts"]
    + [
        float("inf"),
        float("nan"),
        pytest.param(10**1000, id="oversized-positive-integer"),
        pytest.param(-(10**1000), id="oversized-negative-integer"),
    ],
)
def test_invalid_timeout(value):
    with pytest.raises(RimeInputError):
        Rime(api_key="test", timeout=value)


def test_public_boundary_and_credentials(monkeypatch):
    monkeypatch.setenv("RIME_API_KEY", "environment-key")
    assert Rime()._credentials._key == "environment-key"
    assert Rime(api_key="explicit")._credentials._key == "explicit"
    with pytest.raises(RimeAuthenticationError):
        Rime(api_key="")
    client = Rime()
    assert not hasattr(client.tts, "session")
    assert not hasattr(client.tts, "stream_sentences")
    stream = client.tts.stream("Hello.")
    assert not hasattr(stream, "result")
    assert not hasattr(stream, "metadata")
    with pytest.raises(AttributeError):
        stream.format = AudioFormat.MULAW_8000
    with pytest.raises(RimeInputError):
        Rime(api_key="test", model="mist")
    with pytest.raises(RimeInputError):
        client.tts.stream("  ")
    with pytest.raises(RimeAudioFormatError):
        client.tts.stream("Hi", audio_format="wav")


def test_error_conformance():
    for status, expected in CONTRACT["grpc_errors"].items():
        error = rpc_error(getattr(grpc.StatusCode, status), "id")
        assert type(error).__name__ == expected
        assert error.request_id == "id"
        assert not hasattr(error, "grpc_status")


@pytest.mark.parametrize("profile", list(AudioFormat))
def test_audio_profiles_and_chunk_invariance(profile):
    assert {
        key: getattr(profile, key) for key in ("encoding", "sample_rate", "channels")
    } == CONTRACT["profiles"][profile.name]
    pcm = b"".join(
        struct.pack("<h", round(12000 * math.sin(2 * math.pi * 1000 * i / 24000)))
        for i in range(24000)
    )
    whole = Converter(profile)
    expected = whole.process(pcm) + whole.process(b"", final=True)
    split = Converter(profile)
    actual = b"".join(
        split.process(pcm[i : i + 137]) for i in range(0, len(pcm), 137)
    ) + split.process(b"", final=True)
    assert actual == expected
    assert len(actual) == (48000 if profile is AudioFormat.PCM_24000 else 8000)
    if profile is AudioFormat.MULAW_8000:
        import audioop

        decoded = audioop.ulaw2lin(actual, 2)
        assert audioop.rms(decoded, 2) > 7000
        high = b"".join(
            struct.pack("<h", round(12000 * math.sin(2 * math.pi * 7000 * i / 24000)))
            for i in range(24000)
        )
        converter = Converter(profile)
        filtered = converter.process(high) + converter.process(b"", final=True)
        assert audioop.rms(audioop.ulaw2lin(filtered, 2)[200:-200], 2) < 100


async def test_complete_and_incremental_shared_rpc(setup):
    service, client = setup
    async with client.tts.stream("Hello. Final phrase") as audio:
        assert audio.format is AudioFormat.PCM_24000
        assert await collect(audio) == service.payload * 2
        assert audio.request_id == "test-request"
    assert len(service.calls) == 1
    assert service.calls[0][0].header.speaker == "clementine"
    assert service.calls[0][0].header.language == "en"
    assert service.metadata[0]["authorization"] == "Bearer test-token"
    assert not client.tts._streams


async def test_incremental_audio_before_input_end(setup):
    service, client = setup
    release = asyncio.Event()

    async def source():
        yield "First sentence. The next sentence "
        await release.wait()
        yield "is here."

    async with client.tts.stream(source()) as audio:
        assert await asyncio.wait_for(anext(audio), 1)
        assert not audio._input_done
        release.set()
        await collect(audio)
    assert len(service.calls) == 1


async def test_partial_audio_then_error(setup):
    service, client = setup
    service.mode = "partial_error"
    async with client.tts.stream("Hello. The next sentence is waiting.") as audio:
        assert await anext(audio)
        service.release.set()
        with pytest.raises(RimeUnavailableError):
            await collect(audio)
    assert len(service.calls) == 1


async def test_cancel_keeps_sibling(setup):
    service, client = setup
    service.mode = "burst"
    a = client.tts.stream("Hello.")
    b = client.tts.stream("Sibling.")
    assert await anext(a)
    await asyncio.wait_for(a.cancel(), 1)
    with pytest.raises(RimeCancelledError):
        await anext(a)
    service.mode = "normal"
    assert await collect(b)
    assert not client.tts._streams


async def test_overall_timeout_while_paused(setup):
    service, client = setup
    service.mode = "burst"
    async with client.tts.stream("Hello.", timeout=0.08) as audio:
        assert await anext(audio)
        await asyncio.sleep(0.12)
        with pytest.raises(RimeTimeoutError):
            await anext(audio)
        with pytest.raises(RimeTimeoutError):
            await anext(audio)


async def test_lazy_cancel_and_close(setup):
    service, client = setup
    audio = client.tts.stream("Hello.", timeout=0.01)
    await asyncio.sleep(0.03)
    assert not service.calls
    await audio.cancel()
    await audio.cancel()
    with pytest.raises(RimeCancelledError):
        await anext(audio)
    pending = client.tts.stream("Later.")
    await client.close()
    await client.close()
    with pytest.raises(RimeCancelledError):
        await anext(pending)
    with pytest.raises(RimeInputError):
        client.tts.stream("No.")


async def test_no_synthesis_replay(setup):
    service, client = setup
    service.mode = "error_before_audio"
    async with client.tts.stream("Hello.") as audio:
        with pytest.raises(RimeUnavailableError):
            await collect(audio)
    assert len(service.calls) == 1


async def test_invalid_state_from_source_is_an_input_error(setup):
    _, client = setup
    cause = asyncio.InvalidStateError("application source failed")

    async def source():
        raise cause
        yield "Never"

    async with client.tts.stream(source()) as audio:
        with pytest.raises(RimeInputError) as caught:
            await collect(audio)
        assert caught.value.__cause__ is cause


async def test_format_and_sample_alignment(setup):
    service, client = setup
    service.mode = "odd_chunks"
    assert await collect(client.tts.stream("Hello.")) == service.payload
    service.mode = "wrong_format"
    async with client.tts.stream("Hi.") as stream:
        with pytest.raises(RimeAudioFormatError):
            await collect(stream)


async def test_source_failure_cleanup(setup):
    _, client = setup
    closed = asyncio.Event()

    async def source():
        try:
            yield "First sentence. The next sentence "
            raise ValueError("source failure")
        finally:
            closed.set()

    async with client.tts.stream(source()) as audio:
        with pytest.raises(RimeInputError) as caught:
            await collect(audio)
        assert isinstance(caught.value.__cause__, ValueError)
    assert closed.is_set()


@pytest.mark.parametrize("submit_sentence", [False, True])
async def test_source_cancellation_stops_stream(setup, submit_sentence):
    service, client = setup
    upstream = asyncio.get_running_loop().create_future()
    upstream.cancel()
    closed = asyncio.Event()

    async def source():
        try:
            await service.headers_sent.wait()
            if submit_sentence:
                yield "First sentence. The next sentence "
                await service.received.wait()
            await upstream
        finally:
            closed.set()

    async with client.tts.stream(source()) as audio:
        with pytest.raises(RimeCancelledError):
            await asyncio.wait_for(collect(audio), 1)
        await asyncio.wait_for(asyncio.shield(audio._worker), 1)
        assert closed.is_set()
        assert not client.tts._streams
        assert not any(
            task.get_name() in {"rime:input", "rime:audio"} for task in asyncio.all_tasks()
        )
        async with asyncio.timeout(1):
            while service.active:
                await asyncio.sleep(0.01)


@pytest.mark.parametrize("cleanup_stalls", [False, True])
async def test_cancel_allows_source_cleanup_within_budget(setup, cleanup_stalls):
    _, client = setup
    client.tts._policy = replace(client.tts._policy, cleanup_timeout=0.1)
    waiting = asyncio.Event()
    cleanup_started = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            yield "First sentence. The next sentence "
            waiting.set()
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            if cleanup_stalls:
                await asyncio.Event().wait()
            else:
                await asyncio.sleep(0.01)
            closed.set()

    audio = client.tts.stream(source())
    assert await anext(audio)
    await asyncio.wait_for(waiting.wait(), 1)
    await asyncio.wait_for(audio.cancel(), 1)
    assert cleanup_started.is_set()
    assert closed.is_set() is not cleanup_stalls
    assert audio._worker.done()
    assert not client.tts._streams


@pytest.mark.parametrize("operation", ["voices", "languages"])
@pytest.mark.parametrize("request_id", ["discovery-id", None])
@pytest.mark.parametrize("mode", ["discovery_timeout", "discovery_error"])
async def test_discovery_deadline_preserves_headers(setup, operation, request_id, mode):
    service, client = setup
    service.mode = mode
    service.response_metadata = (("x-request-id", request_id),) if request_id else ()
    # A retryable rejection also exercises expiry during the retry delay.
    with pytest.raises(RimeTimeoutError) as caught:
        await getattr(client, operation).list(timeout=0.04)
    assert caught.value.request_id == request_id
    assert not client.tts._discovery_tasks


async def test_discovery_retry_and_auth_singleflight(setup, monkeypatch):
    service, client = setup
    calls = 0

    async def exchange(key, policy):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return _auth.Token("new-token", time.time() + 3600, policy.audience)

    monkeypatch.setattr(_auth, "exchange_key", exchange)
    service.discovery_failures = 1
    voices, languages = await asyncio.gather(client.voices.list("en"), client.languages.list())
    assert voices == ["test-speaker"] and languages == ["en", "de"]
    assert calls == 1


async def test_cancel_during_shared_refresh(setup, monkeypatch):
    _, client = setup
    release = asyncio.Event()
    started = asyncio.Event()

    async def exchange(key, policy):
        started.set()
        await release.wait()
        return _auth.Token("token", time.time() + 3600, policy.audience)

    monkeypatch.setattr(_auth, "exchange_key", exchange)
    a = client.tts.stream("A.")
    b = client.tts.stream("B.")
    async with a, b:
        await started.wait()
        await a.cancel()
        release.set()
        assert await collect(b)


async def test_python_task_cancellation(setup):
    _, client = setup

    async def source():
        await asyncio.Event().wait()
        yield "Never"

    audio = client.tts.stream(source())
    task = asyncio.create_task(anext(audio))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not client.tts._streams


async def test_timeout_inheritance_and_explicit_disablement(setup):
    service, _ = setup
    service.mode = "burst"
    async with Rime(api_key="test", timeout=0.03) as client:
        async with client.tts.stream("Hi.") as a:
            await anext(a)
            await asyncio.sleep(0.06)
            with pytest.raises(RimeTimeoutError):
                await anext(a)
        async with client.tts.stream("Hi.", timeout=None) as b:
            await anext(b)
            await asyncio.sleep(0.06)
            assert await anext(b)


async def test_large_complete_input_and_unsplittable_sentence(setup):
    _, client = setup
    async with client.tts.stream("A short sentence. " * 4000) as audio:
        assert await anext(audio)
    async with client.tts.stream("x" * 70000) as audio:
        with pytest.raises(RimeResourceLimitError):
            await collect(audio)


@pytest.mark.parametrize("size", [1024, 100000])
@pytest.mark.parametrize("char,count", [("a", 65533), ("a", 65535), ("é", 32767)])
def test_sentence_limit_excludes_lookahead(size, char, count):
    first = char * count + "."
    text = first + " Short."
    buffer = SentenceBuffer(65536)
    actual = []
    for offset in range(0, len(text), size):
        actual.extend(buffer.feed(text[offset : offset + size]))
    actual.extend(buffer.feed("", final=True))
    assert actual == [first, " Short."]


@pytest.mark.parametrize("text", ["a" * 65536 + ". Short.", "é" * 32769, " " * 65537])
def test_sentence_limit_still_rejects_oversized_spans(text):
    buffer = SentenceBuffer(65536)
    with pytest.raises(RimeResourceLimitError):
        list(buffer.feed(text))
        list(buffer.feed("", final=True))


async def test_concurrent_operations_have_no_fixed_cap(setup):
    _, client = setup
    results = await asyncio.gather(*(collect(client.tts.stream("Hello.")) for _ in range(20)))
    assert all(results)
    assert not client.tts._streams


async def test_slow_input_is_not_a_service_stall(setup):
    _, client = setup

    async def source():
        yield "First sentence. The next sentence "
        await asyncio.sleep(0.3)
        yield "is complete."

    assert await collect(client.tts.stream(source()))


@pytest.mark.parametrize("size", [1, 7, 32, 511, 4096])
def test_shared_audio_bytes(size):
    case = json.loads((ROOT / "conformance/audio.json").read_text())
    raw = bytes.fromhex(case["pcm_hex"])
    converter = Converter(AudioFormat.MULAW_8000)
    actual = b"".join(converter.process(raw[i : i + size]) for i in range(0, len(raw), size))
    actual += converter.process(b"", final=True)
    assert actual.hex() == case["mulaw_hex"]


async def test_failed_operation_releases_state(setup):
    service, _ = setup
    service.mode = "error_before_audio"
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeUnavailableError):
            await collect(client.tts.stream("Hello."))
        await asyncio.sleep(0.01)
        assert not client.tts._streams


async def test_client_shutdown_stops_paused_output(setup):
    service, _ = setup
    service.mode = "burst"
    client = Rime(api_key="test")
    audio = client.tts.stream("Hello.")
    await anext(audio)
    await asyncio.sleep(0.03)
    await client.close()
    assert not client.tts._streams
    with pytest.raises(RimeCancelledError):
        await anext(audio)
    with pytest.raises(RimeInputError):
        client.tts.stream("Later.")


@pytest.mark.parametrize("profile", list(AudioFormat))
async def test_large_audio_is_delivered_in_bounded_chunks(setup, profile):
    service, client = setup
    # Test queue limits without making FIR conversion race the short stall timeout.
    client.tts._policy = replace(client.tts._policy, output_bytes=96, output_chunk_bytes=16)
    # Mu-law emits one byte per three PCM samples. Exceed the queue in both formats.
    samples = 3 * (client.tts._policy.output_bytes + 1)
    service.payload = b"\x00\x00" * samples
    async with client.tts.stream("Hello.", audio_format=profile) as audio:
        chunks = [part async for part in audio]
    assert all(0 < len(part) <= client.tts._policy.output_chunk_bytes for part in chunks)
    expected = (
        service.payload if profile is AudioFormat.PCM_24000 else b"\xff" * ((samples + 2) // 3)
    )
    assert len(expected) > client.tts._policy.output_bytes
    assert b"".join(chunks) == expected


@pytest.mark.parametrize("queued", [False, True])
async def test_overall_timeout_after_producer_completion(setup, queued):
    service, client = setup
    service.payload = b"\x00\x00" * (client.tts._policy.output_chunk_bytes if queued else 1)
    async with client.tts.stream("Hello.", timeout=0.1) as audio:
        assert await anext(audio)
        # Wait for production to finish without observing iterator completion.
        await asyncio.wait_for(asyncio.shield(audio._worker), 1)
        await asyncio.sleep(0.15)
        with pytest.raises(RimeTimeoutError):
            await anext(audio)


async def test_slow_consumer_does_not_trigger_stall_timeout(setup):
    service, client = setup
    client.tts._policy = replace(client.tts._policy, progress_timeout=0.02)
    # Keep the RPC open while a large response waits for output capacity.
    service.mode = "partial_error"
    service.payload = b"\x00\x00" * client.tts._policy.output_bytes
    async with client.tts.stream("Hello.") as audio:
        assert await anext(audio)
        await asyncio.sleep(0.08)
        assert await anext(audio)


async def test_incomplete_final_pcm_sample(setup):
    service, _ = setup
    service.payload = b"x"
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeAudioFormatError):
            async with client.tts.stream("Hello.") as audio:
                await collect(audio)


@pytest.mark.parametrize("location", ["headers", "trailers", "both"])
@pytest.mark.parametrize(
    "status,error_type",
    [
        (grpc.StatusCode.PERMISSION_DENIED, RimePermissionError),
        (grpc.StatusCode.UNAUTHENTICATED, RimeAuthenticationError),
        (grpc.StatusCode.UNAVAILABLE, RimeUnavailableError),
    ],
)
async def test_server_error_without_audio_metadata(setup, location, status, error_type):
    service, client = setup
    service.mode = "no_audio_error"
    service.rejection_status = status
    service.response_metadata = (("x-request-id", "header-id"),) if location != "trailers" else ()
    service.trailing_metadata = (("x-request-id", "trailer-id"),) if location != "headers" else ()
    expected_id = "trailer-id" if location == "trailers" else "header-id"
    async with client.tts.stream("Hello.", timeout=1) as audio:
        result = asyncio.create_task(collect(audio))
        await service.headers_sent.wait()
        # Missing format metadata must not mask a later server rejection.
        await asyncio.sleep(0.01)
        assert not result.done()
        service.release.set()
        with pytest.raises(error_type) as caught:
            await result
        assert caught.value.request_id == expected_id
        assert audio.request_id == expected_id
    assert len(service.calls) == 1


@pytest.mark.parametrize("mode", ["normal", "empty_audio", "empty_no_headers"])
async def test_missing_audio_metadata_cannot_succeed(setup, mode):
    service, client = setup
    service.mode = mode
    service.response_metadata = (("x-request-id", "header-id"),)
    async with client.tts.stream("Hello.", timeout=1) as audio:
        with pytest.raises(RimeAudioFormatError):
            await anext(audio)
    assert len(service.calls) == 1
    assert [message.WhichOneof("payload") for message in service.calls[0]] == [
        "header",
        "text_chunk",
    ]


async def test_trailers_only_synthesis_error_keeps_request_id(setup):
    service, client = setup
    service.mode = "error_before_audio"

    async def source():
        await asyncio.Event().wait()
        yield "Never"

    async with client.tts.stream(source(), timeout=1) as audio:
        with pytest.raises(RimeUnavailableError) as caught:
            await collect(audio)
        assert caught.value.request_id == "rejected-request"
        assert audio.request_id == "rejected-request"


@pytest.mark.parametrize("namespace", ["voices", "languages"])
@pytest.mark.parametrize("location", ["headers", "trailers", "both"])
async def test_discovery_error_keeps_request_id(setup, namespace, location):
    service, client = setup
    service.mode = "discovery_error"
    service.rejection_status = grpc.StatusCode.PERMISSION_DENIED
    service.response_metadata = (("x-request-id", "header-id"),) if location != "trailers" else ()
    service.trailing_metadata = (("x-request-id", "trailer-id"),) if location != "headers" else ()
    with pytest.raises(RimePermissionError) as caught:
        await getattr(client, namespace).list()
    assert caught.value.request_id == ("trailer-id" if location == "trailers" else "header-id")


async def test_client_close_releases_completed_token(setup):
    _, client = setup
    await client.languages.list()
    token = weakref.ref(client._credentials._token)
    await client.close()
    await asyncio.sleep(0)
    gc.collect()
    assert token() is None
    assert client._credentials._refresh is None
    assert client._credentials._key == ""
    await client.close()
