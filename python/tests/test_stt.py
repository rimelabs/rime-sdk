"""Public recognition behavior through a real local gRPC connection."""

import asyncio
import json
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path

import grpc
import pytest
from google.protobuf.json_format import ParseDict
from rime_api import speech_to_text_pb2 as proto
from stt_service import RecognitionService

from rimelabs_sdk import (
    PCMFormat,
    Rime,
    RimeAudioFormatError,
    RimeAuthenticationError,
    RimeCancelledError,
    RimeInputError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
    RimeUnavailableError,
    TranscriptionFinal,
    TranscriptionMode,
)
from rimelabs_sdk.stt import _policy, _transport
from rimelabs_sdk.stt._protocol import TranscriptState

CASES = json.loads((Path(__file__).parents[2] / "conformance/stt/transcripts.json").read_text())[
    "cases"
]


@pytest.fixture
async def stt_setup(monkeypatch):
    async with RecognitionService() as service:
        monkeypatch.setattr(
            _policy,
            "POLICY",
            replace(
                _policy.POLICY,
                target=service.target,
                acceptance_timeout=0.2,
                completion_timeout=0.2,
                cleanup_timeout=0.05,
            ),
        )
        # Keep production message limits and retry policy while using local plaintext.
        monkeypatch.setattr(
            grpc.aio,
            "secure_channel",
            lambda target, credentials, **kwargs: grpc.aio.insecure_channel(target, **kwargs),
        )
        async with Rime(api_key="test-key") as client:
            yield service, client


async def test_transcribe_forwards_complete_wav_and_returns_result(stt_setup, monkeypatch):
    service, client = stt_setup
    monkeypatch.setattr(_policy, "POLICY", replace(_policy.POLICY, completion_timeout=3))
    # A full-size payload exercises the unary gRPC limit rather than stream chunk limits.
    audio = b"RIFF" + bytes(16 * 1024 * 1024 - 4)
    result = await client.stt.transcribe(
        audio,
        language="en-GB",
        mode=TranscriptionMode.VERBATIM,
        context_terms=[" Super-G ", "uno,dos", "Español"],
    )
    assert (result.text, result.language, result.request_id) == ("Ice cream", "en", "stt-request")
    assert service.calls == []
    (request,) = service.unary_calls
    assert request.audio == audio
    assert request.options.language == "en-GB"
    assert request.options.mode == proto.TRANSCRIPTION_MODE_VERBATIM
    assert list(request.options.context_terms) == [" Super-G ", "uno,dos", "Español"]
    assert service.metadata[0]["authorization"] == "Bearer test-key"


@pytest.mark.parametrize(
    "mode,error",
    [
        ("reject", RimeUnavailableError),
        ("missing_language", RimeStreamError),
        ("no_completion", RimeTimeoutError),
    ],
)
async def test_transcribe_failure_is_not_retried(stt_setup, mode, error):
    service, client = stt_setup
    service.mode = mode
    with pytest.raises(error) as failure:
        await client.stt.transcribe(b"wav", language="en")
    assert failure.value.request_id == ("stt-rejected" if mode == "reject" else "stt-request")
    assert len(service.unary_calls) == 1


async def test_transcribe_silence_and_independent_defaults(stt_setup):
    service, _ = stt_setup
    service.mode = "silence"
    async with Rime(api_key="test-key", timeout=0.000001) as short_tts_client:
        assert (await short_tts_client.stt.transcribe(b"wav", language="en")).text == ""
    assert service.unary_calls[0].options.mode == proto.TRANSCRIPTION_MODE_WRITTEN


@pytest.mark.parametrize("action", ["caller", "close", "deadline"])
async def test_transcribe_cancellation(stt_setup, action):
    service, client = stt_setup
    service.mode = "no_completion"
    task = asyncio.create_task(
        client.stt.transcribe(b"wav", language="en", timeout=0.08 if action == "deadline" else None)
    )
    await asyncio.wait_for(service.config_seen.wait(), 1)
    await asyncio.sleep(0.01)  # let response headers reach the client
    if action == "close":
        await client.close()
    elif action == "caller":
        task.cancel()
    error = {
        "caller": asyncio.CancelledError,
        "close": RimeCancelledError,
        "deadline": RimeTimeoutError,
    }[action]
    with pytest.raises(error) as failure:
        await task
    if action != "caller":
        # grpc.aio may expose unary response headers only once the call completes.
        assert failure.value.request_id in (None, "stt-request")
    await asyncio.wait_for(service.cancelled.wait(), 1)


async def test_transcribe_rejects_invalid_inputs_before_network(stt_setup):
    service, client = stt_setup
    for audio, error in [
        (b"", RimeAudioFormatError),
        ("wav", RimeAudioFormatError),
        (bytes(16 * 1024 * 1024 + 1), RimeResourceLimitError),
    ]:
        with pytest.raises(error):
            await client.stt.transcribe(audio, language="en")
    with pytest.raises(RimeInputError):
        await client.stt.transcribe(b"wav", language="en", mode="other")
    assert not service.unary_calls


async def source(*chunks):
    for chunk in chunks or (b"\0\0",):
        yield chunk


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_transcript_contract(case):
    state = TranscriptState(proto, 65536)
    actual = []

    def run():
        for raw in case["messages"]:
            message = ParseDict(
                raw, proto.StreamingTranscriptionResponse(), ignore_unknown_fields=True
            )
            update = state.accept(message, input_done=case.get("inputDone", True))
            if update is not None:
                actual.append(asdict(update))
        actual.append(asdict(state.finish()))

    if "error" in case:
        with pytest.raises(RimeStreamError):
            run()
    else:
        run()
        assert actual == case["expected"]


async def test_stream_is_lazy_and_returns_partials_before_input_ends(stt_setup):
    service, client = stt_setup
    release = asyncio.Event()

    async def live():
        yield b"\0\0"
        await release.wait()

    stream = client.stt.stream(
        live(),
        language="en-GB",
        mode=TranscriptionMode.VERBATIM,
        context_terms=[" Super-G ", "uno,dos", "Español"],
    )
    assert service.calls == []
    async with stream:
        first = await anext(stream)
        second = await anext(stream)
        assert (first.kind, first.text, second.text) == ("partial", "I scream", "Ice cream")
        assert not service.input_finished.is_set()
        release.set()
        final = await anext(stream)
        assert final == TranscriptionFinal(text="Ice cream", language="en")
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert stream.request_id == "stt-request"
    config = service.calls[0][0].config
    assert config.language == "en-GB"
    assert config.mode == proto.TRANSCRIPTION_MODE_VERBATIM
    assert list(config.context_terms) == [" Super-G ", "uno,dos", "Español"]
    assert service.metadata[0]["authorization"] == "Bearer test-key"


async def test_source_is_not_touched_before_acceptance(stt_setup):
    service, client = stt_setup
    service.mode = "no_acceptance"
    touched = False

    async def live():
        nonlocal touched
        touched = True
        yield b"\0\0"

    stream = client.stt.stream(live(), language="en")
    with pytest.raises(RimeTimeoutError, match="acceptance") as caught:
        await anext(stream)
    assert not touched
    assert caught.value.request_id == "stt-request"
    await stream.cancel()


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("missing_final", RimeStreamError),
        ("duplicate_final", RimeStreamError),
        ("done_then_error", RimeUnavailableError),
        ("early_final", RimeStreamError),
        ("no_completion", RimeTimeoutError),
    ],
)
async def test_incomplete_transports_never_publish_a_final(stt_setup, mode, expected):
    service, client = stt_setup
    service.mode = mode
    updates = []

    async def unfinished():
        await asyncio.Future()
        yield b"\0\0"

    stream = client.stt.stream(unfinished() if mode == "early_final" else source(), language="en")
    with pytest.raises(expected) as caught:
        async with stream:
            async for update in stream:
                updates.append(update)
    assert all(update.kind != "final" for update in updates)
    assert caught.value.request_id == "stt-request"
    if mode == "done_then_error":
        assert str(caught.value) == "deliberate status failure after done"
    assert len(service.calls) == 1


async def test_cancelling_the_reader_cancels_only_its_operation(stt_setup):
    service, client = stt_setup
    service.mode = "no_acceptance"
    stream = client.stt.stream(source(), language="en")
    reader = asyncio.create_task(anext(stream))
    await service.config_seen.wait()
    reader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reader
    with pytest.raises(RimeCancelledError):
        await anext(stream)
    assert not client.stt._streams
    service.mode = "normal"
    updates = [update async for update in client.stt.stream(source(), language="en")]
    assert updates[-1].kind == "final"


@pytest.mark.parametrize(
    "status,error",
    [
        (grpc.StatusCode.UNAUTHENTICATED, RimeAuthenticationError),
        (grpc.StatusCode.PERMISSION_DENIED, RimePermissionError),
        (grpc.StatusCode.INVALID_ARGUMENT, RimeInputError),
        (grpc.StatusCode.RESOURCE_EXHAUSTED, RimeResourceLimitError),
        (grpc.StatusCode.UNAVAILABLE, RimeUnavailableError),
        (grpc.StatusCode.DEADLINE_EXCEEDED, RimeTimeoutError),
        (grpc.StatusCode.CANCELLED, RimeCancelledError),
        (grpc.StatusCode.INTERNAL, RimeStreamError),
    ],
)
async def test_rejection_keeps_status_and_trailing_request_id(stt_setup, status, error):
    service, client = stt_setup
    service.mode, service.rejection = "reject", status
    stream = client.stt.stream(source(), language="und")
    with pytest.raises(error) as caught:
        async with stream:
            await anext(stream)
    assert caught.value.request_id == stream.request_id == "stt-rejected"
    assert str(caught.value) == "deliberate rejection"
    assert service.calls[0][0].config.language == "und"
    await asyncio.wait_for(service.completed.wait(), 1)
    assert service.cancelled.is_set() == (status == grpc.StatusCode.CANCELLED)


async def test_failure_after_partials_preserves_service_diagnostics(stt_setup):
    service, client = stt_setup
    service.mode = "partial_error"
    async with client.stt.stream(source(), language="en") as stream:
        assert (await anext(stream)).kind == "partial"
        service.release.set()
        with pytest.raises(
            RimeUnavailableError, match="^deliberate error after partials$"
        ) as caught:
            async for update in stream:
                assert update.kind != "final"
        assert caught.value.request_id == stream.request_id == "stt-request"
    assert len(service.calls) == 1


@pytest.mark.parametrize("language", ["", " \t\n"])
async def test_blank_language_is_passed_unchanged_for_service_validation(stt_setup, language):
    service, client = stt_setup
    service.mode, service.rejection = "reject", grpc.StatusCode.INVALID_ARGUMENT
    async with client.stt.stream(source(), language=language) as stream:
        with pytest.raises(RimeInputError) as caught:
            await anext(stream)
    assert service.calls[0][0].config.language == language
    assert caught.value.request_id == "stt-rejected"


async def test_valid_silence_has_an_empty_final(stt_setup):
    service, client = stt_setup
    service.mode = "silence"
    async with client.stt.stream(source(), language="en") as stream:
        assert [update async for update in stream] == [TranscriptionFinal(text="", language="en")]
    await asyncio.wait_for(service.completed.wait(), 1)
    assert service.active == 0
    assert not service.cancelled.is_set()


async def test_arbitrary_audio_chunks_are_converted_before_writing(stt_setup):
    service, client = stt_setup
    service.mode = "silence"
    async with client.stt.stream(
        source(b"\x02", b"\x00\x04", b"\x00"), language="en", input_format=PCMFormat(channels=2)
    ) as stream:
        assert (await anext(stream)).kind == "final"
    assert b"".join(message.audio for message in service.calls[0][1:]) == b"\x03\x00"


@pytest.mark.parametrize("bad", [b"\x01", "not bytes"])
async def test_invalid_source_audio_cancels_instead_of_committing(stt_setup, bad, monkeypatch):
    service, client = stt_setup
    half_closes = []
    original_finish = _transport.TranscriptionCall.finish_input

    async def finish(call):
        half_closes.append(call)
        await original_finish(call)

    monkeypatch.setattr(_transport.TranscriptionCall, "finish_input", finish)
    stream = client.stt.stream(source(bad), language="en")
    with pytest.raises(RimeAudioFormatError):
        async with stream:
            await anext(stream)
    await asyncio.wait_for(service.cancelled.wait(), 1)
    assert half_closes == []


async def test_source_exception_is_input_failure(stt_setup, monkeypatch):
    _, client = stt_setup
    half_closes = []
    original_finish = _transport.TranscriptionCall.finish_input

    async def finish(call):
        half_closes.append(call)
        await original_finish(call)

    monkeypatch.setattr(_transport.TranscriptionCall, "finish_input", finish)

    async def broken():
        raise ValueError("source failed")
        yield b""

    stream = client.stt.stream(broken(), language="en")
    with pytest.raises(RimeInputError, match="source failed"):
        async with stream:
            await anext(stream)
    assert half_closes == []


async def test_cancellation_unblocks_pending_source_and_closes_generator(stt_setup):
    service, client = stt_setup
    closed = asyncio.Event()

    async def blocked():
        try:
            yield b"\0\0"
            await asyncio.Event().wait()
        finally:
            closed.set()

    stream = client.stt.stream(blocked(), language="en")
    assert (await anext(stream)).kind == "partial"
    await asyncio.wait_for(stream.cancel(), 1)
    await asyncio.wait_for(closed.wait(), 1)
    await asyncio.wait_for(service.cancelled.wait(), 1)
    await stream.cancel()
    assert not client.stt._streams
    with pytest.raises(RimeCancelledError):
        await anext(stream)


async def test_cancelling_slow_source_cleanup_releases_its_task(stt_setup):
    _, client = stt_setup
    waiting, closed = asyncio.Event(), asyncio.Event()
    cleanup_tasks = []

    class BlockingSource:
        def __aiter__(self):
            return self

        async def __anext__(self):
            waiting.set()
            await asyncio.Future()

        async def aclose(self):
            cleanup_tasks.append(asyncio.current_task())
            try:
                await asyncio.Future()
            finally:
                closed.set()

    stream = client.stt.stream(BlockingSource(), language="en")
    reader = asyncio.create_task(anext(stream))
    await asyncio.wait_for(waiting.wait(), 1)
    try:
        await asyncio.wait_for(stream.cancel(), 1)
        with pytest.raises(RimeCancelledError):
            await reader
        await client.close()
        await asyncio.sleep(0)
        assert closed.is_set()
        assert cleanup_tasks and all(task.done() for task in cleanup_tasks)
    finally:
        for task in cleanup_tasks:
            task.cancel()
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)


@pytest.mark.parametrize("timeout", [timedelta(seconds=1), timedelta.max])
async def test_timedelta_timeout_is_independent_of_tts_default(stt_setup, timeout):
    async with Rime(api_key="test", timeout=timedelta(microseconds=1)) as client:
        async with client.stt.stream(source(), language="en", timeout=timeout) as stream:
            updates = [update async for update in stream]
        assert updates[-1] == TranscriptionFinal(text="Ice cream", language="en")


async def test_timedelta_deadline_expires_while_waiting_for_source(stt_setup):
    _, client = stt_setup

    async def blocked():
        await asyncio.Event().wait()
        yield b"\0\0"

    async with client.stt.stream(
        blocked(), language="en", timeout=timedelta(milliseconds=30)
    ) as stream:
        with pytest.raises(RimeTimeoutError, match="Overall transcription deadline expired"):
            await anext(stream)


async def test_overall_deadline_runs_while_consumer_is_paused(stt_setup):
    service, client = stt_setup
    service.mode = "burst"
    stream = client.stt.stream(source(), language="en", timeout=0.05)
    assert (await anext(stream)).kind == "partial"
    await asyncio.sleep(0.09)
    assert len(stream._queue._items) <= _policy.POLICY.queued_updates
    with pytest.raises(RimeTimeoutError):
        await anext(stream)
    await asyncio.wait_for(stream.cancel(), 1)


@pytest.mark.parametrize("queue_limit", [1, 16])
async def test_completed_transcription_survives_a_paused_consumer(
    stt_setup, monkeypatch, queue_limit
):
    _, client = stt_setup
    monkeypatch.setattr(_policy, "POLICY", replace(_policy.POLICY, queued_updates=queue_limit))
    stream = client.stt.stream(source(), language="en")
    final_ready = asyncio.Event()
    put = stream._queue.put

    async def observe_final(update):
        if isinstance(update, TranscriptionFinal):
            final_ready.set()
        await put(update)

    monkeypatch.setattr(stream._queue, "put", observe_final)
    assert (await anext(stream)).text == "I scream"
    await asyncio.wait_for(final_ready.wait(), 1)
    await asyncio.sleep(_policy.POLICY.completion_timeout + 0.05)
    updates = [update async for update in stream]
    assert [(update.kind, update.text) for update in updates] == [
        ("partial", "Ice cream"),
        ("final", "Ice cream"),
    ]
    assert updates[-1].language == "en"
    assert stream.request_id == "stt-request"


async def test_overall_deadline_still_applies_to_a_completed_transcription(stt_setup):
    _, client = stt_setup
    stream = client.stt.stream(source(), language="en", timeout=0.4)
    assert (await anext(stream)).kind == "partial"
    await asyncio.wait_for(asyncio.shield(stream._worker), 1)
    assert stream in client.stt._streams
    await asyncio.sleep(0.45)
    assert stream not in client.stt._streams
    with pytest.raises(RimeTimeoutError, match="Overall transcription deadline expired"):
        await anext(stream)


async def test_single_reader_and_cancel_before_start(stt_setup):
    service, client = stt_setup
    service.mode = "no_acceptance"
    stream = client.stt.stream(source(), language="en")
    reader = asyncio.create_task(anext(stream))
    await service.config_seen.wait()
    with pytest.raises(RimeInputError, match="one concurrent reader"):
        await anext(stream)
    await stream.cancel()
    with pytest.raises(RimeCancelledError):
        await reader
    never = client.stt.stream(source(), language="en")
    await never.cancel()
    with pytest.raises(RimeCancelledError):
        await anext(never)
    assert len(service.calls) == 1


async def test_cancel_one_stream_leaves_sibling_alive(stt_setup):
    service, client = stt_setup
    first = client.stt.stream(source(), language="en")
    second = client.stt.stream(source(), language="en")
    await first.cancel()
    async with second:
        assert (await anext(second)).kind == "partial"
        assert [update.kind async for update in second][-1] == "final"
    assert len(service.calls) == 1


async def test_client_close_cancels_started_and_unstarted_operations(stt_setup):
    service, client = stt_setup
    service.mode = "no_acceptance"
    started = client.stt.stream(source(), language="en")
    lazy = client.stt.stream(source(), language="en")
    pending = asyncio.create_task(anext(started))
    await service.config_seen.wait()
    await asyncio.wait_for(client.close(), 1)
    with pytest.raises(RimeCancelledError):
        await pending
    with pytest.raises(RimeCancelledError):
        await anext(lazy)
    assert not client.stt._streams


async def test_cleanup_failure_still_closes_all_features(monkeypatch):
    client = Rime(api_key="test")
    closed = []

    async def broken():
        closed.append("realtime")
        raise RuntimeError("cleanup failure")

    async def close_stt():
        closed.append("stt")

    async def close_tts():
        closed.append("tts")

    monkeypatch.setattr(client.realtime, "_close", broken)
    monkeypatch.setattr(client.stt, "_close", close_stt)
    monkeypatch.setattr(client.tts, "_close", close_tts)
    with pytest.raises(RuntimeError, match="cleanup failure"):
        await client.close()
    assert closed == ["realtime", "stt", "tts"]
    assert client._credentials._closed
    with pytest.raises(RuntimeError, match="cleanup failure"):
        await client.close()


async def test_close_while_connecting_does_not_wait_for_connection_deadline(stt_setup, monkeypatch):
    _, client = stt_setup
    connecting = asyncio.Event()
    closed = []

    class Channel:
        async def channel_ready(self):
            connecting.set()
            await asyncio.Event().wait()

        async def close(self):
            closed.append(True)

    monkeypatch.setattr(_transport, "make_channel", lambda policy: Channel())
    stream = client.stt.stream(source(), language="en")
    reader = asyncio.create_task(anext(stream))
    await connecting.wait()
    await asyncio.wait_for(client.close(), 0.5)
    with pytest.raises(RimeCancelledError):
        await reader
    assert closed == [True]


async def test_stt_uses_fixed_endpoint_and_no_tts_connection(monkeypatch):
    targets = []

    def channel(policy):
        targets.append(policy.target)
        raise RimeUnavailableError("test dial")

    monkeypatch.setattr(_transport, "make_channel", channel)
    async with Rime(api_key="test", model="mistv3", endpoint="tts.example:123") as client:
        async with client.stt.stream(source(), language="en") as stream:
            with pytest.raises(RimeUnavailableError):
                await anext(stream)
        assert client.tts._channel is None
    assert targets == ["stt.api.rime.ai:443"]


async def test_cancelling_stt_preserves_tts_and_realtime(setup, stt_setup, monkeypatch):
    from test_realtime import accepted_turn, session

    tts_service, _ = setup
    async with session(monkeypatch) as (realtime, peer, client):
        recognition = client.stt.stream(source(), language="en")
        assert (await anext(recognition)).kind == "partial"
        await recognition.cancel()
        async with client.tts.synthesize("Hello.") as speech:
            assert b"".join([chunk async for chunk in speech]) == tts_service.payload
        response = await accepted_turn(realtime, peer)
        peer.ended(response.response_id)
        assert not peer.closed
        pending = client.stt.stream(source(), language="en")
        await client.close()
        assert peer.closed
        with pytest.raises(RimeCancelledError):
            await anext(pending)


async def test_transcript_queue_blocks_atomically_and_failure_releases_waiters():
    from rimelabs_sdk import TranscriptionPartial
    from rimelabs_sdk.stt._queue import TranscriptQueue

    queue = TranscriptQueue(1)
    first = TranscriptionPartial(text="First complete snapshot")
    second = TranscriptionPartial(text="Second complete snapshot")
    await queue.put(first)
    writer = asyncio.create_task(queue.put(second))
    await asyncio.sleep(0)
    assert not writer.done()
    assert await queue.get() == first
    await writer
    assert await queue.get() == second
    reader = asyncio.create_task(queue.get())
    await asyncio.sleep(0)
    queue.fail(RimeCancelledError("cancelled"))
    with pytest.raises(RimeCancelledError):
        await reader
    full = TranscriptQueue(1)
    await full.put(first)
    writer = asyncio.create_task(full.put(second))
    await asyncio.sleep(0)
    full.fail(RimeCancelledError("cancelled"))
    await asyncio.wait_for(writer, 0.2)


def test_transcript_size_limit_counts_utf8_bytes():
    state = TranscriptState(proto, 4)
    state.accept(
        ParseDict(CASES[0]["messages"][0], proto.StreamingTranscriptionResponse()), input_done=False
    )
    with pytest.raises(RimeResourceLimitError):
        state.accept(
            proto.StreamingTranscriptionResponse(
                hypothesis=proto.TranscriptionHypothesis(text="ééé", revision=1)
            ),
            input_done=False,
        )


async def test_context_enter_then_immediate_exit_cancels_unstarted_worker(stt_setup):
    service, client = stt_setup
    stream = client.stt.stream(source(), language="en")
    async with stream:
        pass
    assert service.calls == []
    assert not client.stt._streams


async def test_stt_file_example(stt_setup, monkeypatch, tmp_path, capsys):
    from test_examples import load_example

    service, _ = stt_setup
    path = tmp_path / "utterance.pcm"
    path.write_bytes(b"\0\0" * 1600)
    monkeypatch.setenv("RIME_API_KEY", "test-key")
    await load_example("stt/stream.py").main(path, "en")
    output = capsys.readouterr().out
    assert "partial: I scream" in output
    assert "final: Ice cream" in output
    assert "request_id: stt-request" in output
    assert service.input_finished.is_set()
