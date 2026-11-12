"""Optional Prism settings and review callbacks through the public session."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from test_realtime import Peer, session

from rimelabs_sdk import Rime, RimeInputError, RimeStreamError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


@asynccontextmanager
async def configured(monkeypatch, *, created_version=None, **options):
    peer = Peer()
    if created_version is not None:
        peer.incoming.get_nowait()
        peer.emit("session.created", session={"id": "session-1", "version": created_version})

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        asyncio.timeout(3),
        Rime(api_key="test") as client,
        client.realtime.connect(**options) as current,
    ):
        yield current, peer


def requested(peer, index=0, response="guess", **fields):
    peer.emit(
        "prism.review.requested",
        review_id=f"review-{response}-{index}",
        response_id=response,
        item_id="speech",
        index=index,
        text="Proposed speech",
        **fields,
    )


async def test_optional_settings_and_confirmed_update(monkeypatch):
    async with configured(
        monkeypatch,
        speed=1.1,
        turn_detection=r.TurnDetection(
            end_of_turn_min_silence_ms=800,
            interrupt_min_words=2,
            interrupt_phrases=("stop",),
            acknowledgment_phrases=("okay",),
        ),
    ) as (current, peer):
        assert peer.settings["speed"] == 1.1
        assert peer.settings["turn_detection"] == {
            "interrupt_response": True,
            "end_of_turn_min_silence_ms": 800,
            "interrupt_min_words": 2,
            "interrupt_phrases": ["stop"],
            "acknowledgment_phrases": ["okay"],
        }
        assert current.info.speed is None  # The peer has not reported a value.
        pending = asyncio.create_task(
            current.update(
                speed=0.9,
                interrupt_on_speech=False,
                turn_detection=r.TurnDetectionUpdate(interrupt_phrases=()),
            )
        )
        request = await peer.next("session.update")
        assert request["session"] == {
            "speed": 0.9,
            "turn_detection": {"interrupt_response": False, "interrupt_phrases": []},
        }
        assert current.info.speed is None
        peer.emit(
            "session.updated",
            prism_request_event_id=request["event_id"],
            session={
                "id": "session-1",
                "speed": 0.9,
                "version": "build-hash",
                "prism_effective_interrupt_response": False,
                "turn_detection": {"interrupt_phrases": [], "end_of_turn_min_silence_ms": 800},
            },
        )
        info = await pending
        assert info.speed == 0.9 and info.version == "build-hash"
        assert info.turn_detection.interrupt_phrases == ()
        assert info.turn_detection.end_of_turn_min_silence_ms == 800


async def test_default_settings_remain_omitted(monkeypatch):
    async with configured(monkeypatch) as (current, peer):
        assert "speed" not in peer.settings
        assert "prism_review" not in peer.settings
        assert peer.settings["turn_detection"] == {"interrupt_response": True}
        assert current.info.version is None and current.info.review is None


async def test_transcript_correction_and_timestamp_reach_public_events(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        events = current.events
        for body in (
            {"delta": "book the night"},
            {"delta": "", "transcript": "book the nine"},
            {"delta": "", "transcript": ""},
        ):
            peer.emit(
                "conversation.item.input_audio_transcription.delta",
                item_id="input",
                server_time_ms=1792857632418,
                **body,
            )
            event = await anext(events)
            assert event.server_time_ms == 1792857632418
            assert event.payload.transcript == body.get("transcript")
        peer.emit("prism.typed_input.ready")
        assert (await anext(events)).server_time_ms is None


@pytest.mark.parametrize("path", ["typed", "instruction", "history"])
async def test_long_text_is_submitted_to_server(monkeypatch, path):
    async with session(monkeypatch) as (current, peer, _):
        text = "é" * 4100
        peer.emit("prism.typed_input.ready")
        operation = (
            current.send_text(text)
            if path == "typed"
            else current.request_reply(instruction=text)
            if path == "instruction"
            else current.add_message("user", text)
        )
        pending = asyncio.create_task(operation)
        request = await peer.next(
            "conversation.item.create" if path == "history" else "response.create"
        )
        if path == "history":
            assert request["item"]["text"] == text
            peer.emit(
                "conversation.item.created",
                prism_request_event_id=request["event_id"],
                item={"id": "history", "type": "message", "role": "user"},
            )
        else:
            assert (
                request["response"]["prism_input_text" if path == "typed" else "prism_instruction"]
                == text
            )
            peer.accepted(request)
        await pending


@pytest.mark.parametrize("proactive", [False, True])
@pytest.mark.parametrize("newer_speech", [False, True])
async def test_rejected_text_restores_only_unchanged_admission(
    monkeypatch, proactive, newer_speech
):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("prism.typed_input.ready")
        pending = asyncio.create_task(
            current.request_reply(instruction="too long")
            if proactive
            else current.send_text("too long")
        )
        request = await peer.next("response.create")
        if newer_speech:
            peer.emit("input_audio_buffer.speech_started", item_id="spoken", audio_start_ms=0)
        peer.emit(
            "error",
            error={
                "code": "invalid_instruction" if proactive else "invalid_typed_input",
                "scope": "event",
                "owner": {"event_id": request["event_id"]},
            },
        )
        with pytest.raises(r.RimeRealtimeError):
            await pending
        following = asyncio.create_task(current.send_text("short follow-up"))
        try:
            if newer_speech:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(peer.sent.get(), 0.03)
                peer.emit("prism.typed_input.ready")
            peer.accepted(await peer.next("response.create"))
            await following
        finally:
            following.cancel()
            await asyncio.gather(following, return_exceptions=True)


@pytest.mark.parametrize(
    "decision,action",
    [(r.Allow(), "allow"), (r.Replace("Replacement"), "replace"), (r.Stop(), "stop")],
)
async def test_review_before_response_created_without_event_consumer(monkeypatch, decision, action):
    seen = []

    async def callback(request):
        seen.append(request)
        return decision

    async with configured(monkeypatch, review=r.Review(callback=callback)) as (_, peer):
        requested(peer)
        result = await peer.next("prism.review.result")
        assert result["action"] == action
        assert result["review_id"] == "review-guess-0"
        assert seen[0].response_id == "guess"
        assert result.get("text") == ("Replacement" if action == "replace" else None)
        requested(peer)  # A duplicate cannot execute the policy twice.
        requested(peer, response="other")
        await peer.next("prism.review.result")
        assert len(seen) == 2


@pytest.mark.parametrize("error_type", [ValueError, TimeoutError])
async def test_review_exception_sends_stop_and_fails_session(monkeypatch, error_type):
    async def callback(request):
        raise error_type("private policy data")

    async with configured(monkeypatch, review=r.Review(callback=callback)) as (current, peer):
        requested(peer)
        assert (await peer.next("prism.review.result"))["action"] == "stop"
        with pytest.raises(RimeStreamError, match="Speech review callback failed") as caught:
            await anext(current.events)
        assert "private" not in str(caught.value)


async def test_review_timeout_cancels_policy_without_sending_verdict(monkeypatch):
    cancelled = asyncio.Event()

    async def callback(request):
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async with configured(
        monkeypatch, review=r.Review(callback=callback, timeout_ms=100, on_timeout=r.Speak())
    ) as (_, peer):
        requested(peer)
        await asyncio.wait_for(cancelled.wait(), 1)
        assert peer.sent.empty()


async def test_replace_keeps_earlier_review_and_cancels_later_review(monkeypatch):
    entered = [asyncio.Event() for _ in range(3)]
    release = [asyncio.Event() for _ in range(3)]
    cancelled = asyncio.Event()

    async def callback(request):
        entered[request.index].set()
        try:
            await release[request.index].wait()
            return r.Replace("end") if request.index == 1 else r.Allow()
        except asyncio.CancelledError:
            if request.index == 2:
                cancelled.set()
            raise

    async with configured(monkeypatch, review=r.Review(callback=callback, timeout_ms=5000)) as (
        _,
        peer,
    ):
        for index in range(3):
            requested(peer, index)
        await asyncio.gather(*(item.wait() for item in entered))
        release[1].set()
        assert (await peer.next("prism.review.result"))["action"] == "replace"
        await cancelled.wait()
        release[0].set()
        assert (await peer.next("prism.review.result"))["review_id"] == "review-guess-0"


async def test_close_cancels_provisional_review(monkeypatch):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def callback(request):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async with configured(monkeypatch, review=r.Review(callback=callback, timeout_ms=5000)) as (
        current,
        peer,
    ):
        requested(peer)
        await entered.wait()
        await current.close()
        assert cancelled.is_set() and peer.sent.empty()


@pytest.mark.parametrize("field,value", [("speed", None), ("version", 2), ("prism_review", None)])
async def test_invalid_received_settings_fail_session(monkeypatch, field, value):
    async with session(monkeypatch) as (current, peer, _):
        peer.emit("session.updated", session={"id": "session-1", field: value})
        with pytest.raises(RimeStreamError):
            await anext(current.events)


def test_phrase_input_is_snapshotted_and_fixed_fields_not_updatable():
    phrases = ["stop"]
    value = r.TurnDetectionUpdate(interrupt_phrases=phrases)
    phrases.clear()
    assert value.interrupt_phrases == ("stop",)
    with pytest.raises(TypeError, match="end_of_turn_min_silence_ms"):
        r.TurnDetectionUpdate(end_of_turn_min_silence_ms=100)
    with pytest.raises(RimeInputError):
        r.TurnDetection(end_of_turn_min_silence_ms=-1)


async def test_version_from_created_survives_omitted_updates(monkeypatch):
    async with configured(monkeypatch, created_version="created-build") as (current, peer):
        assert current.info.version == "created-build"
        pending = asyncio.create_task(current.update(speed=1.1))
        request = await peer.next("session.update")
        peer.emit(
            "session.updated",
            prism_request_event_id=request["event_id"],
            session={"id": "session-1"},
        )
        assert (await pending).version == "created-build"


async def test_cancel_write_cancels_review_before_terminal_event(monkeypatch):
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def review(request):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with configured(monkeypatch, review=r.Review(callback=review)) as (current, peer):
        peer.emit("prism.typed_input.ready")
        pending = asyncio.create_task(current.send_text("Hello"))
        request = await peer.next("response.create")
        peer.accepted(request, response_id="active")
        response = await pending
        requested(peer, response="active")
        await started.wait()
        pending_cancel = asyncio.create_task(current.cancel(response))
        await peer.next("response.cancel")
        await cancelled.wait()
        assert not pending_cancel.done()
        peer.ended(response_id="active", status="cancelled")
        await pending_cancel


async def test_expired_review_does_not_send_after_writer_is_released(monkeypatch):
    completed = asyncio.Event()

    async def review(request):
        completed.set()
        return r.Allow()

    async with configured(monkeypatch, review=r.Review(callback=review, timeout_ms=100)) as (
        current,
        peer,
    ):
        async with current._write_lock:
            requested(peer)
            await completed.wait()
            await asyncio.sleep(0.15)
        # The following request passes through the same writer after the review.
        pending = asyncio.create_task(current.update(speed=1.1))
        request = await peer.next("session.update")
        peer.emit(
            "session.updated",
            prism_request_event_id=request["event_id"],
            session={"id": "session-1"},
        )
        await pending
        assert peer.sent.empty()


@pytest.mark.parametrize("speed", [True, 0.84, 1.31, float("nan"), float("inf")])
async def test_invalid_speed_fails_before_connect(monkeypatch, speed):
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="Speed"):
            async with client.realtime.connect(speed=speed):
                pytest.fail("invalid speed connected")


async def test_callback_failure_survives_expired_stop_write(monkeypatch):
    completed = asyncio.Event()

    async def review(request):
        completed.set()
        raise ValueError("policy failed")

    async with configured(
        monkeypatch, review=r.Review(callback=review, timeout_ms=100, on_timeout=r.Speak())
    ) as (current, peer):
        async with current._write_lock:
            requested(peer)
            await completed.wait()
            await asyncio.sleep(0.15)
        with pytest.raises(RimeStreamError, match="Speech review callback failed"):
            await asyncio.wait_for(anext(current.events), 0.3)
        assert peer.sent.empty(), "An expired Stop must not be submitted"


@pytest.mark.parametrize("decision", [r.Stop(), r.Replace("replacement")])
async def test_expired_verdict_does_not_cancel_later_timeout_speak_chunk(monkeypatch, decision):
    completed = asyncio.Event()
    seen = []

    async def review(request):
        seen.append(request.index)
        completed.set()
        return decision if request.index == 0 else r.Allow()

    async with configured(
        monkeypatch, review=r.Review(callback=review, timeout_ms=100, on_timeout=r.Speak())
    ) as (current, peer):
        async with current._write_lock:
            requested(peer, index=0)
            await completed.wait()
            await asyncio.sleep(0.15)
        # Wait for the abandoned write without supplying any server completion.
        pending = asyncio.create_task(current.update(speed=1.1))
        update = await peer.next("session.update")
        peer.emit(
            "session.updated",
            prism_request_event_id=update["event_id"],
            session={"id": "session-1"},
        )
        await pending
        requested(peer, index=1)
        result = await peer.next("prism.review.result")
        assert result["review_id"] == "review-guess-1"
        assert result["action"] == "allow"
        assert seen == [0, 1]


@pytest.mark.parametrize(
    "options",
    [
        {"interrupt_min_words": True},
        {"interrupt_min_words": 0},
        {"interrupt_min_words": 11},
        {"interrupt_phrases": ["stop"] * 101},
        {"acknowledgment_phrases": [None]},
        {"end_of_turn_min_silence_ms": -1},
        {"end_of_turn_min_silence_ms": 5001},
    ],
)
def test_invalid_turn_settings(options):
    with pytest.raises(RimeInputError):
        r.TurnDetection(**options)


async def test_response_end_cancels_pending_review(monkeypatch):
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def review(request):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with configured(monkeypatch, review=r.Review(callback=review)) as (current, peer):
        peer.emit("prism.typed_input.ready")
        pending = asyncio.create_task(current.send_text("Hello"))
        request = await peer.next("response.create")
        peer.accepted(request, response_id="active")
        await pending
        requested(peer, response="active")
        await started.wait()
        peer.ended(response_id="active")
        await cancelled.wait()
        assert peer.sent.empty()
