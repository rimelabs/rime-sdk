"""Client-ended turns: the client starts and ends each user turn and asks for the reply."""

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from test_realtime import _VALIDATORS, Peer

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


class ClientTurnsPeer(Peer):
    """Echoes turn_detection null on the first update, as Prism does."""

    async def send(self, raw):
        event = json.loads(raw)
        _VALIDATORS[event["type"]].validate(event)
        if event["type"] == "session.update" and self.settings is None:
            self.settings = event["session"]
            ends_turns = event["session"].get("turn_detection", {}) is None
            self.emit(
                "session.updated",
                prism_request_event_id=event["event_id"],
                session={
                    "id": "session-1",
                    "voice": "test",
                    "turn_detection": None if ends_turns else {"eagerness": "auto"},
                    "prism_effective_interrupt_response": not ends_turns,
                },
            )
            self.emit("prism.typed_input.ready")
        else:
            await self.sent.put(event)


@asynccontextmanager
async def connected(monkeypatch, **options):
    peer = ClientTurnsPeer()

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        asyncio.timeout(3),
        Rime(api_key="test") as client,
        client.realtime.connect(
            timeouts=r.RealtimeTimeouts(request_s=0.5, ready_s=1), **options
        ) as current,
    ):
        yield current, peer


def committed(peer, item_id="turn-1"):
    peer.emit("input_audio_buffer.committed", item_id=item_id)


def refused(peer, code, request=None):
    """An error Prism sends; with request, correlated to it."""
    owner = {"kind": "event", "event_id": request["event_id"]} if request else {"kind": "event"}
    peer.emit(
        "error",
        error={
            "type": "invalid_request_error",
            "code": code,
            "message": code,
            "event_id": request["event_id"] if request else None,
            "scope": "event",
            "owner": owner,
        },
    )


def speech(frames=320):
    return r.AudioChunk(data=b"\x00\x00" * frames, format=r.PCMFormat(sample_rate=16000))


async def test_client_ended_turns_send_null_turn_detection(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        assert peer.settings["turn_detection"] is None
        assert current.info.client_ends_turns is True
        assert current.info.interrupt_on_speech is False


async def test_prism_ends_turns_by_default(monkeypatch):
    async with connected(monkeypatch) as (current, peer):
        assert peer.settings["turn_detection"] == {"interrupt_response": True}
        assert current.info.client_ends_turns is False


async def test_push_to_talk_round_trip(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        await current.start_turn()
        start = await peer.next("prism.input_audio_buffer.start")
        assert set(start) == {"type", "event_id"}
        await current.send_audio(speech())
        await peer.next("input_audio_buffer.append")
        ending = asyncio.create_task(current.end_turn())
        commit = await peer.next("input_audio_buffer.commit")
        assert set(commit) == {"type", "event_id"}
        committed(peer)
        await ending

        pending = asyncio.create_task(current.reply_to_turns())
        request = await peer.next("response.create")
        assert request["response"] == {}
        peer.accepted(request)
        reply = await pending
        assert reply.response_id == "reply-1"


async def test_reply_to_turns_carries_labels_without_a_cause(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        pending = asyncio.create_task(current.reply_to_turns(metadata={"order": "42"}))
        request = await peer.next("response.create")
        assert request["response"] == {"metadata": {"order": "42"}}
        peer.accepted(request)
        await pending


async def test_reply_to_turns_waits_for_the_active_reply(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        pending = asyncio.create_task(current.reply_to_turns())
        first = await peer.next("response.create")
        peer.emit(
            "error",
            error={
                "type": "invalid_request_error",
                "code": "conversation_already_has_active_response",
                "message": "a reply is running",
                "event_id": first["event_id"],
                "scope": "event",
                "owner": {"kind": "event", "event_id": first["event_id"]},
            },
        )
        await asyncio.sleep(0.05)
        assert not pending.done()
        peer.emit("prism.typed_input.ready")
        second = await peer.next("response.create")
        assert second["event_id"] != first["event_id"]
        peer.accepted(second)
        await pending


async def test_end_turn_waits_until_prism_commits_the_turn(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        ending = asyncio.create_task(current.end_turn())
        await peer.next("input_audio_buffer.commit")
        await asyncio.sleep(0.05)
        assert not ending.done()
        committed(peer)
        assert await ending is None


@pytest.mark.parametrize("correlated", [True, False])
async def test_end_turn_raises_when_prism_refuses_the_commit(monkeypatch, correlated):
    # A press under 100 ms is refused with an error naming the commit. Speech
    # over the pending audio budget is refused with an error naming nothing.
    code = "input_audio_buffer_commit_empty" if correlated else "pending_speech_overflow"
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        events = current.events
        ending = asyncio.create_task(current.end_turn())
        commit = await peer.next("input_audio_buffer.commit")
        refused(peer, code, commit if correlated else None)
        with pytest.raises(r.RimeRealtimeError) as raised:
            await ending
        assert raised.value.fault.code == code
        event = await anext(events)
        while not isinstance(event.payload, r.FaultEvent):
            event = await anext(events)
        assert event.payload.error.code == code
        # The session stays usable.
        ending = asyncio.create_task(current.end_turn())
        await peer.next("input_audio_buffer.commit")
        committed(peer)
        await ending


async def test_end_turn_also_ends_a_turn_prism_detects(monkeypatch):
    async with connected(monkeypatch) as (current, peer):
        await current.end_turn()
        await peer.next("input_audio_buffer.commit")


async def test_client_commit_end_reason_is_known(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        events = current.events
        peer.emit(
            "input_audio_buffer.speech_stopped",
            item_id="speech",
            audio_end_ms=1200,
            prism_end_reason="client_commit",
        )
        event = await anext(events)
        while not isinstance(event.payload, r.SpeechStopped):
            event = await anext(events)
        assert event.payload.end_reason == "client_commit"


async def test_turn_starts_and_replies_need_client_ended_turns(monkeypatch):
    async with connected(monkeypatch) as (current, peer):
        with pytest.raises(RimeInputError, match="client_ends_turns"):
            await current.start_turn()
        with pytest.raises(RimeInputError, match="client_ends_turns"):
            await current.reply_to_turns()
        assert peer.sent.empty()


@pytest.mark.parametrize(
    "options",
    [
        {"turn_detection": r.TurnDetection(eagerness="low")},
        {"interrupt_on_speech": False},
    ],
)
async def test_client_ended_turns_refuse_turn_settings(monkeypatch, options):
    with pytest.raises(RimeInputError, match="client_ends_turns"):
        async with connected(monkeypatch, client_ends_turns=True, **options):
            pass


async def test_client_ended_turns_refuse_later_turn_settings(monkeypatch):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        with pytest.raises(RimeInputError, match="client_ends_turns"):
            await current.update(interrupt_on_speech=True)
        with pytest.raises(RimeInputError, match="client_ends_turns"):
            await current.update(turn_detection=r.TurnDetectionUpdate(interrupt_min_words=2))
        assert peer.sent.empty()


async def test_reply_to_turns_waits_for_readiness_after_the_turn(monkeypatch):
    """Prism's own order: the turn's start clears readiness, and its end restores it."""
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        events = current.events
        await current.start_turn()
        await peer.next("prism.input_audio_buffer.start")
        peer.emit("input_audio_buffer.speech_started", item_id="turn-1", audio_start_ms=0)
        await current.send_audio(speech())
        await peer.next("input_audio_buffer.append")
        ending = asyncio.create_task(current.end_turn())
        await peer.next("input_audio_buffer.commit")
        peer.emit(
            "input_audio_buffer.speech_stopped",
            item_id="turn-1",
            audio_end_ms=40,
            prism_end_reason="client_commit",
        )
        committed(peer)
        await ending
        while not isinstance((await anext(events)).payload, r.InputCommitted):
            pass

        pending = asyncio.create_task(current.reply_to_turns())
        await asyncio.sleep(0.05)
        assert peer.sent.empty(), "the reply request went out before Prism was ready"
        peer.emit("prism.typed_input.ready")
        request = await peer.next("response.create")
        assert request["response"] == {}
        peer.accepted(request)
        assert (await pending).response_id == "reply-1"


@pytest.mark.parametrize(
    ("operation", "kind"),
    [("start_turn", "prism.input_audio_buffer.start"), ("end_turn", "input_audio_buffer.commit")],
)
async def test_turn_controls_follow_audio_sent_before_them(monkeypatch, operation, kind):
    async with connected(monkeypatch, client_ends_turns=True) as (current, peer):
        # A clear waiting for its acknowledgment holds back the audio sent after it.
        clearing = asyncio.create_task(current.clear_audio())
        await peer.next("input_audio_buffer.clear")
        sending = asyncio.create_task(current.send_audio(speech(frames=1920)))
        await asyncio.sleep(0.01)
        control = asyncio.create_task(getattr(current, operation)())
        await asyncio.sleep(0.05)
        assert peer.sent.empty(), "the turn control overtook audio sent before it"
        peer.emit("input_audio_buffer.cleared")
        for _ in range(3):  # 120 ms of audio is three 40 ms appends.
            await peer.next("input_audio_buffer.append")
        await peer.next(kind)
        if operation == "end_turn":
            committed(peer)
        await asyncio.gather(clearing, sending, control)
