"""Acknowledged partial session updates through the public interface."""

import asyncio

import pytest
from test_realtime import accepted_turn, session

from rimelabs_sdk import RimeInputError
from rimelabs_sdk import realtime as r


def acknowledge(peer, request, revision, voice="astra", interrupt=False):
    peer.emit(
        "session.updated",
        prism_request_event_id=request["event_id"],
        session={
            "id": "session-1",
            "voice": voice,
            "request_id": "trace-update",
            "prism_response_metadata": {"step": "billing"},
            "prism_effective_interrupt_response": interrupt,
            "prism_settings_revision": revision,
        },
    )


async def test_partial_update_and_clear_without_turn_readiness(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        original = current.info
        parameters = {"type": "object"}
        pending = asyncio.create_task(
            current.update(
                instructions="Handle billing.",
                tools=[
                    r.ToolDefinition(
                        name="refund",
                        parameters=parameters,
                        prism_filler=r.PrismFiller(type="none"),
                    )
                ],
            )
        )
        request = await peer.next("session.update")
        parameters["type"] = "string"
        assert request["session"] == {
            "instructions": "Handle billing.",
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "refund",
                        "description": "",
                        "parameters": {"type": "object"},
                    },
                    "prism_filler": {"type": "none"},
                }
            ],
        }
        assert current.info is original
        acknowledge(peer, request, 2)
        result = await pending
        assert result == current.info
        assert result.settings_revision == 2
        assert result.voice == "astra"
        assert result.request_id == "trace-update"
        assert result.response_metadata == {"step": "billing"}
        assert not result.interrupt_on_speech
        assert original.voice == "test"

        pending = asyncio.create_task(
            current.update(instructions="", tools=[], voice="", interrupt_on_speech=False)
        )
        request = await peer.next("session.update")
        assert request["session"] == {
            "instructions": "",
            "tools": [],
            "voice": "",
            "turn_detection": {"interrupt_response": False},
        }
        acknowledge(peer, request, 3)
        assert (await pending).settings_revision == 3

        for options in (
            {"tools": [r.ToolDefinition(name="same", parameters={})] * 2},
            {"tools": [r.ToolDefinition(name="bad", parameters={"n": float("nan")})]},
            {"instructions": False},
            {"interrupt_on_speech": 1},
        ):
            with pytest.raises(RimeInputError):
                await current.update(**options)
        assert peer.sent.empty()
        peer.emit("response.created", response={"id": "early-guess", "prism_settings_revision": 2})
        events = current.events
        assert (await anext(events)).payload.settings_revision == 2
        assert current.info.settings_revision == 3
        await events.aclose()


async def test_rejected_and_concurrent_updates_keep_the_connection(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        original = current.info
        pending = asyncio.create_task(current.update(voice="missing"))
        request = await peer.next("session.update")
        peer.emit(
            "error",
            error={
                "code": "invalid_voice",
                "message": "Unknown voice",
                "scope": "event",
                "owner": {"event_id": request["event_id"]},
            },
        )
        with pytest.raises(r.RimeRealtimeError):
            await pending
        assert current.info is original
        assert not peer.closed
        first = asyncio.create_task(current.update(instructions="First"))
        q1 = await peer.next("session.update")
        second = asyncio.create_task(current.update(tools=[]))
        q2 = await peer.next("session.update")
        acknowledge(peer, {"event_id": "unrelated"}, 99, voice="wrong")
        acknowledge(peer, q1, 2)
        assert (await first).settings_revision == 2
        assert not second.done()
        acknowledge(peer, q2, 3)
        assert (await second).settings_revision == 3
        assert current.info.settings_revision == 3


async def test_cancelled_update_keeps_late_acknowledgment(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        pending = asyncio.create_task(current.update(instructions="Billing"))
        request = await peer.next("session.update")
        pending.cancel()
        acknowledge(peer, request, 2)
        with pytest.raises(asyncio.CancelledError):
            await pending
        # A following acknowledged request provides a barrier after the update.
        message = asyncio.create_task(current.add_message("user", "history"))
        q = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=q["event_id"],
            item={"type": "message", "id": "history"},
        )
        await message
        assert current.info.settings_revision == 2
        assert not peer.closed


async def test_update_preserves_tool_round_and_response_revisions(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        events = current.events
        parent = await accepted_turn(current, peer)
        await anext(events)
        await anext(events)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            item_id="tool-1",
            call_id="call-1",
            name="lookup",
            arguments="{}",
        )
        call = (await anext(events)).payload.call
        update = asyncio.create_task(current.update(instructions="Billing", tools=[]))
        acknowledge(peer, await peer.next("session.update"), 2)
        await update
        peer.emit(
            "response.done",
            response={
                "id": parent.response_id,
                "status": "completed",
                "prism_settings_revision": 1,
            },
        )
        ended = (await anext(events)).payload
        assert ended.settings_revision == 1
        assert current.info.settings_revision == 2
        result = asyncio.create_task(current.submit_tool_result(call, "transferred"))
        q = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=q["event_id"],
            item={"type": "function_call_output", "call_id": call.call_id},
        )
        await result
        continuation = asyncio.create_task(current.continue_reply(parent))
        q = await peer.next("response.create")
        peer.emit(
            "response.created",
            prism_request_event_id=q["event_id"],
            response={
                "id": "reply-2",
                "prism_settings_revision": 2,
                "metadata": q["response"]["metadata"],
            },
        )
        await continuation
        assert (await anext(events)).payload.settings_revision == 2


async def test_reply_label_update_refreshes_settings_revision(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        pending = asyncio.create_task(current.set_response_metadata({"step": "billing"}))
        request = await peer.next("session.update")
        acknowledge(peer, request, 2)
        await pending
        assert current.info.settings_revision == 2
        assert current.info.request_id == "trace-update"
        assert current.info.response_metadata == {"step": "billing"}
        assert not current.info.interrupt_on_speech
