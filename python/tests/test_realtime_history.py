"""History operations use acknowledged requests without turn admission."""

import asyncio

import pytest
from test_realtime import accepted_turn, session

from rimelabs_sdk import RimeInputError, RimeStreamError
from rimelabs_sdk import realtime as r

ITEMS = [
    (
        r.HistoryMessage(id="note", role="system", text=" Transferred. "),
        {"type": "message", "id": "note", "role": "system", "text": " Transferred. "},
    ),
    (
        r.HistoryFunctionCall(id="past", call_id="old-call", name="lookup", arguments=' {"n": 1} '),
        {
            "type": "function_call",
            "id": "past",
            "call_id": "old-call",
            "name": "lookup",
            "arguments": ' {"n": 1} ',
        },
    ),
    (
        r.HistoryFunctionResult(id="result", call_id="old-call", output=" raw output "),
        {
            "type": "function_call_output",
            "id": "result",
            "call_id": "old-call",
            "output": " raw output ",
        },
    ),
]


async def test_history_create_read_delete_and_matching(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        for item, wire in ITEMS:
            anchor = None if isinstance(item, r.HistoryFunctionResult) else "root"
            task = asyncio.create_task(current.create_item(item, previous_item_id=anchor))
            q = await peer.next("conversation.item.create")
            assert q["item"] == wire
            assert q.get("previous_item_id") == anchor
            peer.emit(
                "conversation.item.created",
                prism_request_event_id=q["event_id"],
                item={k: v for k, v in wire.items() if k in ("type", "id", "role", "call_id")},
            )
            assert (await task).item_id == item.id
        first = asyncio.create_task(current.get_history())
        q1 = await peer.next("prism.conversation.get")
        second = asyncio.create_task(current.get_history())
        q2 = await peer.next("prism.conversation.get")
        peer.emit("prism.conversation", prism_request_event_id="unrelated", items=[])
        peer.emit("prism.conversation", prism_request_event_id=q2["event_id"], items=[])
        assert await second == ()
        peer.emit(
            "prism.conversation",
            prism_request_event_id=q1["event_id"],
            items=[wire for _, wire in ITEMS],
        )
        assert await first == tuple(item for item, _ in ITEMS)
        for _ in range(2):
            task = asyncio.create_task(current.delete_item("past"))
            q = await peer.next("conversation.item.delete")
            peer.emit(
                "conversation.item.deleted", prism_request_event_id=q["event_id"], item_id="past"
            )
            await task
        task = asyncio.create_task(current.delete_item("missing"))
        q = await peer.next("conversation.item.delete")
        peer.emit(
            "error",
            error={
                "code": "item_not_found",
                "message": "missing",
                "scope": "event",
                "owner": {"event_id": q["event_id"]},
            },
        )
        with pytest.raises(r.RimeRealtimeError):
            await task
        assert not peer.closed


async def test_cancelled_history_result_still_unblocks_tool_continuation(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            item_id="tool-item",
            call_id="call-1",
            name="lookup",
            arguments="{}",
        )
        peer.ended(parent.response_id)
        task = asyncio.create_task(
            current.create_item(r.HistoryFunctionResult(call_id="call-1", output="result"))
        )
        q = await peer.next("conversation.item.create")
        task.cancel()
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=q["event_id"],
            item={"type": "function_call_output", "id": "result-item", "call_id": "call-1"},
        )
        with pytest.raises(asyncio.CancelledError):
            await task
        task = asyncio.create_task(current.continue_reply(parent))
        q = await peer.next("response.create")
        peer.accepted(q, "reply-2")
        await task


@pytest.mark.parametrize(
    "item,anchor",
    [
        (r.HistoryMessage(role="system", text="x", id=""), None),
        (r.HistoryMessage(role="invalid", text="x"), None),
        (r.HistoryMessage(role="user", text="x" * 4001), None),
        (r.HistoryMessage(role="user", text=chr(0xD800)), None),
        (r.HistoryFunctionCall(call_id="", name="f"), None),
        (r.HistoryFunctionResult(call_id="c", output="x"), "root"),
    ],
)
async def test_invalid_history_never_sends(monkeypatch, item, anchor):
    async with session(monkeypatch) as (current, peer, _):
        with pytest.raises(RimeInputError):
            await current.create_item(item, previous_item_id=anchor)
        assert peer.sent.empty()


async def test_delete_acknowledgment_must_name_requested_item(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        task = asyncio.create_task(current.delete_item("right"))
        q = await peer.next("conversation.item.delete")
        peer.emit(
            "conversation.item.deleted", prism_request_event_id=q["event_id"], item_id="wrong"
        )
        with pytest.raises(RimeStreamError):
            await task


async def test_cancelled_creation_still_validates_acknowledgment(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        task = asyncio.create_task(
            current.create_item(r.HistoryMessage(id="right", role="system", text="note"))
        )
        q = await peer.next("conversation.item.create")
        task.cancel()
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=q["event_id"],
            item={"type": "message", "id": "wrong"},
        )
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(RimeStreamError):
            await current.get_history()


async def test_reused_call_id_does_not_change_a_recorded_response(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            item_id="old-call-item",
            call_id="shared",
            name="lookup",
            arguments="{}",
        )
        peer.ended(parent.response_id)
        old_call = r.ToolCallRef(
            session_id=current.info.session_id, response_id=parent.response_id, call_id="shared"
        )
        task = asyncio.create_task(current.submit_tool_result(old_call, "original"))
        q = await peer.next("conversation.item.create")
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=q["event_id"],
            item={"type": "function_call_output", "call_id": "shared"},
        )
        await task
        task = asyncio.create_task(current.delete_item("old-call-item"))
        q = await peer.next("conversation.item.delete")
        peer.emit(
            "conversation.item.deleted",
            prism_request_event_id=q["event_id"],
            item_id="old-call-item",
        )
        await task
        for item in (
            r.HistoryFunctionCall(id="imported-call", call_id="shared", name="lookup"),
            r.HistoryFunctionResult(id="imported-result", call_id="shared", output="imported"),
        ):
            task = asyncio.create_task(current.create_item(item))
            q = await peer.next("conversation.item.create")
            peer.emit(
                "conversation.item.created",
                prism_request_event_id=q["event_id"],
                item={"type": q["item"]["type"], "id": item.id, "call_id": "shared"},
            )
            await task
        await current.submit_tool_result(old_call, "original")
        assert peer.sent.empty()
