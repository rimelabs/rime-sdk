"""Shared decode cases and request/result matching through the public session."""

import asyncio
import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator
from test_realtime import _CONTRACT, _VALIDATORS, accepted_turn, session

from rimelabs_sdk import RimeInputError, RimeStreamError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _protocol as protocol

CASES = json.loads((Path(__file__).parents[2] / "conformance/prism/protocol.json").read_text())
SCHEMA_CASES = json.loads((Path(__file__).parents[2] / "conformance/prism/schema.json").read_text())


@pytest.mark.parametrize("case", SCHEMA_CASES, ids=lambda case: case["name"])
def test_schema_matches_protocol_contract(case):
    validator = Draft7Validator(
        {"$ref": "#/components/schemas/" + case["schema"], "components": _CONTRACT["components"]}
    )
    assert validator.is_valid(case["value"]) == case["valid"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_protocol_decoding(case):
    raw = json.dumps(case["event"])
    if case.get("invalid"):
        with pytest.raises(RimeStreamError):
            protocol.decode(raw, "session-1")
    else:
        result = protocol.decode(raw, "session-1")
        assert (result.payload.kind if result else "ignored") == case["expected_kind"]
        if "expected_truncation" in case:
            expected = case["expected_truncation"]
            assert result.payload.truncation == (
                None if expected is None else r.ResponseTruncation(**expected)
            )
        if "expected_usage" in case:
            expected = case["expected_usage"]
            assert isinstance(result.payload, r.ResponseEnded)
            assert result.payload.usage == (
                None if expected is None else r.ResponseUsage(**expected)
            )
        if "expected_instruction_item_id" in case:
            assert isinstance(result.payload, r.ResponseStarted)
            assert result.payload.instruction_item_id == case["expected_instruction_item_id"]
        if "expected_idle_timeout_s" in case:
            assert isinstance(result.payload, protocol.SessionIdleTimeout)
            assert result.payload.idle_timeout_s == case["expected_idle_timeout_s"]
        if "expected_kept_text" in case:
            assert isinstance(result.payload, r.ItemTruncated)
            assert result.payload.kept_text == case["expected_kept_text"]
            assert result.payload.item_id == case["event"]["item_id"]
            assert result.payload.content_index == case["event"]["content_index"]
            assert result.payload.audio_end_ms == case["event"]["audio_end_ms"]


def test_large_idle_timeout_is_a_protocol_error():
    event = {"type": "session.idle_timeout", "reason": "idle_timeout", "idle_timeout_s": 10**400}
    with pytest.raises(RimeStreamError):
        protocol.decode(json.dumps(event), "session-1")


@pytest.mark.parametrize(
    "case", [case for case in CASES if case.get("invalid")], ids=lambda case: case["name"]
)
async def test_invalid_payload_fails_pending_request_and_event_consumer(monkeypatch, case):
    async with session(monkeypatch) as (current, peer, _):
        await accepted_turn(current, peer)
        pending = asyncio.create_task(current.add_message("user", "history"))
        await peer.next("conversation.item.create")
        peer.incoming.put_nowait(json.dumps(case["event"]))
        with pytest.raises(RimeStreamError):
            await pending
        with pytest.raises(RimeStreamError):
            await anext(current.events)
        assert peer.sent.empty()


async def test_nudge_instruction_and_usage_reach_public_events(monkeypatch):
    async with asyncio.timeout(2), session(monkeypatch) as (current, peer, _):
        peer.emit("prism.typed_input.ready")
        pending = asyncio.create_task(current.request_reply(instruction="Check in."))
        request = await peer.next("response.create")
        peer.accepted(request, prism_instruction_item_id="item-instruction")
        response = await pending
        events = current.events
        while True:
            started = (await anext(events)).payload
            if isinstance(started, r.ResponseStarted):
                break
        assert started.response == response
        assert started.instruction_item_id == "item-instruction"
        peer.emit(
            "response.done",
            response={
                "id": response.response_id,
                "status": "completed",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 3,
                    "total_tokens": 13,
                    "context_window": 65536,
                },
            },
        )
        ended = (await anext(events)).payload
        assert isinstance(ended, r.ResponseEnded)
        assert ended.usage == r.ResponseUsage(
            input_tokens=10, output_tokens=3, total_tokens=13, context_window=65536
        )
        deleting = asyncio.create_task(current.delete_item(started.instruction_item_id))
        request = await peer.next("conversation.item.delete")
        assert request["item_id"] == "item-instruction"
        peer.emit(
            "conversation.item.deleted",
            prism_request_event_id=request["event_id"],
            item_id="item-instruction",
        )
        await deleting


@pytest.mark.parametrize("kind", ["text", "nudge"])
async def test_request_metadata_labels_only_one_reply(monkeypatch, kind):
    async with asyncio.timeout(2), session(monkeypatch) as (current, peer, _):
        peer.emit("prism.typed_input.ready")
        labels = {"step": "one"}
        if kind == "text":
            pending = asyncio.create_task(current.send_text("hello", metadata=labels))
            cause = "user_text"
        else:
            pending = asyncio.create_task(current.request_reply(metadata=labels))
            cause = "proactive"
        request = await peer.next("response.create")
        assert request["response"]["metadata"] == {"step": "one", "prism_cause": cause}
        assert labels == {"step": "one"}
        peer.accepted(request)
        await pending
        assert "prism_response_metadata" not in peer.settings
        peer.ended()
        peer.emit("prism.typed_input.ready")
        if kind == "text":
            next_pending = asyncio.create_task(current.send_text("again"))
        else:
            next_pending = asyncio.create_task(current.request_reply())
        next_request = await peer.next("response.create")
        assert next_request["response"]["metadata"] == {"prism_cause": cause}
        peer.accepted(next_request, response_id="reply-2")
        await next_pending


async def test_reserved_request_metadata_is_rejected_before_write(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        with pytest.raises(RimeInputError, match="prism_"):
            await current.request_reply(metadata={"prism_cause": "user_text"})
        assert peer.sent.empty()


@pytest.mark.parametrize(
    ("operation", "acknowledgment"),
    [
        ("text", "session.updated"),
        ("proactive", "conversation.item.created"),
        ("history", "session.updated"),
        ("history", "response.created"),
        ("clear", "session.updated"),
        ("tool", "session.updated"),
        ("tool", "conversation.item.created"),
        ("tool", "wrong-call"),
    ],
)
async def test_mismatched_acknowledgment_closes_without_returning_wrong_result(
    monkeypatch, operation, acknowledgment
):
    async with session(monkeypatch) as (current, peer, _):
        if operation in ("text", "proactive"):
            peer.emit("prism.typed_input.ready")
            pending = asyncio.create_task(
                current.send_text("hello") if operation == "text" else current.request_reply()
            )
            request = await peer.next("response.create")
        elif operation == "tool":
            await accepted_turn(current, peer)
            peer.emit(
                "response.function_call_arguments.done",
                response_id="reply-1",
                item_id="tool-1",
                call_id="call-1",
                name="lookup",
                arguments="{}",
            )
            async for event in current.events:
                if isinstance(event.payload, r.ToolCall):
                    call = event.payload.call
                    break
            pending = asyncio.create_task(current.submit_tool_result(call, "done"))
            request = await peer.next("conversation.item.create")
        elif operation == "clear":
            pending = asyncio.create_task(current.clear_audio())
            request = await peer.next("input_audio_buffer.clear")
        else:
            pending = asyncio.create_task(current.add_message("user", "history"))
            request = await peer.next("conversation.item.create")
        if acknowledgment == "session.updated":
            peer.emit(
                acknowledgment,
                prism_request_event_id=request["event_id"],
                session={"id": "session-1"},
            )
        elif acknowledgment == "response.created":
            peer.accepted(request, "wrong-response")
        elif acknowledgment == "wrong-call":
            peer.emit(
                "conversation.item.created",
                prism_request_event_id=request["event_id"],
                item={"type": "function_call_output", "call_id": "another-call"},
            )
        else:
            peer.emit(
                acknowledgment,
                prism_request_event_id=request["event_id"],
                item={"id": "history-1", "type": "message", "role": "user"},
            )
        with pytest.raises(RimeStreamError):
            await pending
        with pytest.raises(RimeStreamError):
            await anext(current.events)
        assert peer.sent.empty()


async def test_tool_result_acknowledgment_needs_no_item_id(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        parent = await accepted_turn(current, peer)
        peer.emit(
            "response.function_call_arguments.done",
            response_id=parent.response_id,
            item_id="tool-1",
            call_id="call-1",
            name="lookup",
            arguments="{}",
        )
        async for event in current.events:
            if isinstance(event.payload, r.ToolCall):
                call = event.payload.call
                break
        pending = asyncio.create_task(current.submit_tool_result(call, "done"))
        request = await peer.next("conversation.item.create")
        acknowledgment = {
            "type": "conversation.item.created",
            "event_id": "server-ack",
            "prism_request_event_id": request["event_id"],
            "item": {"type": "function_call_output", "call_id": call.call_id},
        }
        _VALIDATORS[acknowledgment["type"]].validate(acknowledgment)
        peer.incoming.put_nowait(json.dumps(acknowledgment))
        await pending
        peer.ended()
        continuation = asyncio.create_task(
            current.continue_reply(parent, metadata={"step": "tool"})
        )
        request = await peer.next("response.create")
        assert request["response"]["metadata"] == {
            "step": "tool",
            "prism_cause": "tool_continuation",
            "prism_parent_response_id": parent.response_id,
        }
        peer.accepted(request, "reply-2")
        assert (await continuation).response_id == "reply-2"


async def test_unknown_events_and_stale_acknowledgments_do_not_consume_request(monkeypatch):
    async with session(monkeypatch) as (current, peer, _):
        pending = asyncio.create_task(current.add_message("user", "history"))
        request = await peer.next("conversation.item.create")
        peer.emit(
            "future.event", prism_request_event_id=request["event_id"], unknown={"anything": True}
        )
        peer.emit(
            "session.updated", prism_request_event_id="old-request", session={"id": "session-1"}
        )
        await asyncio.sleep(0)
        assert not pending.done()
        peer.emit(
            "conversation.item.created",
            prism_request_event_id=request["event_id"],
            item={"id": "history-1"},
            future={"ignored": True},
        )
        assert (await pending).item_id == "history-1"


def test_a_started_reply_names_the_user_items_it_answers():
    def started(metadata, **response):
        response = {"id": "reply-1", "metadata": metadata, **response}
        event = {"type": "response.created", "response": response}
        return protocol.decode(json.dumps(event), "session-1").payload

    assert started(None, prism_input_item_ids=["item-1", "item-2"]).input_item_ids == (
        "item-1",
        "item-2",
    )
    assert started(
        {"prism_cause": "user_text", "prism_input_item_id": "item-3"}
    ).input_item_ids == ("item-3",)
    assert started(None).input_item_ids == ()


def test_a_started_reply_shows_the_clients_own_metadata():
    def started(metadata):
        event = {"type": "response.created", "response": {"id": "reply-1", "metadata": metadata}}
        return protocol.decode(json.dumps(event), "session-1").payload

    assert started({"prism_cause": "user_text", "step": "3", "nested": {"a": 1}}).metadata == {
        "step": "3"
    }
    assert started(None).metadata == {}
