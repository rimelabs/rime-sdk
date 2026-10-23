"""Retry classification and error delivery through the realtime interface."""

import asyncio
import json
from pathlib import Path

import pytest
from test_realtime import _CONTRACT, accepted_turn, session

from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _protocol

CASES = json.loads((Path(__file__).parents[2] / "conformance/prism/error-retry.json").read_text())
OVERFLOW = (
    "the prompt is 35401 tokens; with 512 for the reply it is longer than the context of 32768"
)


def test_retry_cases_cover_the_schema_codes():
    codes = {entry["const"] for entry in _CONTRACT["components"]["schemas"]["errorCode"]["oneOf"]}
    tested = {case["code"] for case in CASES}
    assert codes <= tested
    assert len(tested) == len(CASES)
    assert all(not case["retryable"] for case in CASES if case["code"] not in codes)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["code"] or "empty code")
def test_retry_classification_comes_from_the_code(case):
    event = _protocol.decode(
        json.dumps(
            {"type": "error", "error": {"code": case["code"], "retryable": not case["retryable"]}}
        ),
        "session-1",
    )
    assert isinstance(event.payload, r.FaultEvent)
    fault = event.payload.error
    assert fault.code == case["code"]
    assert fault.retryable is case["retryable"]
    assert r.RimeRealtimeError(fault).fault.retryable is case["retryable"]


@pytest.mark.parametrize(
    "case",
    [
        case
        for case in CASES
        if case["retryable"] or case["code"] in {"context_length_exceeded", "future_error"}
    ],
    ids=lambda case: case["code"],
)
async def test_response_fault_preserves_details_without_replay(monkeypatch, case):
    async with asyncio.timeout(2), session(monkeypatch) as (current, peer, _):
        events = current.events
        response = await accepted_turn(current, peer)
        peer.emit(
            "error",
            error={
                "code": case["code"],
                "message": OVERFLOW
                if case["code"] == "context_length_exceeded"
                else "reply failed",
                "scope": "response",
                "owner": {"kind": "response", "response_id": response.response_id},
                "correlation_id": "diagnostic-1",
            },
        )
        async for event in events:
            if isinstance(event.payload, r.FaultEvent):
                fault = event.payload.error
                break
        assert fault.code == case["code"]
        assert fault.retryable is case["retryable"]
        assert fault.scope == "response"
        assert fault.response_id == response.response_id
        assert fault.correlation_id == "diagnostic-1"
        assert fault.message == (
            OVERFLOW if case["code"] == "context_length_exceeded" else "reply failed"
        )
        peer.ended(status="failed")
        assert isinstance((await anext(events)).payload, r.ResponseEnded)
        peer.emit("prism.typed_input.ready")
        assert isinstance((await anext(events)).payload, r.InputReady)
        await current.close()
        assert peer.sent.empty()


async def test_request_error_exposes_the_same_fault_as_the_event(monkeypatch):
    async with asyncio.timeout(2), session(monkeypatch) as (current, peer, _):
        peer.emit("prism.typed_input.ready")
        pending = asyncio.create_task(current.request_reply(instruction="Say hello"))
        request = await peer.next("response.create")
        peer.emit(
            "error",
            error={
                "code": "invalid_instruction",
                "message": "instruction refused",
                "scope": "event",
                "owner": {"kind": "event", "event_id": request["event_id"]},
                "param": "response.prism_instruction",
                "correlation_id": "diagnostic-1",
            },
        )
        with pytest.raises(r.RimeRealtimeError) as result:
            await pending
        async for event in current.events:
            if isinstance(event.payload, r.FaultEvent):
                fault = event.payload.error
                break
        assert result.value.fault is fault
        assert not fault.retryable
        assert fault.code == "invalid_instruction"
        assert fault.message == "instruction refused"
        assert fault.request_id == request["event_id"]
        assert fault.parameter == "response.prism_instruction"
        assert fault.correlation_id == "diagnostic-1"
        assert peer.sent.empty()
