import asyncio
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from rimelabs_sdk import realtime as r


@pytest.mark.parametrize(
    ("stage", "cancel_order"),
    [
        ("before_call", None),
        ("before_end", None),
        ("working", None),
        ("continuing", None),
        ("continuing", "before_abandonment"),
        ("continuing", "after_abandonment"),
    ],
)
async def test_abandonment_prevents_or_cancels_tool_continuation(monkeypatch, stage, cancel_order):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "examples"))
    ManualTools = import_module("realtime.manual_tools").ManualTools
    started, release, continuing = asyncio.Event(), asyncio.Event(), asyncio.Event()
    executed, results, continued, cancelled = [], [], [], []
    finish_task = None

    async def execute(call):
        executed.append(call)
        started.set()
        await release.wait()
        return "{}"

    async def submit(call, output):
        results.append(output)

    async def continuation(response):
        continuing.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(response)
            raise
        continued.append(response)

    session = SimpleNamespace(submit_tool_result=submit, continue_reply=continuation)
    ref = r.ResponseRef(session_id="s", response_id="tools")
    async with asyncio.timeout(1), asyncio.TaskGroup() as tasks:
        handler = ManualTools(session, tasks, execute)

        def emit(payload):
            handler.on_event(SimpleNamespace(payload=payload))

        emit(r.ResponseStarted(response=ref, cause="text"))
        if stage == "before_call":
            emit(r.ResponseAbandoned(response=ref))
        emit(
            r.ToolCall(
                call=r.ToolCallRef(session_id="s", response_id="tools", call_id="c"),
                item_id="i",
                name="test",
                arguments={},
            )
        )
        if stage == "before_end":
            emit(r.ResponseAbandoned(response=ref))
        emit(r.ResponseEnded(response=ref, status="completed"))
        if stage in ("working", "continuing"):
            await started.wait()
            finish_task = next(iter(handler.pending))
            if stage == "continuing":
                release.set()
                await continuing.wait()
            if cancel_order == "before_abandonment":
                handler.cancel()
            emit(r.ResponseAbandoned(response=ref))
            if cancel_order == "after_abandonment":
                handler.cancel()
        release.set()
    assert not continued
    assert len(executed) == (stage in ("working", "continuing"))
    assert results == (["{}"] if stage in ("working", "continuing") else [])
    assert len(cancelled) == (stage == "continuing")
    if finish_task is not None:
        assert finish_task.cancelled() is (cancel_order is not None)
