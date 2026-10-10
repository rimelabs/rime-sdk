"""Run: uv run --project python python/examples/realtime/tools.py"""

import asyncio
import json
import os

from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import (
    FaultEvent,
    RealtimeSession,
    ResponseEnded,
    RimeRealtimeError,
    TextDelta,
    ToolCall,
    ToolDefinition,
)

LOOKUP_ORDER = ToolDefinition(
    name="lookup_order",
    description="Look up an order in the example store.",
    parameters={
        "type": "object",
        "properties": {"order_id": {"type": "string"}},
        "required": ["order_id"],
        "additionalProperties": False,
    },
)


async def submit_result(session: RealtimeSession, tool: ToolCall) -> None:
    # This tool uses local fixture data. No external operation is performed.
    order_id = tool.arguments.get("order_id")
    if tool.name != "lookup_order":
        result = {"error": "Unknown tool"}
    elif not isinstance(order_id, str) or set(tool.arguments) != {"order_id"}:
        result = {"error": "Provide only a string order_id"}
    elif order_id == "demo-123":
        result = {"order_id": order_id, "status": "shipped"}
    else:
        result = {"error": "Order not found"}
    output = json.dumps(result)
    print(f"\nTool {tool.name}: {output}")
    await session.submit_tool_result(tool.call, output)


async def handle_events(session: RealtimeSession, tasks: asyncio.TaskGroup) -> None:
    tool_responses: set[str] = set()
    async for event in session.events:
        payload = event.payload
        if isinstance(payload, TextDelta):
            print(payload.delta, end="", flush=True)
        elif isinstance(payload, ToolCall):
            tool_responses.add(payload.call.response_id)
            # Keep consuming events while the result waits for acknowledgment.
            tasks.create_task(submit_result(session, payload))
        elif isinstance(payload, FaultEvent):
            raise RimeRealtimeError(payload.error)
        elif isinstance(payload, ResponseEnded):
            if payload.status != "completed":
                raise RuntimeError(f"Response {payload.status}: {payload.reason}")
            if payload.response.response_id in tool_responses:
                tool_responses.remove(payload.response.response_id)
                # The SDK also waits for every tool result acknowledgment.
                tasks.create_task(session.continue_reply(payload.response))
            else:
                print()
                return
    raise RuntimeError("Session closed before the response ended")


async def main() -> None:
    async with (
        asyncio.timeout(60),
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.getenv("PRISM_VOICE"),
            instructions="Use lookup_order for order questions. Keep the answer short.",
            tools=[LOOKUP_ORDER],
        ) as session,
        asyncio.TaskGroup() as tasks,
    ):
        tasks.create_task(handle_events(session, tasks))
        await session.send_text("Use lookup_order to check order demo-123.")


if __name__ == "__main__":
    asyncio.run(main())
