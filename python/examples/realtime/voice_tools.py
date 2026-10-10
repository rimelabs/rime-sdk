"""Say 'Please check order demo-123', then interrupt or ask another question."""

import json

from realtime.tools import LOOKUP_ORDER
from realtime.voice import cli


async def lookup_order(call):
    # Replace this local fixture with an async request to your order service.
    order_id = call.arguments.get("order_id")
    if call.name != "lookup_order":
        result = {"error": "Unknown tool"}
    elif not isinstance(order_id, str) or set(call.arguments) != {"order_id"}:
        result = {"error": "Provide only a string order_id"}
    elif order_id == "demo-123":
        result = {"order_id": order_id, "status": "shipped", "arrival": "Thursday"}
    else:
        result = {"error": "Order not found in demo data"}
    print(f"Tool {call.name}: {result}", flush=True)
    return json.dumps(result)


if __name__ == "__main__":
    cli(tools=[LOOKUP_ORDER], execute_tool=lookup_order)
