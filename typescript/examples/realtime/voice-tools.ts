import { type ToolCall, type ToolDefinition } from "@rimelabs/sdk";
import { voice } from "./conversation.js";

const lookupOrder: ToolDefinition = {
  name: "lookup_order",
  description: "Look up an order in the example store.",
  parameters: {
    type: "object",
    properties: { order_id: { type: "string" } },
    required: ["order_id"],
    additionalProperties: false,
  },
};

export async function lookup(call: ToolCall): Promise<string> {
  // Replace this local fixture with an async request to your order service.
  const id = call.arguments.order_id;
  let result;
  if (call.name !== "lookup_order") result = { error: "Unknown tool" };
  else if (typeof id !== "string" || Object.keys(call.arguments).length !== 1)
    result = { error: "Provide only a string order_id" };
  else if (id === "demo-123")
    result = { order_id: id, status: "shipped", arrival: "Thursday" };
  else result = { error: "Order not found in demo data" };
  console.log(`Tool ${call.name}:`, result);
  return JSON.stringify(result);
}

voice([lookupOrder], lookup);
