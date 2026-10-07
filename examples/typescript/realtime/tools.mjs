import { Rime, RimeRealtimeError } from "@rimelabs/sdk";

const lookupOrder = {
  name: "lookup_order",
  description: "Look up an order in the example store.",
  parameters: {
    type: "object",
    properties: { order_id: { type: "string" } },
    required: ["order_id"],
    additionalProperties: false,
  },
};

async function submitResult(session, tool) {
  // This tool uses local fixture data. It performs no external operation.
  const orderId = tool.arguments.order_id;
  let result;
  if (tool.name !== "lookup_order") result = { error: "Unknown tool" };
  else if (
    typeof orderId !== "string" ||
    Object.keys(tool.arguments).length !== 1
  )
    result = { error: "Provide only a string order_id" };
  else if (orderId === "demo-123")
    result = { order_id: orderId, status: "shipped" };
  else result = { error: "Order not found" };
  const output = JSON.stringify(result);
  console.log(`\nTool ${tool.name}: ${output}`);
  await session.submitToolResult(tool.call, output);
}

async function handleEvents(session, start) {
  const toolResponses = new Set();
  for await (const { payload } of session.events) {
    switch (payload.kind) {
      case "text.delta":
        process.stdout.write(payload.delta);
        break;
      case "tool.call":
        toolResponses.add(payload.call.responseId);
        // Keep consuming events while each result waits for acknowledgment.
        start(submitResult(session, payload));
        break;
      case "error":
        throw new RimeRealtimeError(payload.error);
      case "response.ended":
        if (payload.status !== "completed")
          throw new Error(`Response ${payload.status}: ${payload.reason}`);
        if (toolResponses.delete(payload.response.responseId))
          start(session.continueReply(payload.response));
        else {
          console.log();
          return;
        }
    }
  }
  throw new Error("Session closed before the response ended");
}

const client = new Rime();
const deadline = setTimeout(() => void client.close(), 60_000);
const pending = new Set();
let fail;
const failure = new Promise((_, reject) => {
  fail = reject;
});
function start(task) {
  pending.add(task);
  task.then(
    () => pending.delete(task),
    (error) => {
      pending.delete(task);
      fail(error);
    },
  );
}
let reading;
try {
  const session = await client.realtime.connect({
    endpoint: process.env.PRISM_URL,
    voice: process.env.PRISM_VOICE,
    instructions:
      "Use lookup_order for order questions. Keep the answer short.",
    tools: [lookupOrder],
  });
  reading = handleEvents(session, start);
  await Promise.race([
    Promise.all([
      reading,
      session.sendText("Use lookup_order to check order demo-123."),
    ]),
    failure,
  ]);
  await Promise.all(pending);
} finally {
  clearTimeout(deadline);
  await client.close();
  await Promise.allSettled(pending);
  await reading?.catch(() => {});
}
