import { test } from "node:test";
import assert from "node:assert/strict";
import {
  Rime,
  RimeInputError,
  RimeRealtimeError,
  RimeStreamError,
} from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";
async function setup(t) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  return { session, peer };
}
const items = [
  [
    { type: "message", id: "note", role: "system", text: " Transferred. " },
    { type: "message", id: "note", role: "system", text: " Transferred. " },
  ],
  [
    {
      type: "function_call",
      id: "past",
      callId: "old-call",
      name: "lookup",
      arguments: ' {"n": 1} ',
    },
    {
      type: "function_call",
      id: "past",
      call_id: "old-call",
      name: "lookup",
      arguments: ' {"n": 1} ',
    },
  ],
  [
    {
      type: "function_call_output",
      id: "result",
      callId: "old-call",
      output: " raw output ",
    },
    {
      type: "function_call_output",
      id: "result",
      call_id: "old-call",
      output: " raw output ",
    },
  ],
];
test("history create, read, delete, correlation and refusal", async (t) => {
  const { session, peer } = await setup(t);
  for (const [item, wire] of items) {
    const options =
      item.type === "function_call_output" ? {} : { previousItemId: "root" };
    const pending = session.createItem(item, options);
    const q = await peer.next("conversation.item.create");
    assert.deepEqual(q.item, wire);
    assert.equal(q.previous_item_id, options.previousItemId);
    peer.emit("conversation.item.created", {
      prism_request_event_id: q.event_id,
      item: wire,
    });
    assert.equal((await pending).itemId, item.id);
  }
  const first = session.getHistory();
  const q1 = await peer.next("prism.conversation.get");
  const second = session.getHistory();
  const q2 = await peer.next("prism.conversation.get");
  peer.emit("prism.conversation", {
    prism_request_event_id: q2.event_id,
    items: [],
  });
  assert.deepEqual(await second, []);
  peer.emit("prism.conversation", {
    prism_request_event_id: q1.event_id,
    items: items.map(([, wire]) => wire),
  });
  assert.deepEqual(
    await first,
    items.map(([item]) => item),
  );
  for (let i = 0; i < 2; i++) {
    const pending = session.deleteItem("past");
    const q = await peer.next("conversation.item.delete");
    peer.emit("conversation.item.deleted", {
      prism_request_event_id: q.event_id,
      item_id: "past",
    });
    await pending;
  }
  const pending = session.deleteItem("missing");
  const refused = assert.rejects(pending, RimeRealtimeError);
  peer.fault(await peer.next("conversation.item.delete"), "item_not_found");
  await refused;
  assert.deepEqual(peer.invalid, []);
});
test("wrong deletion acknowledgment fails session", async (t) => {
  const { session, peer } = await setup(t);
  const pending = session.deleteItem("right");
  const failed = assert.rejects(pending, RimeStreamError);
  const q = await peer.next("conversation.item.delete");
  peer.emit("conversation.item.deleted", {
    prism_request_event_id: q.event_id,
    item_id: "wrong",
  });
  await failed;
});
for (const item of [
  { type: "message", id: "", role: "system", text: "x" },
  { type: "message", role: "bad", text: "x" },
  { type: "message", role: "user", text: "x".repeat(4001) },
  { type: "message", role: "user", text: "\ud800" },
  { type: "function_call", callId: "", name: "f", arguments: "{}" },
  { type: "unknown" },
])
  test("invalid history " + JSON.stringify(item).slice(0, 80), async (t) => {
    const { session, peer } = await setup(t);
    const before = peer.requests.length;
    await assert.rejects(session.createItem(item), RimeInputError);
    assert.equal(peer.requests.length, before);
  });
test("cancelled history result still unblocks an active tool", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prism.typed_input.ready");
  const turn = session.sendText("hello");
  peer.accepted(await peer.next("response.create"));
  const parent = await turn;
  peer.emit("response.function_call_arguments.done", {
    response_id: parent.responseId,
    item_id: "tool-item",
    call_id: "call-1",
    name: "lookup",
    arguments: "{}",
  });
  peer.ended(parent.responseId);
  const controller = new AbortController();
  const pending = session.createItem(
    { type: "function_call_output", callId: "call-1", output: "result" },
    { signal: controller.signal },
  );
  const cancelled = assert.rejects(pending, { name: "RimeCancelledError" });
  const q = await peer.next("conversation.item.create");
  controller.abort();
  peer.emit("conversation.item.created", {
    prism_request_event_id: q.event_id,
    item: {
      type: "function_call_output",
      id: "result-item",
      call_id: "call-1",
    },
  });
  await cancelled;
  const next = session.continueReply(parent);
  peer.accepted(await peer.next("response.create"), "reply-2");
  await next;
});

test("cancelled history creation still validates item identity", async (t) => {
  const { session, peer } = await setup(t);
  const controller = new AbortController();
  const task = session.createItem(
    { type: "message", id: "right", role: "system", text: "note" },
    { signal: controller.signal },
  );
  const cancelled = assert.rejects(task, { name: "RimeCancelledError" });
  const q = await peer.next("conversation.item.create");
  controller.abort();
  peer.emit("conversation.item.created", {
    prism_request_event_id: q.event_id,
    item: { type: "message", id: "wrong" },
  });
  await cancelled;
  await assert.rejects(session.getHistory(), RimeStreamError);
});

test("reusing a deleted call ID keeps the original response result cached", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prism.typed_input.ready");
  const turn = session.sendText("hello");
  peer.accepted(await peer.next("response.create"));
  const parent = await turn;
  peer.tool(parent.responseId, "shared");
  peer.ended(parent.responseId);
  let originalCall;
  for await (const { payload } of session.events) {
    if (payload.kind === "tool.call") {
      originalCall = payload.call;
      break;
    }
  }
  assert.ok(originalCall);

  const recorded = session.submitToolResult(originalCall, "original");
  peer.ack(await peer.next("conversation.item.create"));
  await recorded;
  const deleted = session.deleteItem("item-shared");
  const request = await peer.next("conversation.item.delete");
  peer.emit("conversation.item.deleted", {
    prism_request_event_id: request.event_id,
    item_id: "item-shared",
  });
  await deleted;

  for (const item of [
    {
      type: "function_call",
      id: "imported-call",
      callId: "shared",
      name: "lookup",
      arguments: "{}",
    },
    {
      type: "function_call_output",
      id: "imported-result",
      callId: "shared",
      output: "imported",
    },
  ]) {
    const imported = session.createItem(item);
    const request = await peer.next("conversation.item.create");
    peer.emit("conversation.item.created", {
      prism_request_event_id: request.event_id,
      item: request.item,
    });
    await imported;
  }

  const requestCount = peer.requests.length;
  await session.submitToolResult(originalCall, "original");
  assert.equal(peer.requests.length, requestCount);
  assert.equal(peer.queue.length, 0);
});
