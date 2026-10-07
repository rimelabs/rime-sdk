import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Rime, RimeStreamError } from "../dist/index.js";
import { decode } from "../dist/realtime/protocol.js";
import { Peer } from "./realtime-peer.mjs";

const cases = JSON.parse(
  readFileSync(
    new URL("../../conformance/prism/protocol.json", import.meta.url),
    "utf8",
  ),
);
async function setup(t) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    timeouts: { requestS: 1, readyS: 1 },
  });
  return { session, peer };
}
const watch = (promise) => {
  promise.catch(() => {});
  return promise;
};
async function turn(session, peer) {
  peer.emit("prsm.typed_input.ready");
  const result = watch(session.sendText("hello"));
  peer.accepted(await peer.next("response.create"));
  return result;
}
async function toolCall(session, peer) {
  const parent = await turn(session, peer);
  peer.tool();
  for await (const { payload } of session.events)
    if (payload.kind === "tool.call") return { parent, call: payload.call };
  throw new Error("No tool call");
}
for (const item of cases) {
  test(`shared protocol: ${item.name}`, () => {
    const raw = JSON.stringify(item.event);
    if (item.invalid)
      assert.throws(() => decode(raw, "session-1"), RimeStreamError);
    else
      assert.equal(
        decode(raw, "session-1")?.payload.kind ?? "ignored",
        item.expected_kind,
      );
  });
  if (item.invalid)
    test(`invalid payload fails pending request and iterator: ${item.name}`, async (t) => {
      const { session, peer } = await setup(t);
      await turn(session, peer);
      const pending = watch(session.addMessage("user", "history"));
      await peer.next("conversation.item.create");
      peer.socket.send(JSON.stringify(item.event));
      await assert.rejects(pending, RimeStreamError);
      await assert.rejects(session.events.next(), RimeStreamError);
      assert.equal(peer.queue.length, 0);
    });
}
for (const [operation, acknowledgment] of [
  ["text", "session.updated"],
  ["proactive", "conversation.item.created"],
  ["history", "session.updated"],
  ["history", "response.created"],
  ["clear", "session.updated"],
  ["tool", "session.updated"],
  ["tool", "conversation.item.created"],
  ["tool", "wrong-call"],
]) {
  test(`reject ${acknowledgment} acknowledgment for ${operation}`, async (t) => {
    const { session, peer } = await setup(t);
    let pending, request;
    if (operation === "text" || operation === "proactive") {
      peer.emit("prsm.typed_input.ready");
      pending = watch(
        operation === "text"
          ? session.sendText("hello")
          : session.requestReply(),
      );
      request = await peer.next("response.create");
    } else if (operation === "tool") {
      const { call } = await toolCall(session, peer);
      pending = watch(session.submitToolResult(call, "done"));
      request = await peer.next("conversation.item.create");
    } else if (operation === "clear") {
      pending = watch(session.clearAudio());
      request = await peer.next("input_audio_buffer.clear");
    } else {
      pending = watch(session.addMessage("user", "history"));
      request = await peer.next("conversation.item.create");
    }
    if (acknowledgment === "session.updated")
      peer.emit(acknowledgment, {
        prsm_request_event_id: request.event_id,
        session: { id: "session-1" },
      });
    else if (acknowledgment === "response.created")
      peer.accepted(request, "wrong-response");
    else if (acknowledgment === "wrong-call")
      peer.emit("conversation.item.created", {
        prsm_request_event_id: request.event_id,
        item: { type: "function_call_output", call_id: "another-call" },
      });
    else
      peer.emit(acknowledgment, {
        prsm_request_event_id: request.event_id,
        item: { id: "history-1", type: "message", role: "user" },
      });
    await assert.rejects(pending, RimeStreamError);
    await assert.rejects(session.events.next(), RimeStreamError);
    assert.equal(peer.queue.length, 0);
  });
}

test("tool result acknowledgment without item ID permits continuation", async (t) => {
  const { session, peer } = await setup(t);
  const { parent, call } = await toolCall(session, peer);
  const result = watch(session.submitToolResult(call, "done"));
  const request = await peer.next("conversation.item.create");
  peer.emit("conversation.item.created", {
    prsm_request_event_id: request.event_id,
    item: { type: "function_call_output", call_id: call.callId },
  });
  await result;
  peer.ended();
  const continuation = watch(session.continueReply(parent));
  peer.accepted(await peer.next("response.create"), "reply-2");
  assert.equal((await continuation).responseId, "reply-2");
});

test("unknown events and stale acknowledgments do not consume a request", async (t) => {
  const { session, peer } = await setup(t);
  const pending = watch(session.addMessage("user", "history"));
  const request = await peer.next("conversation.item.create");
  peer.emit("future.event", {
    prsm_request_event_id: request.event_id,
    anything: true,
  });
  peer.emit("session.updated", {
    prsm_request_event_id: "old-request",
    session: { id: "session-1" },
  });
  peer.ack(request);
  assert.equal((await pending).itemId, "history-1");
});
