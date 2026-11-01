import { test } from "node:test";
import assert from "node:assert/strict";
import {
  Rime,
  RimeInputError,
  RimeResourceLimitError,
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
  return { peer, session, events: session.events };
}
async function payload(events, kind) {
  const next = await events.next();
  assert.equal(next.value.payload.kind, kind);
  return next.value.payload;
}
async function observed(peer, events, id, tool = false) {
  peer.emit("response.created", { response: { id } });
  const { response } = await payload(events, "response.started");
  let call;
  if (tool) {
    peer.tool(id, `call-${id}`);
    call = (await payload(events, "tool.call")).call;
  }
  peer.ended(id);
  await payload(events, "response.ended");
  return { response, call };
}
async function age(peer, events, prefix = "later") {
  for (let i = 0; i < 140; i++) await observed(peer, events, `${prefix}-${i}`);
}

test(
  "a completion for a never-started response fails the session",
  { timeout: 3000 },
  async (t) => {
    const { peer, session, events } = await setup(t);
    peer.ended("never-started");
    await assert.rejects(events.next(), RimeStreamError);
    assert.throws(
      () =>
        session.holdResponse({
          sessionId: "session-1",
          responseId: "never-started",
        }),
      RimeStreamError,
    );
    assert.equal(peer.requests.length, 1, "invalid completion sends no writes");
  },
);

test(
  "duplicate completions use the last 128 evicted IDs without extending their history",
  { timeout: 3000 },
  async (t) => {
    const { peer, session, events } = await setup(t);
    const { response } = await observed(peer, events, "oldest");
    for (let i = 0; i < 255; i++) await observed(peer, events, `later-${i}`);
    assert.throws(() => session.holdResponse(response), RimeInputError);
    peer.ended("oldest");
    peer.ended("oldest");
    peer.ended("later-254");
    peer.emit("prism.typed_input.ready");
    await payload(events, "input.ready");
    assert.equal(
      peer.requests.length,
      1,
      "duplicate completions send no writes",
    );
    await observed(peer, events, "overflow");
    peer.ended("later-0");
    peer.emit("prism.typed_input.ready");
    await payload(events, "input.ready");
    peer.ended("oldest");
    await assert.rejects(events.next(), RimeStreamError);
    assert.equal(peer.requests.length, 1, "expired completion sends no writes");
  },
);

test(
  "response creation cannot reuse a recently evicted ID",
  { timeout: 3000 },
  async (t) => {
    const { peer, events } = await setup(t);
    await observed(peer, events, "evicted");
    await age(peer, events);
    peer.emit("response.created", { response: { id: "evicted" } });
    await assert.rejects(events.next(), RimeStreamError);
    assert.equal(peer.requests.length, 1, "duplicate creation sends no writes");
  },
);

test("ordinary expiry, delayed playback, nested holds and duplicate terminal events", async (t) => {
  const { peer, session, events } = await setup(t);
  const { response } = await observed(peer, events, "held");
  const first = session.holdResponse(response),
    second = session.holdResponse(response);
  const ordinary = (await observed(peer, events, "ordinary")).response;
  first.release();
  first.release();
  await age(peer, events);
  assert.throws(() => session.holdResponse(ordinary), RimeInputError);
  assert.throws(
    () => session.holdResponse({ ...response, sessionId: "other" }),
    RimeInputError,
  );
  assert.throws(
    () => session.holdResponse({ ...response, responseId: "missing" }),
    RimeInputError,
  );
  assert.equal(peer.requests.length, 1, "holds send no writes");
  peer.ended("held");
  peer.ended("ordinary");
  peer.emit("prism.typed_input.ready");
  await payload(events, "input.ready");
  await session.reportPlayback({ kind: "finished", response, playedMs: 12 });
  assert.equal((await peer.next("prism.playback.drained")).response_id, "held");
  second.release();
  second.release();
  assert.throws(() => session.holdResponse(response), RimeInputError);
  await session.close();
  second.release();
});

test("unresolved tools survive 140 responses and a hold covers result and report", async (t) => {
  const { peer, session, events } = await setup(t);
  const { response, call } = await observed(peer, events, "tool", true);
  await age(peer, events);
  const hold = session.holdResponse(call);
  try {
    assert.throws(
      () => session.holdResponse({ ...call, callId: "missing" }),
      RimeInputError,
    );
    const result = session.submitToolResult(call, "done");
    peer.ack(await peer.next("conversation.item.create"));
    await result;
    await session.submitToolResult(call, "done");
    await assert.rejects(
      session.submitToolResult(call, "different"),
      RimeInputError,
    );
    await assert.rejects(session.continueReply(response), /superseded/);
    await age(peer, events, "after-result");
    assert.equal(
      peer.requests.filter((r) => r.type === "conversation.item.create").length,
      1,
    );
    peer.emit("prism.typed_input.ready");
    const report = session.requestReply({ toolCall: call });
    const request = await peer.next("response.create");
    assert.equal(request.response.prism_call_id, call.callId);
    peer.accepted(request, "report");
    await report;
  } finally {
    hold.release();
  }
  assert.throws(() => session.holdResponse(response), RimeInputError);
});

for (const overflow of ["generation", "late tool"]) {
  test(`protected capacity refusal stays local; incoming ${overflow} fails explicitly`, async (t) => {
    const { peer, session, events } = await setup(t);
    const ordinary = (await observed(peer, events, "ordinary")).response;
    let last;
    const holds = [];
    for (let i = 0; i < 128; i++) {
      last = (await observed(peer, events, `protected-${i}`)).response;
      holds.push(session.holdResponse(last));
    }
    assert.throws(() => session.holdResponse(ordinary), RimeResourceLimitError);
    const nested = session.holdResponse(last);
    nested.release();
    nested.release();
    assert.equal(peer.requests.length, 1);
    await session.reportPlayback({ kind: "finished", response: ordinary });
    await peer.next("prism.playback.drained");
    if (overflow === "generation")
      peer.emit("response.created", { response: { id: "overflow" } });
    else peer.tool("ordinary", "overflow-call");
    await assert.rejects(events.next(), RimeResourceLimitError);
    await session.close();
    for (const hold of holds) {
      hold.release();
      hold.release();
    }
  });
}

test("active generation remains protected after its last hold is released", async (t) => {
  const { peer, session, events } = await setup(t);
  peer.emit("response.created", { response: { id: "active" } });
  const { response } = await payload(events, "response.started");
  const hold = session.holdResponse(response);
  hold.release();
  await age(peer, events);
  session.holdResponse(response).release();
  assert.throws(() => session.holdResponse(null), RimeInputError);
  peer.ended("active");
  await payload(events, "response.ended");
  assert.throws(() => session.holdResponse(response), RimeInputError);
  assert.equal(peer.requests.length, 1);
});
