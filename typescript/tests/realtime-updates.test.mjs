import { test } from "node:test";
import assert from "node:assert/strict";
import { Rime, RimeInputError, RimeRealtimeError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

async function setup(t) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  peer.autoInitialize = false;
  return { session, peer };
}
function acknowledge(peer, request, revision, voice = "astra") {
  peer.emit("session.updated", {
    prism_request_event_id: request.event_id,
    session: {
      id: "session-1",
      voice,
      prism_effective_interrupt_response: false,
      prism_settings_revision: revision,
    },
  });
}

test("partial updates and clearing work without turn readiness", async (t) => {
  const { session, peer } = await setup(t);
  const original = session.info;
  const parameters = { type: "object" };
  const pending = session.update({
    instructions: "Handle billing.",
    tools: [{ name: "refund", parameters, prismFiller: { type: "none" } }],
  });
  parameters.type = "string";
  const q = await peer.next("session.update");
  assert.deepEqual(q.session, {
    instructions: "Handle billing.",
    tools: [
      {
        type: "function",
        function: {
          name: "refund",
          description: "",
          parameters: { type: "object" },
        },
        prism_filler: { type: "none" },
      },
    ],
  });
  assert.equal(session.info, original);
  acknowledge(peer, q, 2);
  const info = await pending;
  assert.equal(info, session.info);
  assert.equal(info.settingsRevision, 2);
  assert.equal(info.voice, "astra");
  assert.equal(original.voice, "test");
  const cleared = session.update({
    instructions: "",
    tools: [],
    voice: "",
    interruptOnSpeech: false,
  });
  const clear = await peer.next("session.update");
  assert.deepEqual(clear.session, {
    instructions: "",
    tools: [],
    voice: "",
    turn_detection: { interrupt_response: false },
  });
  acknowledge(peer, clear, 3);
  assert.equal((await cleared).settingsRevision, 3);
  for (const options of [
    {
      tools: [
        { name: "same", parameters: {} },
        { name: "same", parameters: {} },
      ],
    },
    { tools: [{ name: "bad", parameters: { n: NaN } }] },
    { instructions: null },
    { interruptOnSpeech: null },
  ])
    await assert.rejects(session.update(options), RimeInputError);
  peer.emit("response.created", {
    response: { id: "early-guess", prism_settings_revision: 2 },
  });
  const events = session.events;
  assert.equal((await events.next()).value.payload.settingsRevision, 2);
  assert.equal(session.info.settingsRevision, 3);
  await events.return();
  assert.deepEqual(peer.invalid, []);
});

test("refusals and concurrent updates preserve the connection and request matching", async (t) => {
  const { session, peer } = await setup(t);
  const original = session.info;
  const pending = session.update({ voice: "missing" });
  const refused = assert.rejects(pending, RimeRealtimeError);
  peer.fault(await peer.next("session.update"), "invalid_voice");
  await refused;
  assert.equal(session.info, original);
  const first = session.update({ instructions: "First" });
  const q1 = await peer.next("session.update");
  const second = session.update({ tools: [] });
  const q2 = await peer.next("session.update");
  acknowledge(peer, { event_id: "unrelated" }, 99, "wrong");
  acknowledge(peer, q1, 2);
  assert.equal((await first).settingsRevision, 2);
  acknowledge(peer, q2, 3);
  assert.equal((await second).settingsRevision, 3);
  assert.equal(session.info.settingsRevision, 3);
});

test("a cancelled submitted update still applies its acknowledgment", async (t) => {
  const { session, peer } = await setup(t);
  const controller = new AbortController();
  const pending = session.update({
    instructions: "Billing",
    signal: controller.signal,
  });
  const q = await peer.next("session.update");
  const cancelled = assert.rejects(pending, { name: "RimeCancelledError" });
  controller.abort();
  acknowledge(peer, q, 2);
  await cancelled;
  assert.equal(session.info.settingsRevision, 2);
});

test("updates preserve tool calls and each response's revision", async (t) => {
  const { session, peer } = await setup(t);
  const events = session.events;
  peer.emit("prism.typed_input.ready");
  const turn = session.sendText("hello");
  peer.accepted(await peer.next("response.create"));
  const parent = await turn;
  await events.next();
  await events.next();
  peer.tool();
  const call = (await events.next()).value.payload.call;
  const update = session.update({ instructions: "Billing", tools: [] });
  acknowledge(peer, await peer.next("session.update"), 2);
  await update;
  peer.emit("response.done", {
    response: {
      id: "reply-1",
      status: "completed",
      prism_settings_revision: 1,
    },
  });
  assert.equal((await events.next()).value.payload.settingsRevision, 1);
  assert.equal(session.info.settingsRevision, 2);
  const result = session.submitToolResult(call, "transferred");
  const q = await peer.next("conversation.item.create");
  peer.emit("conversation.item.created", {
    prism_request_event_id: q.event_id,
    item: { type: "function_call_output", call_id: call.callId },
  });
  await result;
  const next = session.continueReply(parent);
  const request = await peer.next("response.create");
  peer.emit("response.created", {
    prism_request_event_id: request.event_id,
    response: {
      id: "reply-2",
      prism_settings_revision: 2,
      metadata: request.response.metadata,
    },
  });
  await next;
  assert.equal((await events.next()).value.payload.settingsRevision, 2);
  await events.return();
});
