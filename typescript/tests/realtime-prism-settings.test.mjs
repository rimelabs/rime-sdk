import { test as nativeTest } from "node:test";
import assert from "node:assert/strict";
import {
  Rime,
  RimeInputError,
  RimeRealtimeError,
  RimeStreamError,
} from "../dist/index.js";
import { decode } from "../dist/realtime/protocol.js";
import { Peer } from "./realtime-peer.mjs";

const test = (name, body) => nativeTest(name, { timeout: 5000 }, body);

async function setup(t, options = {}, fields = {}) {
  const peer = await new Peer().start();
  peer.sessionFields = fields;
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    ...options,
  });
  return { session, peer, client };
}

test("new settings are omitted by default and old session views remain valid", async (t) => {
  const { session, peer } = await setup(t);
  assert.deepEqual(peer.settings.turn_detection, { interrupt_response: true });
  for (const key of ["speed", "prism_review"])
    assert.equal(key in peer.settings, false);
  for (const key of ["speed", "version", "review"])
    assert.equal(key in session.info, false);
  assert.equal("interruptPhrases" in session.info.turnDetection, false);
});

test("speed and turn settings are copied and applied only after confirmation", async (t) => {
  const phrases = ["stop"];
  const options = {
    speed: 0.85,
    turnDetection: {
      interruptMinWords: 2,
      interruptPhrases: phrases,
      acknowledgmentPhrases: [],
      endOfTurnMinSilenceMs: 800,
    },
  };
  const { session, peer } = await setup(t, options, {
    speed: 0.85,
    version: "20261104",
    turn_detection: {
      interrupt_min_words: 2,
      interrupt_phrases: ["stop"],
      acknowledgment_phrases: [],
      end_of_turn_min_silence_ms: 800,
    },
  });
  phrases.push("wait");
  assert.deepEqual(peer.settings.turn_detection, {
    interrupt_response: true,
    interrupt_min_words: 2,
    interrupt_phrases: ["stop"],
    acknowledgment_phrases: [],
    end_of_turn_min_silence_ms: 800,
  });
  assert.equal(session.info.speed, 0.85);
  assert.equal(session.info.version, "20261104");
  assert.deepEqual(session.info.turnDetection.interruptPhrases, ["stop"]);
  assert.equal(session.info.turnDetection.endOfTurnMinSilenceMs, 800);
  peer.autoInitialize = false;
  const before = session.info;
  const clearing = [];
  const pending = session.update({
    speed: 1.3,
    interruptOnSpeech: false,
    turnDetection: {
      interruptPhrases: clearing,
      acknowledgmentPhrases: ["yes"],
    },
  });
  clearing.push("changed after call");
  const request = await peer.next("session.update");
  assert.deepEqual(request.session, {
    speed: 1.3,
    turn_detection: {
      interrupt_response: false,
      interrupt_phrases: [],
      acknowledgment_phrases: ["yes"],
    },
  });
  assert.equal(session.info, before);
  peer.emit("session.updated", {
    prism_request_event_id: request.event_id,
    session: {
      id: "session-1",
      version: "development-hash",
      speed: 1.3,
      turn_detection: {
        interrupt_phrases: [],
        acknowledgment_phrases: ["yes"],
      },
    },
  });
  assert.equal((await pending).speed, 1.3);
  assert.equal(session.info.version, "development-hash");
  const applied = session.info;
  const refused = session.update({
    speed: 1,
    turnDetection: { interruptMinWords: 3 },
  });
  const rejection = assert.rejects(refused, RimeRealtimeError);
  peer.fault(await peer.next("session.update"), "invalid_session");
  await rejection;
  assert.equal(session.info, applied);
});

test("version from session.created survives initialization and later updates", async (t) => {
  const peer = await new Peer().start();
  peer.autoCreate = false;
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const connected = new Promise((resolve) =>
    peer.server.once("connection", resolve),
  );
  const pending = client.realtime.connect({ endpoint: peer.endpoint });
  await connected;
  peer.emit("session.created", {
    session: { id: "session-1", version: "commit-123" },
  });
  const session = await pending;
  assert.equal(session.info.version, "commit-123");
  await session.update({ instructions: "Hello" });
  assert.equal(session.info.version, "commit-123");
});

test("invalid settings fail before sending and fixed turn fields cannot be updated", async (t) => {
  const { session, peer, client } = await setup(t);
  for (const speed of [null, NaN, Infinity, 0.84, 1.31, "1"])
    await assert.rejects(
      client.realtime.connect({ endpoint: peer.endpoint, speed }),
      RimeInputError,
    );
  for (const turnDetection of [
    null,
    { interruptMinWords: 0 },
    { interruptMinWords: 1.5 },
    { interruptPhrases: null },
    { acknowledgmentPhrases: Array(101).fill("yes") },
    { interruptPhrases: [null] },
    { endOfTurnMinSilenceMs: 5001 },
  ])
    await assert.rejects(
      client.realtime.connect({ endpoint: peer.endpoint, turnDetection }),
      RimeInputError,
    );
  for (const turnDetection of [
    { endOfTurnMinSilenceMs: 0 },
    { endOfTurnTimeoutMs: 1000 },
    { eagerness: "low" },
  ])
    await assert.rejects(session.update({ turnDetection }), RimeInputError);
  assert.equal(peer.requests.length, 1);
});

test("long Unicode text reaches Prism through typed, proactive and history operations", async (t) => {
  const { session, peer } = await setup(t);
  const value = "😀".repeat(4001);
  for (const operation of [
    () => session.sendText(value),
    () => session.requestReply({ instruction: value }),
  ]) {
    peer.emit("prism.typed_input.ready");
    const result = operation();
    const request = await peer.next("response.create");
    assert.equal(
      request.response.prism_input_text ?? request.response.prism_instruction,
      value,
    );
    const rejected = assert.rejects(result, RimeRealtimeError);
    peer.fault(
      request,
      request.response.prism_input_text
        ? "invalid_typed_input"
        : "invalid_instruction",
    );
    await rejected;
  }
  const history = session.createItem({
    type: "message",
    role: "user",
    text: value,
  });
  const request = await peer.next("conversation.item.create");
  assert.equal(request.item.text, value);
  peer.ack(request);
  await history;
  peer.emit("prism.typed_input.ready");
  const following = session.sendText("still usable");
  peer.accepted(await peer.next("response.create"));
  await following;
});

test("public events retain server time and cumulative transcript including empty corrections", async (t) => {
  const { session, peer } = await setup(t);
  const events = session.events;
  for (const fields of [
    { delta: "Hello" },
    { delta: "", transcript: "Hallo", server_time_ms: 1792857632418 },
    { delta: "", transcript: "", server_time_ms: 1792857632419 },
  ]) {
    peer.emit("conversation.item.input_audio_transcription.delta", {
      item_id: "input-1",
      content_index: 0,
      ...fields,
    });
    const event = (await events.next()).value;
    assert.equal(event.serverTimeMs, fields.server_time_ms);
    assert.equal(event.payload.transcript, fields.transcript);
    assert.equal(event.payload.delta, fields.delta);
  }
  peer.emit("response.created", {
    server_time_ms: 1792857632420,
    response: { id: "reply-1" },
  });
  assert.equal((await events.next()).value.serverTimeMs, 1792857632420);
  peer.emit("response.done", {
    server_time_ms: 1792857632421,
    response: { id: "reply-1", status: "completed" },
  });
  assert.equal((await events.next()).value.serverTimeMs, 1792857632421);
  peer.emit("error", {
    server_time_ms: 1792857632422,
    error: { code: "review_not_found", scope: "event" },
  });
  assert.equal((await events.next()).value.serverTimeMs, 1792857632422);
  await events.return();
});

test("decoders reject malformed present optional values", () => {
  const session = { type: "session.created", session: { id: "session-1" } };
  for (const fields of [
    { version: null },
    { version: 123 },
    { speed: null },
    { speed: "1" },
    { speed: 1.31 },
    { prism_review: null },
    { turn_detection: { interrupt_min_words: 0 } },
    { turn_detection: { interrupt_phrases: null } },
  ])
    assert.throws(
      () =>
        decode(
          JSON.stringify({
            ...session,
            session: { ...session.session, ...fields },
          }),
          null,
        ),
      RimeStreamError,
    );
  const delta = {
    type: "conversation.item.input_audio_transcription.delta",
    item_id: "input",
    delta: "",
  };
  for (const fields of [
    { transcript: null },
    { transcript: 1 },
    ...[-1, 0.1, "1", null, Number.MAX_SAFE_INTEGER + 1].map(
      (server_time_ms) => ({ server_time_ms }),
    ),
  ])
    assert.throws(
      () => decode(JSON.stringify({ ...delta, ...fields }), "session-1"),
      RimeStreamError,
    );
});
