import { test as nativeTest } from "node:test";
import assert from "node:assert/strict";
import { Rime, RimeInputError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

const test = (name, body) => nativeTest(name, { timeout: 5000 }, body);

/** A peer that echoes turn_detection null on the first update, as Prism does. */
async function setup(t, options = {}) {
  const peer = await new Peer().start();
  const endsTurns = options.clientEndsTurns === true;
  peer.sessionFields = endsTurns
    ? { turn_detection: null, prism_effective_interrupt_response: false }
    : {
        turn_detection: { eagerness: "auto" },
        prism_effective_interrupt_response: true,
      };
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    timeouts: { requestS: 0.5, readyS: 1 },
    ...options,
  });
  peer.emit("prism.typed_input.ready");
  return { session, peer };
}

async function nextOf(events, kind) {
  for (;;) {
    const { value } = await events.next();
    if (value.payload.kind === kind) return value;
  }
}

function speech() {
  return {
    data: new Uint8Array(640),
    format: { sampleRate: 16000, channels: 1 },
  };
}

test("client-ended turns send turn_detection null", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  assert.equal(peer.settings.turn_detection, null);
  assert.equal(session.info.clientEndsTurns, true);
  assert.equal(session.info.interruptOnSpeech, false);
  assert.deepEqual(peer.invalid, []);
});

test("Prism ends turns by default", async (t) => {
  const { session, peer } = await setup(t);
  assert.deepEqual(peer.settings.turn_detection, { interrupt_response: true });
  assert.equal(session.info.clientEndsTurns, false);
});

test("push-to-talk round trip", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  await session.startTurn();
  const start = await peer.next("prism.input_audio_buffer.start");
  assert.deepEqual(Object.keys(start).sort(), ["event_id", "type"]);
  await session.sendAudio(speech());
  await peer.next("input_audio_buffer.append");
  await session.endTurn();
  const commit = await peer.next("input_audio_buffer.commit");
  assert.deepEqual(Object.keys(commit).sort(), ["event_id", "type"]);
  const pending = session.replyToTurns();
  const request = await peer.next("response.create");
  assert.deepEqual(request.response, {});
  peer.accepted(request);
  assert.equal((await pending).responseId, "reply-1");
  assert.deepEqual(peer.invalid, []);
});

test("replyToTurns carries labels without a cause", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  const pending = session.replyToTurns({ metadata: { order: "42" } });
  const request = await peer.next("response.create");
  assert.deepEqual(request.response, { metadata: { order: "42" } });
  peer.accepted(request);
  await pending;
  assert.deepEqual(peer.invalid, []);
});

test("replyToTurns waits while another reply is active", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  let settled = false;
  const pending = session.replyToTurns().finally(() => (settled = true));
  const first = await peer.next("response.create");
  peer.fault(first, "conversation_already_has_active_response");
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(settled, false);
  peer.emit("prism.typed_input.ready");
  const second = await peer.next("response.create");
  assert.notEqual(second.event_id, first.event_id);
  peer.accepted(second);
  await pending;
});

test("an endTurn refusal arrives as an error event", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  const events = session.events;
  await session.endTurn();
  const commit = await peer.next("input_audio_buffer.commit");
  peer.fault(commit, "input_audio_buffer_commit_empty");
  const event = await nextOf(events, "error");
  assert.equal(event.payload.error.code, "input_audio_buffer_commit_empty");
  assert.equal(event.payload.error.requestId, commit.event_id);
  await session.endTurn();
  await peer.next("input_audio_buffer.commit");
});

test("endTurn also ends a turn Prism detects", async (t) => {
  const { session, peer } = await setup(t);
  await session.endTurn();
  await peer.next("input_audio_buffer.commit");
});

test("client_commit is a known end reason", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  const events = session.events;
  peer.emit("input_audio_buffer.speech_stopped", {
    item_id: "speech",
    audio_end_ms: 1200,
    prism_end_reason: "client_commit",
  });
  const event = await nextOf(events, "speech.stopped");
  assert.equal(event.payload.endReason, "client_commit");
});

test("turn starts and replies need client-ended turns", async (t) => {
  const { session, peer } = await setup(t);
  await assert.rejects(session.startTurn(), /clientEndsTurns/);
  await assert.rejects(session.replyToTurns(), /clientEndsTurns/);
  assert.equal(
    peer.requests.some(
      (event) =>
        event.type === "prism.input_audio_buffer.start" ||
        event.type === "response.create",
    ),
    false,
  );
});

for (const [name, options] of [
  ["turn settings", { turnDetection: { eagerness: "low" } }],
  ["interruption off", { interruptOnSpeech: false }],
]) {
  test(`client-ended turns refuse ${name}`, async (t) => {
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    await assert.rejects(
      client.realtime.connect({
        endpoint: peer.endpoint,
        clientEndsTurns: true,
        ...options,
      }),
      (error) =>
        error instanceof RimeInputError &&
        /clientEndsTurns/.test(error.message),
    );
  });
}

test("client-ended turns refuse later turn settings", async (t) => {
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  const before = peer.requests.length;
  await assert.rejects(
    session.update({ interruptOnSpeech: true }),
    /clientEndsTurns/,
  );
  await assert.rejects(
    session.update({ turnDetection: { interruptMinWords: 2 } }),
    /clientEndsTurns/,
  );
  assert.equal(peer.requests.length, before);
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("replyToTurns waits for readiness after the turn", async (t) => {
  // Prism's own order: the turn's start clears readiness, and its end restores it.
  const { session, peer } = await setup(t, { clientEndsTurns: true });
  const events = session.events;
  await session.startTurn();
  await peer.next("prism.input_audio_buffer.start");
  peer.emit("input_audio_buffer.speech_started", {
    item_id: "turn-1",
    audio_start_ms: 0,
  });
  await session.sendAudio(speech());
  await peer.next("input_audio_buffer.append");
  await session.endTurn();
  await peer.next("input_audio_buffer.commit");
  peer.emit("input_audio_buffer.speech_stopped", {
    item_id: "turn-1",
    audio_end_ms: 40,
    prism_end_reason: "client_commit",
  });
  peer.emit("input_audio_buffer.committed", { item_id: "turn-1" });
  await nextOf(events, "input.committed");
  const pending = session.replyToTurns();
  await sleep(50);
  assert.equal(
    peer.queue.length,
    0,
    "the reply request went out before Prism was ready",
  );
  peer.emit("prism.typed_input.ready");
  const request = await peer.next("response.create");
  assert.deepEqual(request.response, {});
  peer.accepted(request);
  assert.equal((await pending).responseId, "reply-1");
});

for (const [operation, kind] of [
  ["startTurn", "prism.input_audio_buffer.start"],
  ["endTurn", "input_audio_buffer.commit"],
]) {
  test(`${operation} follows audio sent before it`, async (t) => {
    const { session, peer } = await setup(t, { clientEndsTurns: true });
    // A clear waiting for its acknowledgment holds back the audio sent after it.
    const clearing = session.clearAudio();
    await peer.next("input_audio_buffer.clear");
    const sending = session.sendAudio({
      data: new Uint8Array(3840),
      format: { sampleRate: 16000, channels: 1 },
    });
    await sleep(10);
    const control = session[operation]();
    await sleep(50);
    assert.equal(
      peer.queue.length,
      0,
      "the turn control overtook audio sent before it",
    );
    peer.emit("input_audio_buffer.cleared");
    await Promise.all([clearing, sending, control]);
    // 120 ms of audio is three 40 ms appends.
    for (let i = 0; i < 3; i++) await peer.next("input_audio_buffer.append");
    await peer.next(kind);
  });
}
