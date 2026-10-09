import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import {
  Rime,
  RimeInputError,
  RimeAudioFormatError,
  RimeCancelledError,
  RimeResourceLimitError,
  RimeStreamError,
  RimeTimeoutError,
  RimeAuthenticationError,
  RimeUnavailableError,
  RimeRealtimeError,
  RealtimeAdmissionTimeout,
} from "../dist/index.js";
import { RealtimeConnection } from "../dist/realtime/transport.js";
import { Peer } from "./realtime-peer.mjs";

const tick = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const watch = (promise) => {
  promise.catch(() => {});
  return promise;
};
const tool = {
  name: "lookup_order",
  parameters: { type: "object", properties: { order_id: { type: "string" } } },
};
async function setup(t, options = {}) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    tools: [tool],
    timeouts: { requestS: 0.4, readyS: 0.5, connectS: 1 },
    ...options,
  });
  const events = session.events;
  return { peer, client, session, events };
}
async function payload(events, kind) {
  for (;;) {
    const item = await events.next();
    assert.equal(item.done, false, `Missing ${kind}`);
    if (item.value.payload.kind === kind) return item.value.payload;
  }
}
async function turn(session, peer, id = "reply-1") {
  peer.emit("prsm.typed_input.ready");
  const result = watch(session.sendText("hello"));
  const request = await peer.next("response.create");
  peer.accepted(request, id);
  return result;
}
async function round(session, peer, events) {
  const ref = await turn(session, peer);
  peer.tool();
  const call = (await payload(events, "tool.call")).call;
  const result = watch(session.submitToolResult(call, "done"));
  peer.ack(await peer.next("conversation.item.create"));
  await result;
  peer.ended();
  await payload(events, "response.ended");
  return { ref, call };
}

test("Prism config, direct credentials, typed output and large incoming audio", async (t) => {
  const { session, peer, events } = await setup(t, {
    voice: "test",
    instructions: "Be brief",
  });
  assert.equal(peer.authorization, "Bearer test");
  assert.deepEqual(peer.settings, {
    modalities: ["text", "audio"],
    input_audio_format: "pcm16",
    turn_detection: { interrupt_response: true },
    tools: [{ type: "function", function: { ...tool, description: "" } }],
    voice: "test",
    instructions: "Be brief",
  });
  assert.deepEqual(session.info, {
    sessionId: "session-1",
    model: "prism",
    voice: "test",
    interruptOnSpeech: false,
    toolResultTimeoutS: 20,
    toolContinuationTimeoutS: 5,
  });
  const ref = await turn(session, peer);
  const start = await payload(events, "response.started");
  assert.deepEqual(start.response, ref);
  assert.equal(start.cause, "text");
  peer.message();
  const output = (await payload(events, "message.started")).output;
  const coordinates = {
    response_id: ref.responseId,
    item_id: output.itemId,
    output_index: 0,
    content_index: 0,
  };
  peer.emit("response.text.delta", { ...coordinates, delta: "Hello" });
  assert.equal((await payload(events, "text.delta")).delta, "Hello");
  const bytes = Buffer.alloc(192002, 1);
  peer.emit("response.audio.delta", {
    ...coordinates,
    delta: bytes.toString("base64"),
  });
  const audio = await payload(events, "audio.delta");
  assert.deepEqual(audio.audio.data, bytes);
  assert.deepEqual(audio.audio.format, {
    sampleRate: 24000,
    channels: 1,
    encoding: "pcm_s16le",
  });
  peer.ended();
  assert.equal((await payload(events, "response.ended")).status, "completed");
});

test("one readiness event admits one turn; named busy refusal waits for new readiness", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prsm.typed_input.ready");
  const first = watch(session.sendText("first")),
    second = watch(session.sendText("second"));
  const refused = await peer.next("response.create");
  peer.fault(refused, "typed_turn_busy");
  await sleep(20);
  assert.equal(peer.queue.length, 0);
  peer.emit("prsm.typed_input.ready");
  peer.accepted(await peer.next("response.create"));
  await first;
  await sleep(20);
  assert.equal(peer.queue.length, 0);
  peer.ended();
  peer.emit("prsm.typed_input.ready");
  peer.accepted(await peer.next("response.create"), "reply-2");
  assert.equal((await second).responseId, "reply-2");
});

test("admission timeout is recoverable and abort before readiness sends nothing", async (t) => {
  const { session, peer } = await setup(t, {
    timeouts: { readyS: 0.03, requestS: 0.5 },
  });
  await assert.rejects(session.sendText("waiting"), RealtimeAdmissionTimeout);
  const controller = new AbortController();
  const cancelled = watch(
    session.sendText("never", { signal: controller.signal }),
  );
  controller.abort();
  await assert.rejects(cancelled, RimeCancelledError);
  assert.equal(peer.queue.length, 0);
  await turn(session, peer);
});

test("request acknowledgment has its own deadline after admission", async (t) => {
  const { session, peer } = await setup(t, {
    timeouts: { readyS: 0.03, requestS: 0.4 },
  });
  peer.emit("prsm.typed_input.ready");
  const result = watch(session.sendText("hello"));
  const request = await peer.next("response.create");
  await sleep(60);
  peer.accepted(request);
  assert.equal((await result).responseId, "reply-1");
});

test("unknown request outcome closes the session and never replays", async (t) => {
  const { session, peer, events } = await setup(t, {
    timeouts: { readyS: 0.2, requestS: 0.03 },
  });
  peer.emit("prsm.typed_input.ready");
  const result = watch(session.sendText("hello"));
  await peer.next("response.create");
  await assert.rejects(
    result,
    (error) =>
      error instanceof RimeTimeoutError &&
      !(error instanceof RealtimeAdmissionTimeout),
  );
  await assert.rejects(events.next(), RimeTimeoutError);
  await assert.rejects(session.sendText("retry"), RimeTimeoutError);
  assert.equal(
    peer.requests.filter((r) => r.type === "response.create").length,
    1,
  );
});

test("aborted submitted turn cancels its late response exactly once", async (t) => {
  const { session, peer, events } = await setup(t);
  const controller = new AbortController();
  peer.emit("prsm.typed_input.ready");
  const result = watch(
    session.sendText("hello", { signal: controller.signal }),
  );
  const request = await peer.next("response.create");
  controller.abort();
  await assert.rejects(result, RimeCancelledError);
  peer.accepted(request);
  assert.equal(
    (await payload(events, "response.abandoned")).response.responseId,
    "reply-1",
  );
  await peer.next("response.cancel");
  peer.ended("reply-1", "cancelled");
  assert.equal((await payload(events, "response.ended")).status, "cancelled");
  await tick();
  assert.equal(
    peer.requests.filter((r) => r.type === "response.cancel").length,
    1,
  );
  assert.equal(
    peer.requests.filter((r) => r.type === "prsm.playback.drained").length,
    0,
  );
});

test("typed supersession matches the acknowledged item ID", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prsm.typed_input.ready");
  const result = watch(session.sendText("hello"));
  const request = await peer.next("response.create");
  peer.ack(request);
  peer.fault(null, "typed_turn_superseded", "utterance", {
    item_id: "history-1",
  });
  await assert.rejects(
    result,
    (error) => error.fault.code === "typed_turn_superseded",
  );
  await turn(session, peer);
});

test("history and proactive replies use distinct wire operations", async (t) => {
  const { session, peer } = await setup(t);
  const history = watch(session.addMessage("assistant", "Prior answer"));
  const request = await peer.next("conversation.item.create");
  assert.deepEqual(request.item, {
    type: "message",
    role: "assistant",
    text: "Prior answer",
  });
  peer.ack(request);
  assert.deepEqual(await history, {
    sessionId: "session-1",
    itemId: "history-1",
  });
  peer.emit("prsm.typed_input.ready");
  const reply = watch(
    session.requestReply({ instruction: "Welcome the caller" }),
  );
  const proactive = await peer.next("response.create");
  assert.deepEqual(proactive.response, {
    prsm_instruction: "Welcome the caller",
    metadata: { prsm_cause: "proactive" },
  });
  peer.accepted(proactive);
  await reply;
});

test("tool continuation waits for every result acknowledgment and parent completion", async (t) => {
  const { session, peer, events } = await setup(t);
  const ref = await turn(session, peer);
  peer.tool();
  peer.tool();
  peer.tool("reply-1", "call-2");
  const call1 = (await payload(events, "tool.call")).call;
  const call2 = (await payload(events, "tool.call")).call;
  assert.equal(call2.callId, "call-2");
  const continuation = watch(session.continueReply(ref));
  const result1 = watch(session.submitToolResult(call1, "one"));
  const result2 = watch(session.submitToolResult(call2, "two"));
  const ack1 = await peer.next("conversation.item.create");
  assert.equal(peer.queue.length, 0);
  peer.ack(ack1);
  await result1;
  const ack2 = await peer.next("conversation.item.create");
  peer.ended();
  await payload(events, "response.ended");
  assert.equal(peer.queue.length, 0);
  peer.ack(ack2);
  await result2;
  const request = await peer.next("response.create");
  assert.deepEqual(request.response.metadata, {
    prsm_cause: "tool_continuation",
    prsm_parent_response_id: ref.responseId,
  });
  peer.accepted(request, "reply-2");
  await continuation;
  await session.submitToolResult(call1, "one");
  await assert.rejects(
    session.submitToolResult(call1, "changed"),
    RimeInputError,
  );
  await assert.rejects(session.continueReply(ref), RimeInputError);
});

for (const code of ["tool_continuation_not_ready", "invalid_client_event"]) {
  test(`continuation retry protection after ${code}`, async (t) => {
    const { session, peer, events } = await setup(t);
    const { ref } = await round(session, peer, events);
    const continuation = watch(session.continueReply(ref));
    peer.fault(await peer.next("response.create"), code);
    await assert.rejects(continuation, RimeRealtimeError);
    if (code === "tool_continuation_not_ready") {
      const retry = watch(session.continueReply(ref));
      peer.accepted(await peer.next("response.create"), "reply-2");
      await retry;
    } else await assert.rejects(session.continueReply(ref), RimeInputError);
  });
}

test("late tool result is recorded and can be reported by call ID", async (t) => {
  const { session, peer, events } = await setup(t);
  const first = await turn(session, peer);
  peer.tool();
  const call = (await payload(events, "tool.call")).call;
  peer.ended();
  await turn(session, peer, "reply-2");
  await assert.rejects(session.continueReply(first), RimeInputError);
  await assert.rejects(
    session.requestReply({ toolCall: call }),
    RimeInputError,
  );
  const result = watch(session.submitToolResult(call, "late"));
  peer.ack(await peer.next("conversation.item.create"));
  await result;
  peer.ended("reply-2");
  peer.emit("prsm.typed_input.ready");
  const report = watch(session.requestReply({ toolCall: call }));
  const request = await peer.next("response.create");
  assert.equal(request.response.prsm_call_id, call.callId);
  peer.accepted(request, "reply-3");
  await report;
});

test("cancelled tool result keeps matching its acknowledgment", async (t) => {
  const { session, peer, events } = await setup(t);
  const ref = await turn(session, peer);
  peer.tool();
  const call = (await payload(events, "tool.call")).call;
  const controller = new AbortController();
  const result = watch(
    session.submitToolResult(call, "done", { signal: controller.signal }),
  );
  const request = await peer.next("conversation.item.create");
  controller.abort();
  await assert.rejects(result, RimeCancelledError);
  const continued = watch(session.continueReply(ref));
  peer.ended();
  peer.ack(request);
  peer.accepted(await peer.next("response.create"), "reply-2");
  await continued;
  await session.submitToolResult(call, "done");
});

test("clear remains serialized after caller cancellation; refusal releases its slot", async (t) => {
  const { session, peer } = await setup(t);
  const controller = new AbortController();
  const first = watch(session.clearAudio({ signal: controller.signal }));
  await peer.next("input_audio_buffer.clear");
  controller.abort();
  const second = watch(session.clearAudio());
  await sleep(20);
  assert.equal(peer.queue.length, 0);
  peer.emit("input_audio_buffer.cleared");
  await assert.rejects(first, RimeCancelledError);
  const secondRequest = await peer.next("input_audio_buffer.clear");
  peer.fault(secondRequest, "invalid_client_event");
  await assert.rejects(second, RimeRealtimeError);
  const third = watch(session.clearAudio());
  await peer.next("input_audio_buffer.clear");
  peer.emit("input_audio_buffer.cleared");
  await third;
});

test("playback uses message identity, waits for done, and sends the drain token", async (t) => {
  const { session, peer, events } = await setup(t);
  const ref = await turn(session, peer);
  peer.message();
  const output = (await payload(events, "message.started")).output;
  const stop = watch(
    session.reportPlayback({ kind: "interrupted", output, audioEndMs: 123 }),
  );
  const truncate = await peer.next("conversation.item.truncate");
  assert.equal(truncate.item_id, output.itemId);
  peer.emit("conversation.item.truncated", {
    item_id: output.itemId,
    content_index: 0,
  });
  await stop;
  const finish = watch(
    session.reportPlayback({
      kind: "finished",
      response: ref,
      playedMs: 123,
      audibleTailMs: 2,
    }),
  );
  await sleep(20);
  assert.equal(peer.queue.length, 0);
  peer.ended();
  await finish;
  const drained = await peer.next("prsm.playback.drained");
  assert.deepEqual(drained, {
    type: "prsm.playback.drained",
    response_id: ref.responseId,
    drain_token: "drain-1",
    played_ms: 123,
    audible_tail_ms: 2,
  });
  await assert.rejects(
    session.reportPlayback({
      kind: "interrupted",
      output: { ...output, outputIndex: 1 },
      audioEndMs: 0,
    }),
    RimeInputError,
  );
});

test("cancel requests share a slot; completion can win the cancellation race", async (t) => {
  const { session, peer, events } = await setup(t);
  const ref = await turn(session, peer);
  const first = watch(session.cancel(ref)),
    second = watch(session.cancel(ref));
  const request = await peer.next("response.cancel");
  peer.fault(request, "cancel_refused");
  await assert.rejects(first, RimeRealtimeError);
  await peer.next("response.cancel");
  peer.ended();
  await second;
  assert.equal((await payload(events, "response.ended")).status, "completed");
  await session.cancel(ref);
  assert.equal(peer.queue.length, 0);
});

test("response faults do not end generation; unknown enum values stay explicit", async (t) => {
  const { session, peer, events } = await setup(t);
  peer.emit("response.created", {
    response: { id: "reply-1", metadata: { prsm_cause: "future" } },
  });
  const start = await payload(events, "response.started");
  assert.equal(start.cause, "unknown");
  peer.fault(null, "generation_problem", "response", {
    response_id: "reply-1",
  });
  assert.equal((await payload(events, "error")).error.scope, "response");
  const cancel = watch(session.cancel(start.response));
  await peer.next("response.cancel");
  peer.ended("reply-1", "future_status");
  await cancel;
  assert.equal((await payload(events, "response.ended")).status, "unknown");
  peer.fault(null, "new_error", "future_scope");
  assert.equal((await payload(events, "error")).error.scope, "unknown");
});

test("transcript events and speech supersede tool admission", async (t) => {
  const { session, peer, events } = await setup(t);
  const { ref } = await round(session, peer, events);
  peer.emit("input_audio_buffer.speech_started", {
    item_id: "speech",
    audio_start_ms: 10,
  });
  assert.equal((await payload(events, "speech.started")).audioStartMs, 10);
  await assert.rejects(session.continueReply(ref), RimeInputError);
  peer.emit("input_audio_buffer.speech_stopped", {
    item_id: "speech",
    audio_end_ms: 50,
  });
  assert.equal((await payload(events, "speech.stopped")).audioEndMs, 50);
  peer.emit("input_audio_buffer.committed", { item_id: "speech" });
  assert.equal((await payload(events, "input.committed")).itemId, "speech");
  peer.emit("conversation.item.input_audio_transcription.delta", {
    item_id: "speech",
    delta: "hi",
  });
  assert.equal((await payload(events, "transcript.delta")).delta, "hi");
  peer.emit("conversation.item.input_audio_transcription.completed", {
    item_id: "speech",
    transcript: "hi",
  });
  assert.equal((await payload(events, "transcript.final")).text, "hi");
  peer.emit("conversation.item.input_audio_transcription.failed", {
    item_id: "speech",
    error: { code: "no_transcript", message: "No speech" },
  });
  const failed = await payload(events, "transcript.failed");
  assert.equal(failed.error.scope, "utterance");
});

test("one event consumer, cross-session references, and owner cleanup", async (t) => {
  const { session, peer, client, events } = await setup(t);
  const waiting = events.next();
  await assert.rejects(session.events.next(), RimeInputError);
  peer.emit("prsm.typed_input.ready");
  await waiting;
  await assert.rejects(
    session.cancel({ sessionId: "another", responseId: "reply-1" }),
    RimeInputError,
  );
  const pending = watch(session.addMessage("user", "history"));
  await peer.next("conversation.item.create");
  await client.close();
  await assert.rejects(pending, RimeStreamError);
  assert.equal((await events.next()).done, true);
  await session.close();
  await client.close();
  await assert.rejects(
    client.realtime.connect({ endpoint: peer.endpoint }),
    RimeInputError,
  );
});

for (const mode of [
  "overflow",
  "lost",
  "malformed",
  "oversize",
  "session-fault",
]) {
  test(`session failure: ${mode}`, async (t) => {
    const { session, peer, events } = await setup(t);
    const pending = watch(session.addMessage("user", "history"));
    await peer.next("conversation.item.create");
    if (mode === "overflow")
      for (let i = 0; i < 257; i++) peer.emit("prsm.typed_input.ready");
    if (mode === "lost") peer.socket.terminate();
    if (mode === "malformed") peer.socket.send("{");
    if (mode === "oversize") peer.socket.send(" ".repeat(1024 * 1024 + 1));
    if (mode === "session-fault") peer.fault(null, "fatal", "session");
    const errorClass =
      mode === "overflow"
        ? RimeResourceLimitError
        : mode === "session-fault"
          ? RimeRealtimeError
          : RimeStreamError;
    await assert.rejects(pending, errorClass);
    await assert.rejects(events.next(), errorClass);
  });
}

for (const status of [401, 403, 500, 302]) {
  test(`handshake HTTP ${status} maps to an SDK error without following redirects`, async (t) => {
    const server = createServer((req, res) => {
      res.writeHead(status, { Location: "http://localhost/elsewhere" });
      res.end();
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await new Promise((resolve) => server.close(resolve));
    });
    await assert.rejects(
      client.realtime.connect({
        endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
      }),
      status < 500 && status !== 302
        ? RimeAuthenticationError
        : RimeUnavailableError,
    );
  });
}

test("closing the owner during initialization cancels only the connection", async (t) => {
  const peer = await new Peer().start();
  peer.autoInitialize = false;
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const opening = watch(client.realtime.connect({ endpoint: peer.endpoint }));
  await peer.next("session.update");
  await client.close();
  await assert.rejects(opening, RimeCancelledError);
});

test("input validation rejects invalid configuration before a socket is opened", async () => {
  const client = new Rime({ apiKey: "test" });
  try {
    for (const endpoint of [
      "https://localhost/v1/realtime",
      "ws://example.com/v1/realtime",
      "wss://host/path",
      "wss://user:secret@host/v1/realtime",
      "wss://host/v1/realtime#fragment",
      "invalid",
    ])
      await assert.rejects(
        client.realtime.connect({ endpoint }),
        RimeInputError,
      );
    for (const config of [
      { model: "coda" },
      { voice: " " },
      { interruptOnSpeech: 1 },
      { tools: [tool, tool] },
      { tools: [{ name: "bad", parameters: { x: Infinity } }] },
      { timeouts: { readyS: 0 } },
      { timeouts: { requestS: NaN } },
    ])
      await assert.rejects(
        client.realtime.connect({
          endpoint: "ws://localhost/v1/realtime",
          ...config,
        }),
        RimeInputError,
      );
  } finally {
    await client.close();
  }
});

for (const value of [NaN, Infinity, -1, 0.5, 4294967296, true]) {
  test(`invalid playback interruption ${value}`, async (t) => {
    const { session, peer, events } = await setup(t);
    await turn(session, peer);
    peer.message();
    const output = (await payload(events, "message.started")).output;
    await assert.rejects(
      session.reportPlayback({
        kind: "interrupted",
        output,
        audioEndMs: value,
      }),
      RimeInputError,
    );
  });
}

test("outgoing audio validation, downmixing, resampling, and chunk limit", async (t) => {
  const { session, peer } = await setup(t);
  await assert.rejects(
    session.sendAudio({ data: Buffer.alloc(192002) }),
    RimeAudioFormatError,
  );
  await assert.rejects(
    session.sendAudio({ data: Buffer.alloc(1) }),
    RimeAudioFormatError,
  );
  await assert.rejects(
    session.sendAudio({ data: Buffer.alloc(4), format: { sampleRate: 44100 } }),
    RimeAudioFormatError,
  );
  const stereo = Buffer.alloc(4800 * 4);
  for (let i = 0; i < stereo.length; i += 4) {
    stereo.writeInt16LE(1000, i);
    stereo.writeInt16LE(3000, i + 2);
  }
  await session.sendAudio({
    data: stereo,
    format: { sampleRate: 48000, channels: 2 },
  });
  const parts = [];
  for (let i = 0; i < 3; i++)
    parts.push(
      Buffer.from(
        (await peer.next("input_audio_buffer.append")).audio,
        "base64",
      ),
    );
  assert.deepEqual(
    parts.map((p) => p.length),
    [1280, 1280, 640],
  );
  const data = Buffer.concat(parts);
  for (let i = 0; i < data.length; i += 2)
    assert.equal(data.readInt16LE(i), 2000);
});

// Block the private transport write to make pre/post-submission races deterministic.
function holdWrites(t, kind) {
  const original = RealtimeConnection.prototype.send;
  let unblock, started;
  const waiting = new Promise((resolve) => {
    started = resolve;
  });
  const gate = new Promise((resolve) => {
    unblock = resolve;
  });
  let held = false;
  t.mock.method(RealtimeConnection.prototype, "send", async function (encoded) {
    if (!held && JSON.parse(encoded).type === kind) {
      held = true;
      started();
      await gate;
    }
    return original.call(this, encoded);
  });
  return { waiting, unblock };
}

for (const kind of ["text", "continuation"]) {
  test(`aborted unsent ${kind} restores its reservation`, async (t) => {
    const { session, peer, events } = await setup(t);
    const parent =
      kind === "continuation" ? (await round(session, peer, events)).ref : null;
    const hold = holdWrites(t, "conversation.item.create");
    const blocker = watch(session.addMessage("user", "history"));
    await hold.waiting;
    if (kind === "text") {
      peer.emit("prsm.typed_input.ready");
      await payload(events, "input.ready");
    }
    const controller = new AbortController();
    const operation = watch(
      parent
        ? session.continueReply(parent, { signal: controller.signal })
        : session.sendText("first", { signal: controller.signal }),
    );
    await tick();
    controller.abort();
    await assert.rejects(operation, RimeCancelledError);
    hold.unblock();
    peer.ack(await peer.next("conversation.item.create"));
    await blocker;
    const retry = watch(
      parent ? session.continueReply(parent) : session.sendText("retry"),
    );
    const request = await peer.next("response.create");
    peer.accepted(request, "reply-2");
    await retry;
    assert.equal(peer.queue.length, 0);
  });
}

test("pre-send cancellation does not restore readiness after new speech", async (t) => {
  const { session, peer, events } = await setup(t, {
    timeouts: { readyS: 0.08, requestS: 0.5 },
  });
  const hold = holdWrites(t, "conversation.item.create");
  const blocker = watch(session.addMessage("user", "history"));
  await hold.waiting;
  peer.emit("prsm.typed_input.ready");
  await payload(events, "input.ready");
  const controller = new AbortController();
  const request = watch(
    session.sendText("first", { signal: controller.signal }),
  );
  await tick();
  peer.emit("input_audio_buffer.speech_started", {
    item_id: "speech",
    audio_start_ms: 0,
  });
  await payload(events, "speech.started");
  controller.abort();
  await assert.rejects(request, RimeCancelledError);
  hold.unblock();
  peer.ack(await peer.next("conversation.item.create"));
  await blocker;
  await assert.rejects(session.sendText("retry"), RealtimeAdmissionTimeout);
});

test("operation limit does not consume readiness", async (t) => {
  const { session, peer, events } = await setup(t, {
    timeouts: { requestS: 2, readyS: 1 },
  });
  const pending = Array.from({ length: 128 }, () =>
    watch(session.addMessage("user", "history")),
  );
  const requests = [];
  for (let i = 0; i < 128; i++)
    requests.push(await peer.next("conversation.item.create"));
  peer.emit("prsm.typed_input.ready");
  await payload(events, "input.ready");
  await assert.rejects(session.sendText("first"), RimeResourceLimitError);
  for (const request of requests) peer.ack(request);
  await Promise.all(pending);
  const retry = watch(session.sendText("retry"));
  peer.accepted(await peer.next("response.create"));
  await retry;
});

test("partial audio submission cancellation closes the session", async (t) => {
  const { session, events } = await setup(t);
  const hold = holdWrites(t, "input_audio_buffer.append");
  const controller = new AbortController();
  const result = watch(
    session.sendAudio(
      { data: Buffer.alloc(2560) },
      { signal: controller.signal },
    ),
  );
  await hold.waiting;
  controller.abort();
  await assert.rejects(result, RimeCancelledError);
  await assert.rejects(events.next(), RimeStreamError);
  hold.unblock();
});

const audioVectors = JSON.parse(
  readFileSync(
    new URL("../../conformance/pcm-input.json", import.meta.url),
    "utf8",
  ),
).vectors;
function pcm(samples) {
  const bytes = Buffer.alloc(samples.length * 2);
  samples.forEach((sample, index) => bytes.writeInt16LE(sample, index * 2));
  return bytes;
}
async function audioBarrier(session, peer) {
  const barrier = watch(session.addMessage("user", "audio barrier"));
  const chunks = [];
  for (;;) {
    // The barrier follows all audio writes on the same socket.
    const event =
      peer.queue.shift() ??
      (await new Promise((resolve) => peer.waiters.push(resolve)));
    if (event.type === "conversation.item.create") {
      peer.ack(event);
      break;
    }
    assert.equal(event.type, "input_audio_buffer.append");
    chunks.push(Buffer.from(event.audio, "base64"));
  }
  await barrier;
  return Buffer.concat(chunks);
}
for (const vector of audioVectors) {
  for (const split of [false, true]) {
    test(`Python audio parity ${vector.sampleRate} Hz / ${vector.channels} channels / split=${split}`, async (t) => {
      const { session, peer } = await setup(t);
      const input = pcm(vector.input),
        format = { sampleRate: vector.sampleRate, channels: vector.channels };
      if (split) {
        let offset = 0;
        for (const frames of [1, 3, 7, 21]) {
          const end = offset + frames * vector.channels * 2;
          await session.sendAudio({
            data: input.subarray(offset, end),
            format,
          });
          offset = end;
        }
      } else await session.sendAudio({ data: input, format });
      assert.deepEqual(await audioBarrier(session, peer), pcm(vector.output));
    });
  }
}

for (const mode of [
  "clear",
  "refused-clear",
  "cancelled-clear",
  "format-change",
  "oversize",
]) {
  test(`conversion state after ${mode}`, async (t) => {
    const { session, peer } = await setup(t);
    const vector = audioVectors.find(
      (v) => v.sampleRate === 24000 && v.channels === 1,
    );
    const format = { sampleRate: 24000 },
      input = pcm(vector.input);
    await session.sendAudio({ data: input.subarray(0, 14), format });
    const before = await audioBarrier(session, peer);
    if (mode.includes("clear")) {
      const controller = new AbortController();
      const clear = watch(session.clearAudio({ signal: controller.signal }));
      const request = await peer.next("input_audio_buffer.clear");
      if (mode === "cancelled-clear") controller.abort();
      if (mode === "refused-clear") {
        peer.fault(request, "invalid_client_event");
        await assert.rejects(clear, RimeRealtimeError);
      } else {
        peer.emit("input_audio_buffer.cleared");
        if (mode === "clear") await clear;
        else await assert.rejects(clear, RimeCancelledError);
      }
    } else if (mode === "format-change") {
      await session.sendAudio({
        data: Buffer.alloc(0),
        format: { sampleRate: 16000 },
      });
    } else
      await assert.rejects(
        session.sendAudio({ data: Buffer.alloc(192002), format }),
        RimeAudioFormatError,
      );
    await session.sendAudio({ data: input.subarray(14), format });
    const after = await audioBarrier(session, peer);
    if (["refused-clear", "oversize"].includes(mode))
      assert.deepEqual(Buffer.concat([before, after]), pcm(vector.output));
    else {
      // Reset conversion starts with the first new input sample.
      assert.equal(after.readInt16LE(0), input.readInt16LE(14));
      assert.equal(after.length, 34);
    }
  });
}

for (const changeFormat of [false, true]) {
  test(`audio cancellation before write restores conversion, changed format=${changeFormat}`, async (t) => {
    const { session, peer } = await setup(t);
    const vector = audioVectors.find(
      (v) => v.sampleRate === 24000 && v.channels === 1,
    );
    const input = pcm(vector.input),
      format = { sampleRate: 24000 };
    await session.sendAudio({ data: input.subarray(0, 14), format });
    const before = await audioBarrier(session, peer);
    const hold = holdWrites(t, "conversation.item.create");
    const blocker = watch(session.addMessage("user", "blocked"));
    await hold.waiting;
    const controller = new AbortController();
    const send = watch(
      session.sendAudio(
        {
          data: input.subarray(14, 22),
          format: changeFormat ? { sampleRate: 8000 } : format,
        },
        { signal: controller.signal },
      ),
    );
    await tick();
    controller.abort();
    await assert.rejects(send, RimeCancelledError);
    hold.unblock();
    peer.ack(await peer.next("conversation.item.create"));
    await blocker;
    await session.sendAudio({ data: input.subarray(14), format });
    assert.deepEqual(
      Buffer.concat([before, await audioBarrier(session, peer)]),
      pcm(vector.output),
    );
  });
}

test("aborting a queued audio sender preserves the active sender", async (t) => {
  const { session, peer } = await setup(t);
  const hold = holdWrites(t, "input_audio_buffer.append");
  const active = watch(session.sendAudio({ data: pcm([1, 2, 3]) }));
  await hold.waiting;
  const controller = new AbortController();
  const queued = watch(
    session.sendAudio({ data: pcm([4, 5]) }, { signal: controller.signal }),
  );
  controller.abort();
  await assert.rejects(queued, RimeCancelledError);
  hold.unblock();
  await active;
  assert.deepEqual(await audioBarrier(session, peer), pcm([1, 2, 3]));
});

test("operation limit does not reserve a tool continuation", async (t) => {
  const { session, peer, events } = await setup(t, {
    timeouts: { readyS: 1, requestS: 2 },
  });
  const { ref } = await round(session, peer, events);
  const pending = Array.from({ length: 128 }, () =>
    watch(session.addMessage("user", "history")),
  );
  const requests = [];
  for (let i = 0; i < 128; i++)
    requests.push(await peer.next("conversation.item.create"));
  await assert.rejects(session.continueReply(ref), RimeResourceLimitError);
  for (const request of requests) peer.ack(request);
  await Promise.all(pending);
  const retry = watch(session.continueReply(ref));
  peer.accepted(await peer.next("response.create"), "reply-2");
  await retry;
});

test("submitted cancelled continuation cannot be repeated", async (t) => {
  const { session, peer, events } = await setup(t);
  const { ref } = await round(session, peer, events);
  const controller = new AbortController();
  const continued = watch(
    session.continueReply(ref, { signal: controller.signal }),
  );
  const request = await peer.next("response.create");
  controller.abort();
  await assert.rejects(continued, RimeCancelledError);
  await assert.rejects(session.continueReply(ref), RimeInputError);
  peer.accepted(request, "reply-2");
  await peer.next("response.cancel");
  peer.ended("reply-2", "cancelled");
});

for (const refusal of [
  "tool_continuation_not_ready",
  "tool_continuation_unavailable",
]) {
  test(`cancelled continuation handles late ${refusal}`, async (t) => {
    const { session, peer, events } = await setup(t);
    const { ref } = await round(session, peer, events);
    const controller = new AbortController();
    const continuation = watch(
      session.continueReply(ref, { signal: controller.signal }),
    );
    const request = await peer.next("response.create");
    controller.abort();
    await assert.rejects(continuation, RimeCancelledError);
    peer.fault(request, refusal);
    await payload(events, "error");
    // A subsequent acknowledgment confirms the preceding refusal was handled.
    const barrier = watch(session.addMessage("user", "history"));
    peer.ack(await peer.next("conversation.item.create"));
    await barrier;
    if (refusal === "tool_continuation_not_ready") {
      const retry = watch(session.continueReply(ref));
      const retried = await peer.next("response.create");
      assert.notEqual(retried.event_id, request.event_id);
      peer.accepted(retried, "reply-2");
      assert.equal((await retry).responseId, "reply-2");
    } else {
      await assert.rejects(session.continueReply(ref), /already continued/);
    }
  });
}

test("duplicate tool and terminal events are emitted once; unknown events are ignored", async (t) => {
  const { session, peer, events } = await setup(t);
  await turn(session, peer);
  peer.tool();
  peer.tool();
  peer.ended();
  peer.ended();
  peer.emit("future.event", { anything: true });
  peer.emit("prsm.typed_input.ready");
  const kinds = [];
  for (;;) {
    const kind = (await events.next()).value.payload.kind;
    kinds.push(kind);
    if (kind === "input.ready" && kinds.includes("response.ended")) break;
  }
  assert.equal(kinds.filter((kind) => kind === "tool.call").length, 1);
  assert.equal(kinds.filter((kind) => kind === "response.ended").length, 1);
});

test("old references expire after 128 responses", async (t) => {
  const { session, peer, events } = await setup(t);
  let first;
  for (let i = 0; i < 129; i++) {
    peer.emit("response.created", { response: { id: `response-${i}` } });
    const ref = (await payload(events, "response.started")).response;
    first ??= ref;
    peer.ended(ref.responseId);
    await payload(events, "response.ended");
  }
  await assert.rejects(session.cancel(first), RimeInputError);
});

for (const mode of [
  "bad-base64",
  "odd-audio",
  "content-index",
  "nonobject-tool",
  "reused-call",
]) {
  test(`invalid server payload: ${mode}`, async (t) => {
    const { session, peer, events } = await setup(t);
    await turn(session, peer);
    await payload(events, "response.started");
    if (mode === "nonobject-tool") peer.tool("reply-1", "call-1", "[]");
    else if (mode === "reused-call") {
      peer.tool();
      await payload(events, "tool.call");
      peer.emit("response.created", { response: { id: "reply-2" } });
      await payload(events, "response.started");
      peer.tool("reply-2", "call-1");
    } else
      peer.emit("response.audio.delta", {
        response_id: "reply-1",
        item_id: "message-1",
        output_index: 0,
        content_index: mode === "content-index" ? 1 : 0,
        delta: mode === "bad-base64" ? "??" : "AA==",
      });
    await assert.rejects(events.next(), RimeStreamError);
  });
}

for (const phase of ["handshake", "created", "updated"]) {
  test(`connection timeout during ${phase} releases the socket`, async (t) => {
    const client = new Rime({ apiKey: "test" });
    let endpoint;
    if (phase === "handshake") {
      const server = createServer();
      const sockets = new Set();
      server.on("connection", (socket) => {
        sockets.add(socket);
        socket.on("close", () => sockets.delete(socket));
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      endpoint = `ws://127.0.0.1:${server.address().port}/v1/realtime`;
      t.after(async () => {
        await client.close();
        for (const socket of sockets) socket.destroy();
        await new Promise((resolve) => server.close(resolve));
      });
    } else {
      const peer = await new Peer().start();
      peer.autoCreate = phase !== "created";
      peer.autoInitialize = false;
      endpoint = peer.endpoint;
      t.after(async () => {
        await client.close();
        await peer.close();
      });
    }
    await assert.rejects(
      client.realtime.connect({
        endpoint,
        timeouts: { connectS: 0.03, requestS: 0.03 },
      }),
      RimeTimeoutError,
    );
  });
}

test("connect signal applies to setup only; tool schemas are copied before awaiting", async (t) => {
  const peer = await new Peer().start(),
    client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const controller = new AbortController();
  const parameters = {
    type: "object",
    properties: { order_id: { type: "string" } },
  };
  const connecting = client.realtime.connect({
    endpoint: peer.endpoint,
    signal: controller.signal,
    tools: [{ name: "lookup_order", parameters }],
  });
  parameters.properties.order_id.type = "number";
  const session = await connecting;
  assert.equal(
    peer.settings.tools[0].function.parameters.properties.order_id.type,
    "string",
  );
  controller.abort();
  await turn(session, peer);
});

test("aborting connection setup closes the pending session", async (t) => {
  const peer = await new Peer().start();
  peer.autoInitialize = false;
  const client = new Rime({ apiKey: "test" }),
    controller = new AbortController();
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const connecting = watch(
    client.realtime.connect({
      endpoint: peer.endpoint,
      signal: controller.signal,
    }),
  );
  await peer.next("session.update");
  controller.abort();
  await assert.rejects(connecting, RimeCancelledError);
});

test("unavailable local endpoint raises RimeUnavailableError", async () => {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const endpoint = `ws://127.0.0.1:${server.address().port}/v1/realtime`;
  await new Promise((resolve) => server.close(resolve));
  const client = new Rime({ apiKey: "test" });
  try {
    await assert.rejects(
      client.realtime.connect({ endpoint }),
      RimeUnavailableError,
    );
  } finally {
    await client.close();
  }
});

test("very large finite deadlines do not overflow Node timers", async (t) => {
  const { session, peer } = await setup(t, {
    timeouts: { connectS: 1e10, requestS: 1e10, readyS: 1e10 },
  });
  await turn(session, peer);
});

test("readiness waiters are released on cancellation and can wait again", async () => {
  const { Flag } = await import("../dist/realtime/async.js");
  const { getEventListeners } = await import("node:events");
  const flag = new Flag();
  for (let i = 0; i < 100; i++) {
    const controller = new AbortController();
    const result = watch(flag.wait(controller.signal));
    controller.abort(new Error("cancelled"));
    await assert.rejects(result, /cancelled/);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  }
  const controller = new AbortController(),
    result = flag.wait(controller.signal);
  flag.set();
  await result;
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  flag.clear();
  assert.equal(flag.isSet, false);
});
