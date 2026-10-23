import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Rime, RimeRealtimeError } from "../dist/index.js";
import { decode } from "../dist/realtime/protocol.js";
import { Peer } from "./realtime-peer.mjs";

const cases = JSON.parse(
  readFileSync(
    new URL("../../conformance/prism/error-retry.json", import.meta.url),
    "utf8",
  ),
);
const overflow =
  "the prompt is 35401 tokens; with 512 for the reply it is longer than the context of 32768";
const previousFault = {
  code: "unavailable",
  message: "Service unavailable",
  scope: "response",
  requestId: "request-1",
  responseId: "response-1",
  itemId: "item-1",
  callIds: ["call-1"],
  correlationId: "diagnostic-1",
  parameter: "response.prism_instruction",
};

async function setup(t) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    timeouts: { readyS: 1, requestS: 1 },
  });
  return { session, peer };
}

async function nextFault(events) {
  for (;;) {
    const { value, done } = await events.next();
    if (done) throw new Error("No fault event");
    if (value.payload.kind === "error") return value.payload.error;
  }
}

for (const item of cases) {
  test(`caller-created fault classification comes from the code: ${item.code || "empty code"}`, () => {
    for (const supplied of [{}, { retryable: !item.retryable }]) {
      const input = { ...previousFault, code: item.code, ...supplied };
      const error = new RimeRealtimeError(input);
      assert.deepEqual(error.fault, { ...input, retryable: item.retryable });
      assert.equal(error.message, `${item.code}: ${input.message}`);
      assert.equal(error.requestId, input.requestId);
    }
  });

  test(`retry classification comes from the code: ${item.code || "empty code"}`, () => {
    const event = decode(
      JSON.stringify({
        type: "error",
        error: { code: item.code, retryable: !item.retryable },
      }),
      "session-1",
    );
    assert.equal(event.payload.kind, "error");
    const fault = event.payload.error;
    assert.equal(fault.code, item.code);
    assert.equal(fault.retryable, item.retryable);
    assert.equal(new RimeRealtimeError(fault).fault.retryable, item.retryable);
  });

  if (
    item.retryable ||
    ["context_length_exceeded", "future_error"].includes(item.code)
  ) {
    test(`response fault preserves details without replay: ${item.code}`, async (t) => {
      const { session, peer } = await setup(t);
      const events = session.events;
      peer.emit("prism.typed_input.ready");
      const pending = session.sendText("hello");
      pending.catch(() => {});
      peer.accepted(await peer.next("response.create"));
      const response = await pending;
      const message =
        item.code === "context_length_exceeded" ? overflow : "reply failed";
      peer.emit("error", {
        error: {
          code: item.code,
          message,
          scope: "response",
          owner: { kind: "response", response_id: response.responseId },
          correlation_id: "diagnostic-1",
        },
      });
      const fault = await nextFault(events);
      assert.equal(fault.code, item.code);
      assert.equal(fault.retryable, item.retryable);
      assert.equal(fault.message, message);
      assert.equal(fault.scope, "response");
      assert.equal(fault.responseId, response.responseId);
      assert.equal(fault.correlationId, "diagnostic-1");
      peer.ended(response.responseId, "failed");
      assert.equal((await events.next()).value.payload.kind, "response.ended");
      peer.emit("prism.typed_input.ready");
      assert.equal((await events.next()).value.payload.kind, "input.ready");
      await session.close();
      assert.equal(peer.queue.length, 0);
    });
  }
}

test("copied faults derive classification after a code change", () => {
  const event = decode(
    JSON.stringify({ type: "error", error: { code: "unavailable" } }),
    "session-1",
  );
  const error = new RimeRealtimeError({
    ...event.payload.error,
    code: "context_length_exceeded",
  });
  assert.equal(error.fault.retryable, false);
  assert.equal(event.payload.error.retryable, true);
});

test("constructed faults preserve inherited getters", () => {
  class CallerFault {}
  for (const [field, value] of Object.entries(previousFault)) {
    Object.defineProperty(CallerFault.prototype, field, { get: () => value });
  }
  const input = new CallerFault();
  const error = new RimeRealtimeError(input);
  assert.deepEqual(error.fault, { ...previousFault, retryable: true });
  assert.equal(error.message, "unavailable: Service unavailable");
  assert.equal(error.requestId, previousFault.requestId);
  assert.ok(Object.isFrozen(error.fault));
  assert.ok(Object.isFrozen(error.fault.callIds));
  assert.notEqual(error.fault.callIds, input.callIds);
});

test("constructed faults preserve non-enumerable Error messages", () => {
  const input = Object.assign(new Error(previousFault.message), previousFault);
  const error = new RimeRealtimeError(input);
  assert.deepEqual(error.fault, { ...previousFault, retryable: true });
  assert.equal(error.message, "unavailable: Service unavailable");
  assert.equal(error.requestId, previousFault.requestId);
  assert.ok(Object.isFrozen(error.fault));
  assert.ok(Object.isFrozen(error.fault.callIds));
  assert.notEqual(error.fault.callIds, input.callIds);
});

test("constructed faults are immutable and independent of caller changes", () => {
  const input = { ...previousFault, callIds: [...previousFault.callIds] };
  const error = new RimeRealtimeError(input);
  input.code = "context_length_exceeded";
  input.retryable = false;
  input.callIds.push("call-2");
  assert.deepEqual(error.fault, { ...previousFault, retryable: true });
  assert.ok(Object.isFrozen(error.fault));
  assert.ok(Object.isFrozen(error.fault.callIds));
  assert.throws(() => {
    error.fault.code = "context_length_exceeded";
  }, TypeError);
  assert.throws(() => {
    error.fault.retryable = false;
  }, TypeError);
  assert.throws(() => error.fault.callIds.push("call-3"), TypeError);
});

test("request error exposes the same fault as the event", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prism.typed_input.ready");
  const pending = session.requestReply({ instruction: "Say hello" });
  pending.catch(() => {});
  const request = await peer.next("response.create");
  peer.emit("error", {
    error: {
      code: "invalid_instruction",
      message: "instruction refused",
      scope: "event",
      owner: { kind: "event", event_id: request.event_id },
      param: "response.prism_instruction",
      correlation_id: "diagnostic-1",
    },
  });
  let rejected;
  await assert.rejects(pending, (error) => {
    assert.ok(error instanceof RimeRealtimeError);
    rejected = error;
    return true;
  });
  const fault = await nextFault(session.events);
  assert.equal(rejected.fault, fault);
  assert.equal(fault.retryable, false);
  assert.equal(fault.code, "invalid_instruction");
  assert.equal(fault.message, "instruction refused");
  assert.equal(fault.requestId, request.event_id);
  assert.equal(fault.parameter, "response.prism_instruction");
  assert.equal(fault.correlationId, "diagnostic-1");
  assert.equal(peer.queue.length, 0);
});
