import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import Ajv from "ajv";
import { parse } from "yaml";
import {
  Rime,
  RimeInputError,
  RimeStreamError,
  RealtimeIdleTimeout,
} from "../dist/index.js";
import { decode } from "../dist/realtime/protocol.js";
import { Peer } from "./realtime-peer.mjs";

const cases = JSON.parse(
  readFileSync(
    new URL("../../conformance/prism/protocol.json", import.meta.url),
    "utf8",
  ),
);
const schemaCases = JSON.parse(
  readFileSync(
    new URL("../../conformance/prism/schema.json", import.meta.url),
    "utf8",
  ),
);
const contract = parse(
  readFileSync(
    new URL(
      "../../conformance/prism/speech_to_speech.asyncapi.yaml",
      import.meta.url,
    ),
    "utf8",
  ),
);
const ajv = new Ajv({ strict: false, validateFormats: false });
for (const item of schemaCases) {
  test(`protocol schema: ${item.name}`, () => {
    const validate = ajv.compile({
      $ref: `#/components/schemas/${item.schema}`,
      components: contract.components,
    });
    assert.equal(validate(item.value), item.valid);
  });
}
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
  peer.emit("prism.typed_input.ready");
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
test("nudge instruction and token usage reach public events", async (t) => {
  const { session, peer } = await setup(t);
  peer.emit("prism.typed_input.ready");
  const pending = session.requestReply({ instruction: "Check in." });
  const request = await peer.next("response.create");
  peer.accepted(request, "reply-1", {
    prism_instruction_item_id: "item-instruction",
  });
  const response = await pending;
  const events = session.events;
  let started;
  do {
    started = (await events.next()).value.payload;
  } while (started.kind !== "response.started");
  assert.deepEqual(started.response, response);
  assert.equal(started.instructionItemId, "item-instruction");
  peer.emit("response.done", {
    response: {
      id: response.responseId,
      status: "completed",
      usage: {
        input_tokens: 10,
        output_tokens: 3,
        total_tokens: 13,
        context_window: 65536,
      },
    },
  });
  assert.deepEqual((await events.next()).value.payload.usage, {
    inputTokens: 10,
    outputTokens: 3,
    totalTokens: 13,
    contextWindow: 65536,
  });
  const deleting = session.deleteItem(started.instructionItemId);
  const deletion = await peer.next("conversation.item.delete");
  assert.equal(deletion.item_id, "item-instruction");
  peer.emit("conversation.item.deleted", {
    prism_request_event_id: deletion.event_id,
    item_id: "item-instruction",
  });
  await deleting;
});
for (const kind of ["text", "nudge"]) {
  test(`${kind} request labels apply to one reply`, async (t) => {
    const { session, peer } = await setup(t);
    peer.emit("prism.typed_input.ready");
    const metadata = { step: "one" };
    const pending =
      kind === "text"
        ? session.sendText("hello", { metadata })
        : session.requestReply({ metadata });
    const request = await peer.next("response.create");
    assert.deepEqual(request.response.metadata, {
      step: "one",
      prism_cause: kind === "text" ? "user_text" : "proactive",
    });
    assert.deepEqual(metadata, { step: "one" });
    peer.accepted(request);
    await pending;
  });
}
test("reserved request labels fail before a write", async (t) => {
  const { session, peer } = await setup(t);
  await assert.rejects(
    session.requestReply({ metadata: { prism_cause: "user_text" } }),
    RimeInputError,
  );
  assert.equal(peer.queue.length, 0);
});
test("idle timeout ends pending work and event reading with its duration", async (t) => {
  const { session, peer } = await setup(t);
  const pending = session.addMessage("user", "history");
  await peer.next("conversation.item.create");
  peer.emit("session.idle_timeout", {
    reason: "idle_timeout",
    idle_timeout_s: 300,
  });
  await assert.rejects(
    pending,
    (error) =>
      error instanceof RealtimeIdleTimeout && error.idleTimeoutS === 300,
  );
  await assert.rejects(
    session.events.next(),
    (error) =>
      error instanceof RealtimeIdleTimeout && error.idleTimeoutS === 300,
  );
});
test("a started reply shows the client's own metadata", () => {
  const started = (metadata) =>
    decode(
      JSON.stringify({
        type: "response.created",
        response: { id: "reply-1", metadata },
      }),
      "session-1",
    ).payload;
  assert.deepEqual(
    started({ prism_cause: "user_text", step: "3", nested: { a: 1 } }).metadata,
    { step: "3" },
  );
  assert.deepEqual(started(undefined).metadata, {});
});
test("a started reply names the user items it answers", () => {
  const started = (metadata, response = {}) =>
    decode(
      JSON.stringify({
        type: "response.created",
        response: { id: "reply-1", metadata, ...response },
      }),
      "session-1",
    ).payload;
  assert.deepEqual(
    started(undefined, { prism_input_item_ids: ["item-1", "item-2"] })
      .inputItemIds,
    ["item-1", "item-2"],
  );
  assert.deepEqual(
    started({ prism_cause: "user_text", prism_input_item_id: "item-3" })
      .inputItemIds,
    ["item-3"],
  );
  assert.deepEqual(started(undefined).inputItemIds, []);
});
for (const item of cases) {
  test(`shared protocol: ${item.name}`, () => {
    const raw = JSON.stringify(item.event);
    if (item.invalid)
      assert.throws(() => decode(raw, "session-1"), RimeStreamError);
    else {
      assert.equal(
        decode(raw, "session-1")?.payload.kind ?? "ignored",
        item.expected_kind,
      );
      if ("expected_kept_text" in item) {
        assert.deepEqual(decode(raw, "session-1").payload, {
          kind: "item.truncated",
          itemId: item.event.item_id,
          contentIndex: item.event.content_index,
          audioEndMs: item.event.audio_end_ms,
          keptText: item.expected_kept_text,
        });
      }
      if ("expected_truncation" in item) {
        const expected = item.expected_truncation;
        assert.deepEqual(
          decode(raw, "session-1").payload.truncation,
          expected === null
            ? null
            : {
                itemsLeftOut: expected.items_left_out,
                firstKeptItemId: expected.first_kept_item_id ?? null,
              },
        );
      }
      if ("expected_usage" in item) {
        const usage = item.expected_usage;
        assert.deepEqual(
          decode(raw, "session-1").payload.usage,
          usage === null
            ? null
            : {
                inputTokens: usage.input_tokens,
                outputTokens: usage.output_tokens,
                totalTokens: usage.total_tokens,
                contextWindow: usage.context_window,
              },
        );
      }
      if ("expected_instruction_item_id" in item)
        assert.equal(
          decode(raw, "session-1").payload.instructionItemId,
          item.expected_instruction_item_id,
        );
      if ("expected_idle_timeout_s" in item)
        assert.equal(
          decode(raw, "session-1").payload.idleTimeoutS,
          item.expected_idle_timeout_s,
        );
    }
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
      peer.emit("prism.typed_input.ready");
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
        prism_request_event_id: request.event_id,
        session: { id: "session-1" },
      });
    else if (acknowledgment === "response.created")
      peer.accepted(request, "wrong-response");
    else if (acknowledgment === "wrong-call")
      peer.emit("conversation.item.created", {
        prism_request_event_id: request.event_id,
        item: { type: "function_call_output", call_id: "another-call" },
      });
    else
      peer.emit(acknowledgment, {
        prism_request_event_id: request.event_id,
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
    prism_request_event_id: request.event_id,
    item: { type: "function_call_output", call_id: call.callId },
  });
  await result;
  peer.ended();
  const continuation = watch(
    session.continueReply(parent, { metadata: { step: "tool" } }),
  );
  const continuationRequest = await peer.next("response.create");
  assert.deepEqual(continuationRequest.response.metadata, {
    step: "tool",
    prism_cause: "tool_continuation",
    prism_parent_response_id: parent.responseId,
  });
  peer.accepted(continuationRequest, "reply-2");
  assert.equal((await continuation).responseId, "reply-2");
});

test("unknown events and stale acknowledgments do not consume a request", async (t) => {
  const { session, peer } = await setup(t);
  const pending = watch(session.addMessage("user", "history"));
  const request = await peer.next("conversation.item.create");
  peer.emit("future.event", {
    prism_request_event_id: request.event_id,
    anything: true,
  });
  peer.emit("session.updated", {
    prism_request_event_id: "old-request",
    session: { id: "session-1" },
  });
  peer.ack(request);
  assert.equal((await pending).itemId, "history-1");
});
