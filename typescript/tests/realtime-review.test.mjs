import { test as nativeTest } from "node:test";
import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  Rime,
  RimeInputError,
  RimeStreamError,
  RimeResourceLimitError,
} from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

const test = (name, body) => nativeTest(name, { timeout: 5000 }, body);

async function setup(t, review) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    review,
    timeouts: { requestS: 0.5 },
  });
  return { session, peer, client };
}
function request(peer, id, index = 0, response = "provisional") {
  peer.emit("prism.review.requested", {
    review_id: id,
    response_id: response,
    item_id: "message-1",
    index,
    text: `chunk ${index}`,
  });
}
function stopped(signal) {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) =>
    signal.addEventListener("abort", resolve, { once: true }),
  );
}

test("review callback runs before response.created without an event consumer or success ACK", async (t) => {
  const calls = [];
  const { session, peer } = await setup(t, {
    callback: async (chunk) => {
      calls.push(chunk);
      return { action: "allow" };
    },
  });
  assert.deepEqual(peer.settings.prism_review, {
    timeout_ms: 1000,
    on_timeout: "stop",
  });
  request(peer, "review-1");
  const result = await peer.next("prism.review.result");
  assert.equal(result.review_id, "review-1");
  assert.equal(result.action, "allow");
  assert.ok(result.event_id);
  assert.deepEqual(calls, [
    {
      reviewId: "review-1",
      responseId: "provisional",
      itemId: "message-1",
      index: 0,
      text: "chunk 0",
    },
  ]);
  assert.equal(Object.isFrozen(calls[0]), true);
  request(peer, "review-1");
  await session.update({ instructions: "A reader barrier" });
  assert.equal(calls.length, 1);
  // No ACK should be required after more than the normal request deadline.
  await new Promise((resolve) => setTimeout(resolve, 550));
  await session.update({ instructions: "Still connected" });
});

test("pipelined review replacement cancels later callbacks but preserves earlier ones", async (t) => {
  const ready = [
    Promise.withResolvers(),
    Promise.withResolvers(),
    Promise.withResolvers(),
  ];
  const decisions = [Promise.withResolvers(), Promise.withResolvers()];
  const laterStopped = Promise.withResolvers();
  const { peer, session } = await setup(t, {
    callback: async (chunk, signal) => {
      ready[chunk.index].resolve();
      if (chunk.index === 2) {
        await stopped(signal);
        laterStopped.resolve();
        return { action: "allow" };
      }
      return Promise.race([
        decisions[chunk.index].promise,
        stopped(signal).then(() => ({ action: "stop" })),
      ]);
    },
  });
  for (let i = 0; i < 3; i++) request(peer, `review-${i}`, i);
  await Promise.all(ready.map((v) => v.promise));
  decisions[1].resolve({ action: "replace", text: "Approved replacement" });
  const replacement = await peer.next("prism.review.result");
  assert.equal(replacement.review_id, "review-1");
  assert.equal(replacement.text, "Approved replacement");
  await laterStopped.promise;
  decisions[0].resolve({ action: "allow" });
  assert.equal((await peer.next("prism.review.result")).review_id, "review-0");
  await session.update({ instructions: "barrier" });
  assert.equal(
    peer.requests.filter((v) => v.type === "prism.review.result").length,
    2,
  );
});

test("expiry aborts provisional callback work and discards a late decision", async (t) => {
  const expired = Promise.withResolvers();
  const { session, peer } = await setup(t, {
    timeoutMs: 100,
    onTimeout: { action: "speak" },
    callback: async (_chunk, signal) => {
      await stopped(signal);
      expired.resolve();
      return { action: "allow" };
    },
  });
  request(peer, "review-expiring");
  await expired.promise;
  await session.update({ instructions: "barrier" });
  assert.equal(
    peer.requests.filter((v) => v.type === "prism.review.result").length,
    0,
  );
});

test("close aborts and joins cooperative callback work with no terminal response", async (t) => {
  const entered = Promise.withResolvers();
  let finished = false;
  const { session, peer } = await setup(t, {
    callback: async (_chunk, signal) => {
      entered.resolve();
      await stopped(signal);
      finished = true;
      return { action: "allow" };
    },
  });
  request(peer, "review-closing");
  await entered.promise;
  await session.close();
  assert.equal(finished, true);
  assert.equal(
    peer.requests.filter((v) => v.type === "prism.review.result").length,
    0,
  );
});

test("response completion cancels its outstanding review work", async (t) => {
  const entered = Promise.withResolvers();
  const cancelled = Promise.withResolvers();
  const { session, peer } = await setup(t, {
    callback: async (_chunk, signal) => {
      entered.resolve();
      await stopped(signal);
      cancelled.resolve();
      return { action: "allow" };
    },
  });
  peer.emit("response.created", { response: { id: "reply" } });
  request(peer, "review-ending", 0, "reply");
  await entered.promise;
  peer.ended("reply");
  await cancelled.promise;
  await session.update({ instructions: "barrier" });
  assert.equal(
    peer.requests.filter((v) => v.type === "prism.review.result").length,
    0,
  );
});

test("client close joins callback cleanup after transport failure", async (t) => {
  const entered = Promise.withResolvers();
  const cancelled = Promise.withResolvers();
  const cleanup = Promise.withResolvers();
  const { client, peer } = await setup(t, {
    callback: async (_chunk, signal) => {
      entered.resolve();
      await stopped(signal);
      cancelled.resolve();
      await cleanup.promise;
      return { action: "allow" };
    },
  });
  request(peer, "review-disconnected");
  await entered.promise;
  peer.socket.terminate();
  await cancelled.promise;
  await nextTurn();
  let closed = false;
  const closing = client.close().then(() => {
    closed = true;
  });
  try {
    await nextTurn();
    assert.equal(closed, false);
  } finally {
    cleanup.resolve();
    await closing;
  }
});

for (const [name, callback] of [
  [
    "exception",
    async () => {
      throw new Error("private policy error");
    },
  ],
  ["invalid action", async () => ({ action: "unknown" })],
  ["invalid replacement", async () => ({ action: "replace", text: "" })],
]) {
  test(`callback ${name} submits stop and reports a typed failure`, async (t) => {
    const { session, peer } = await setup(t, { callback });
    const failure = assert.rejects(session.events.next(), RimeStreamError);
    request(peer, "review-bad");
    const result = await peer.next("prism.review.result");
    assert.equal(result.action, "stop");
    await failure;
  });
}

test("review callback concurrency is bounded and excess work fails the session", async (t) => {
  const ready = Promise.withResolvers();
  let entered = 0,
    finished = 0;
  const { session, peer } = await setup(t, {
    timeoutMs: 5000,
    callback: async (_chunk, signal) => {
      if (++entered === 32) ready.resolve();
      await stopped(signal);
      finished++;
      return { action: "allow" };
    },
  });
  for (let i = 0; i < 32; i++) request(peer, `review-${i}`, i);
  await ready.promise;
  const failure = assert.rejects(session.events.next(), RimeResourceLimitError);
  request(peer, "review-overflow", 32);
  await failure;
  await session.close();
  assert.equal(entered, 32);
  assert.equal(finished, 32);
});

test("review timeout policy is copied and validated before connection", async (t) => {
  const { client, peer } = await setup(t, undefined);
  for (const review of [
    null,
    {},
    { callback: null },
    { callback: async () => ({ action: "allow" }), timeoutMs: 99 },
    {
      callback: async () => ({ action: "allow" }),
      onTimeout: { action: "say", text: "" },
    },
    {
      callback: async () => ({ action: "allow" }),
      onTimeout: { action: "stop", text: "unexpected" },
    },
  ])
    await assert.rejects(
      client.realtime.connect({ endpoint: peer.endpoint, review }),
      RimeInputError,
    );
  const onTimeout = { action: "say", text: "One moment." };
  peer.sessionFields = {
    prism_review: {
      timeout_ms: 1000,
      on_timeout: "say",
      timeout_line: "One moment.",
    },
  };
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    review: { callback: async () => ({ action: "stop" }), onTimeout },
  });
  onTimeout.text = "Changed";
  assert.equal(peer.settings.prism_review.timeout_line, "One moment.");
  assert.deepEqual(session.info.review, {
    timeoutMs: 1000,
    onTimeout: { action: "say", text: "One moment." },
  });
});
