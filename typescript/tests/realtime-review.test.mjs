import { test as nativeTest } from "node:test";
import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  Rime,
  RimeInputError,
  RimeStreamError,
  RimeResourceLimitError,
  RimeCancelledError,
} from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";
import { RealtimeConnection } from "../dist/realtime/transport.js";

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

test("review callback runs without response.created, an event consumer, or a success ACK", async (t) => {
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

test("active review IDs stay deduplicated after completed request eviction", async (t) => {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  let calls = 0;
  const { session, peer } = await setup(t, {
    timeoutMs: 5000,
    callback: async (chunk, signal) => {
      if (chunk.reviewId === "held-review") {
        calls++;
        entered.resolve();
        await Promise.race([release.promise, stopped(signal)]);
      }
      return { action: "allow" };
    },
  });
  request(peer, "held-review");
  await entered.promise;
  try {
    for (let index = 1; index <= 257; index++) {
      request(peer, `completed-${index}`, index);
      assert.equal(
        (await peer.next("prism.review.result")).review_id,
        `completed-${index}`,
      );
    }
    request(peer, "held-review");
    await session.update({ instructions: "Reader barrier" });
    assert.equal(calls, 1);
    release.resolve();
    assert.equal(
      (await peer.next("prism.review.result")).review_id,
      "held-review",
    );
    await session.update({ instructions: "Submission barrier" });
    assert.equal(
      peer.requests.filter(
        (value) =>
          value.type === "prism.review.result" &&
          value.review_id === "held-review",
      ).length,
      1,
    );
  } finally {
    release.resolve();
  }
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

for (const cancelBeforeWrite of [false, true]) {
  const name = cancelBeforeWrite
    ? "cancelling a queued response.cancel leaves its reviews active"
    : "writing response.cancel aborts reviews before response.done";
  test(name, async (t) => {
    const entered = Promise.withResolvers();
    const releaseCallback = Promise.withResolvers();
    let callbackSignal;
    const { session, peer } = await setup(t, {
      timeoutMs: 5000,
      callback: async (_chunk, signal) => {
        callbackSignal = signal;
        entered.resolve();
        await Promise.race([releaseCallback.promise, stopped(signal)]);
        return { action: "allow" };
      },
    });
    peer.emit("prism.typed_input.ready");
    const creating = session.sendText("Hello");
    peer.accepted(await peer.next("response.create"), "reply");
    const response = await creating;
    request(peer, "review-cancelling", 0, "reply");
    await entered.promise;
    const held = Promise.withResolvers();
    const releaseWriter = Promise.withResolvers();
    const original = RealtimeConnection.prototype.send;
    let blocker;
    let cancellation;
    try {
      if (cancelBeforeWrite) {
        t.mock.method(
          RealtimeConnection.prototype,
          "send",
          async function (encoded) {
            if (JSON.parse(encoded).type === "session.update") {
              held.resolve();
              await releaseWriter.promise;
            }
            return original.call(this, encoded);
          },
        );
        blocker = session.update({
          instructions: "Hold the serialized writer",
        });
        blocker.catch(() => {});
        await held.promise;
        const caller = new AbortController();
        cancellation = session.cancel(response, { signal: caller.signal });
        await nextTurn();
        caller.abort();
        await assert.rejects(cancellation, RimeCancelledError);
        assert.equal(callbackSignal.aborted, false);
        releaseWriter.resolve();
        await blocker;
        releaseCallback.resolve();
        assert.equal(
          (await peer.next("prism.review.result")).review_id,
          "review-cancelling",
        );
      } else {
        cancellation = session.cancel(response);
        cancellation.catch(() => {});
        await peer.next("response.cancel");
        await session.update({ instructions: "Cancellation writer barrier" });
        assert.equal(callbackSignal.aborted, true);
      }
    } finally {
      releaseWriter.resolve();
      releaseCallback.resolve();
      peer.ended("reply", "cancelled");
      await cancellation?.catch(() => {});
      await blocker?.catch(() => {});
    }
    await session.update({ instructions: "The session remains usable" });
    assert.equal(
      peer.requests.filter((event) => event.type === "response.cancel").length,
      cancelBeforeWrite ? 0 : 1,
    );
    assert.equal(
      peer.requests.filter((event) => event.type === "prism.review.result")
        .length,
      cancelBeforeWrite ? 1 : 0,
    );
  });
}

for (const ending of ["response completion", "preceding stop"]) {
  test(`${ending} cancels queued review writes without failing the session`, async (t) => {
    const callbackEntered = Promise.withResolvers();
    const { session, peer } = await setup(t, {
      callback: async (chunk) => {
        if (chunk.index === 1) callbackEntered.resolve();
        return {
          action:
            ending === "preceding stop" && chunk.index === 0 ? "stop" : "allow",
        };
      },
    });
    const held = Promise.withResolvers();
    const release = Promise.withResolvers();
    const original = RealtimeConnection.prototype.send;
    let blocked = false;
    t.mock.method(
      RealtimeConnection.prototype,
      "send",
      async function (encoded) {
        const kind = JSON.parse(encoded).type;
        if (
          !blocked &&
          kind ===
            (ending === "preceding stop"
              ? "prism.review.result"
              : "session.update")
        ) {
          blocked = true;
          held.resolve();
          await release.promise;
        }
        return original.call(this, encoded);
      },
    );
    let blocker;
    try {
      if (ending === "response completion") {
        peer.emit("response.created", { response: { id: "reply" } });
        blocker = session.update({
          instructions: "Hold the serialized writer",
        });
        blocker.catch(() => {});
      } else request(peer, "review-first", 0, "reply");
      await held.promise;
      request(peer, "review-queued", 1, "reply");
      await callbackEntered.promise;
      await nextTurn();
      if (ending === "response completion") {
        peer.ended("reply");
        await nextTurn();
      }
      release.resolve();
      if (blocker) await blocker;
      else await peer.next("prism.review.result");
      await session.update({ instructions: "The session remains usable" });
      assert.equal(
        peer.requests.filter(
          (event) =>
            event.type === "prism.review.result" &&
            event.review_id === "review-queued",
        ).length,
        0,
      );
    } finally {
      release.resolve();
      if (blocker) await blocker.catch(() => {});
    }
  });
}

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

test("a discarded later chunk consumes no review capacity", async (t) => {
  const full = Promise.withResolvers();
  let calls = 0;
  const { session, peer } = await setup(t, {
    timeoutMs: 5000,
    callback: async (chunk, signal) => {
      if (chunk.responseId === "stopped-reply") return { action: "stop" };
      if (++calls === 32) full.resolve();
      await stopped(signal);
      return { action: "allow" };
    },
  });
  request(peer, "stop-here", 0, "stopped-reply");
  assert.equal((await peer.next("prism.review.result")).action, "stop");
  for (let index = 0; index < 32; index++)
    request(peer, `pending-${index}`, index, "another-reply");
  await full.promise;
  request(peer, "already-discarded", 1, "stopped-reply");
  await session.update({ instructions: "Reader barrier" });
  assert.equal(calls, 32);
  assert.equal(
    peer.requests.some(
      (value) =>
        value.type === "prism.review.result" &&
        value.review_id === "already-discarded",
    ),
    false,
  );
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
