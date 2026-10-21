import { test } from "node:test";
import assert from "node:assert/strict";
import https from "node:https";
import { Rime, RimeInputError, RimeUnavailableError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

test("omitted endpoint uses public Prism with API key authentication", async (t) => {
  const requests = [];
  t.mock.method(https, "request", (options) => {
    requests.push(options);
    throw new Error("Stop before network access");
  });
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  await assert.rejects(client.realtime.connect(), RimeUnavailableError);
  await assert.rejects(client.realtime.connect({}), RimeUnavailableError);
  assert.equal(requests.length, 2);
  for (const request of requests) {
    assert.equal(request.host, "prism.api.rime.ai");
    assert.equal(request.port, 443);
    assert.equal(request.path, "/v1/realtime");
    assert.equal(request.headers.Authorization, "Bearer test");
  }
});

const cases = [
  [undefined, {}],
  [{}, {}],
  ...["low", "medium", "high", "auto"].map((eagerness) => [
    { eagerness },
    { eagerness },
  ]),
  ...[1000, 10000].map((value) => [
    { endOfTurnTimeoutMs: value },
    { prism_end_of_turn_timeout_ms: value },
  ]),
  [
    { eagerness: "low", endOfTurnTimeoutMs: 2000 },
    { eagerness: "low", prism_end_of_turn_timeout_ms: 2000 },
  ],
];
for (const [turnDetection, wire] of cases) {
  test(`turn overrides and effective server settings: ${JSON.stringify(turnDetection)}`, async (t) => {
    const peer = await new Peer().start();
    peer.autoInitialize = false;
    peer.onRequest = (event) => {
      assert.equal(event.type, "session.update");
      peer.settings = event.session;
      peer.emit("session.updated", {
        prism_request_event_id: event.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prism_effective_interrupt_response: false,
          turn_detection: {
            interrupt_response: false,
            eagerness: "medium",
            prism_end_of_turn_timeout_ms: 3000,
          },
        },
      });
    };
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    const session = await client.realtime.connect({
      endpoint: peer.endpoint,
      interruptOnSpeech: false,
      turnDetection,
    });
    assert.deepEqual(peer.settings.turn_detection, {
      interrupt_response: false,
      ...wire,
    });
    assert.equal("voice" in peer.settings, false);
    assert.equal(session.info.interruptOnSpeech, false);
    assert.deepEqual(session.info.turnDetection, {
      eagerness: "medium",
      endOfTurnTimeoutMs: 3000,
    });
    assert.equal(Object.isFrozen(session.info.turnDetection), true);
  });
}

test("invalid turn settings fail before opening a socket", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const turnDetection of [
    null,
    false,
    [],
    "low",
    ...[null, "future", "", true, 1].map((eagerness) => ({ eagerness })),
    ...[null, 999, 10001, 1000.5, true, "2000", NaN, Infinity].map(
      (endOfTurnTimeoutMs) => ({ endOfTurnTimeoutMs }),
    ),
  ]) {
    await assert.rejects(
      client.realtime.connect({ turnDetection }),
      RimeInputError,
    );
  }
});

for (const eagerness of [undefined, "future"]) {
  test(`unknown or missing effective eagerness: ${eagerness}`, async (t) => {
    const peer = await new Peer().start();
    peer.autoInitialize = false;
    peer.onRequest = (event) =>
      peer.emit("session.updated", {
        prism_request_event_id: event.event_id,
        session: { id: "session-1", turn_detection: { eagerness } },
      });
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    const session = await client.realtime.connect({ endpoint: peer.endpoint });
    assert.deepEqual(session.info.turnDetection, {
      eagerness: eagerness === undefined ? null : "unknown",
      endOfTurnTimeoutMs: null,
    });
  });
}
