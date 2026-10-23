import { test } from "node:test";
import assert from "node:assert/strict";
import https from "node:https";
import { Rime, RimeInputError, RimeUnavailableError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

for (const filler of [
  undefined,
  { type: "none" },
  { type: "text", text: " One moment. " },
  { type: "auto" },
  { type: "auto", instructions: " Say you are checking. " },
  { type: "text", text: "x" },
  { type: "text", text: "😀".repeat(500) },
  { type: "auto", instructions: "😀".repeat(500) },
  ...[
    "\ufeff",
    "\u001c",
    "\u001d",
    "\u001e",
    "\u001f",
    "\u0085Hi\u0085",
    "\ud800\udc00",
  ].flatMap((value) => [
    { type: "text", text: value },
    { type: "auto", instructions: value },
  ]),
]) {
  test(`tool filler serializes at tool level: ${JSON.stringify(filler)}`, async (t) => {
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    await client.realtime.connect({
      endpoint: peer.endpoint,
      tools: [
        {
          name: "press_digits",
          parameters: { type: "object" },
          prismFiller: filler,
        },
        { name: "other", parameters: {} },
      ],
    });
    const expected = {
      type: "function",
      function: {
        name: "press_digits",
        description: "",
        parameters: { type: "object" },
      },
    };
    if (filler !== undefined) expected.prism_filler = filler;
    assert.deepEqual(peer.settings.tools[0], expected);
    assert.equal("prism_filler" in peer.settings.tools[1], false);
    assert.deepEqual(peer.invalid, []);
  });
}

for (const [prismFiller, expected] of [
  [
    { type: "none", text: undefined, instructions: undefined },
    { type: "none" },
  ],
  [
    { type: "text", text: "Hi", instructions: undefined },
    { type: "text", text: "Hi" },
  ],
  [
    { type: "auto", text: undefined, instructions: undefined },
    { type: "auto" },
  ],
  [
    { type: "auto", text: undefined, instructions: "Check the order." },
    { type: "auto", instructions: "Check the order." },
  ],
]) {
  test(`undefined filler fields are omitted: ${JSON.stringify(expected)}`, async (t) => {
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    await client.realtime.connect({
      endpoint: peer.endpoint,
      tools: [{ name: "lookup", parameters: {}, prismFiller }],
    });
    assert.deepEqual(peer.settings.tools[0].prism_filler, expected);
    assert.deepEqual(peer.invalid, []);
  });
}

test("invalid Unicode filler fails before opening a socket", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const value of [
    "\u0085",
    "\ud800",
    "\udfff",
    "Hi\ud800",
    "\udfffHi",
    "\ud800\ud800",
    "\udfff\udfff",
  ]) {
    for (const prismFiller of [
      { type: "text", text: value },
      { type: "auto", instructions: value },
    ]) {
      await assert.rejects(
        client.realtime.connect({
          tools: [{ name: "lookup", parameters: {}, prismFiller }],
        }),
        RimeInputError,
      );
    }
  }
});

test("invalid tool filler fails before opening a socket", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const filler of [
    null,
    false,
    [],
    "none",
    {},
    { type: "future" },
    { type: "text" },
    { type: "none", text: "Hi" },
    { type: "none", instructions: "Hi" },
    { type: "none", extra: true },
    { type: "none", text: null },
    { type: "none", text: "" },
    { type: "text", text: "Hi", instructions: null },
    { type: "text", text: undefined },
    { type: "auto", text: "Hi" },
    { type: "text", text: "Hi", instructions: "Hi" },
    ...[null, "", " \n\t", 1, [], "😀".repeat(501)].flatMap((value) => [
      { type: "text", text: value },
      { type: "auto", instructions: value },
    ]),
  ]) {
    await assert.rejects(
      client.realtime.connect({
        tools: [{ name: "lookup", parameters: {}, prismFiller: filler }],
      }),
      RimeInputError,
    );
  }
});

test("filler settings are copied before opening a connection", async (t) => {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const filler = { type: "text", text: "Original" };
  const opening = client.realtime.connect({
    endpoint: peer.endpoint,
    tools: [{ name: "lookup", parameters: {}, prismFiller: filler }],
  });
  filler.text = "Changed";
  await opening;
  assert.deepEqual(peer.settings.tools[0].prism_filler, {
    type: "text",
    text: "Original",
  });
  assert.deepEqual(peer.invalid, []);
});

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

for (const [requestId, used] of [
  ["client-7", "client-7"],
  [undefined, "server-made"],
]) {
  test(`request id on the upgrade, and the one Prism used: ${requestId}`, async (t) => {
    const peer = await new Peer().start();
    peer.sessionFields = { request_id: used };
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    const session = await client.realtime.connect({
      endpoint: peer.endpoint,
      requestId,
    });
    assert.equal(peer.requestId, requestId);
    assert.equal(session.info.requestId, used);
  });
}

test("invalid request ids fail before opening a socket", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const requestId of [123, "", "  ", "line\nbreak", "é"])
    await assert.rejects(
      client.realtime.connect({ requestId }),
      RimeInputError,
    );
});

test("response metadata is set on connect, changed mid-call, and shown in info", async (t) => {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const first = { scenario: "refund", step: "1" };
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    responseMetadata: first,
  });
  assert.deepEqual(peer.settings.prism_response_metadata, first);
  assert.deepEqual(session.info.responseMetadata, first);
  const second = { scenario: "refund", step: "2" };
  await session.setResponseMetadata(second);
  assert.deepEqual(peer.settings, { prism_response_metadata: second });
  assert.deepEqual(session.info.responseMetadata, second);
  assert.deepEqual(peer.invalid, []);
  // A server that does not confirm the labels leaves none shown.
  peer.echoResponseMetadata = false;
  await session.setResponseMetadata({ step: "3" });
  assert.equal(session.info.responseMetadata, null);
});

test("invalid response metadata fails before it is sent", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const responseMetadata of [[], "step", { step: Number.NaN }])
    await assert.rejects(
      client.realtime.connect({ responseMetadata }),
      RimeInputError,
    );
  const peer = await new Peer().start();
  t.after(() => peer.close());
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  const sent = peer.requests.length;
  await assert.rejects(session.setResponseMetadata([]), RimeInputError);
  assert.equal(peer.requests.length, sent);
});
