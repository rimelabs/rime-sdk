import { test } from "node:test";
import assert from "node:assert/strict";
import https from "node:https";
import { Rime, RimeInputError, RimeResourceLimitError } from "../dist/index.js";
import { InterruptionPolicies } from "../dist/realtime/settings.js";
import { Peer } from "./realtime-peer.mjs";

function invalidTools() {
  const cyclic = {};
  cyclic.self = cyclic;
  return [
    ...["\u0085", "\u2003\u0085", "a".repeat(4001), "😀".repeat(4001)].map(
      (name) => ({ name, parameters: {} }),
    ),
    ...["\ud800", "\udfff", "prefix\ud800", "\udfff suffix"].flatMap(
      (value) => [
        { name: value, parameters: {} },
        { name: "lookup", description: value, parameters: {} },
        { name: "lookup", parameters: { properties: { nested: [value] } } },
        { name: "lookup", parameters: { properties: { [value]: {} } } },
      ],
    ),
    { name: "lookup", parameters: cyclic },
  ];
}

test("invalid tool text fails before opening a connection", async (t) => {
  t.mock.method(https, "request", () => assert.fail("Must not open a socket"));
  const client = new Rime({ apiKey: "test" });
  t.after(() => client.close());
  for (const tool of invalidTools())
    await assert.rejects(
      client.realtime.connect({ tools: [tool] }),
      RimeInputError,
    );
});

test("invalid tool updates fail before submission", async (t) => {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  const requests = peer.requests.length;
  for (const tool of invalidTools())
    await assert.rejects(session.update({ tools: [tool] }), RimeInputError);
  assert.equal(peer.requests.length, requests);
});

test("valid Unicode tool text survives connection and update unchanged", async (t) => {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const tools = ["\ufeff", "\u001c", "😀".repeat(4000)].map((name) => ({
    name,
    description: "Description 😀",
    parameters: { properties: { "😀": { enum: ["\ud800\udc00"] } } },
  }));
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    tools,
  });
  const expected = tools.map((tool) => ({ type: "function", function: tool }));
  assert.deepEqual(peer.settings.tools, expected);
  await session.update({ tools });
  assert.deepEqual(peer.settings.tools, expected);
  assert.deepEqual(peer.invalid, []);
});

test("missing revisions preserve known history and reject ambiguous replies", () => {
  const policies = new InterruptionPolicies();
  policies.record(1, true);
  policies.record(100, true);
  policies.record(null, false);
  assert.equal(policies.resolve(null), false);
  // Repeated changes without revisions retain the same unknown interval.
  for (let i = 0; i < 200; i++) policies.record(null, i % 2 === 0);
  policies.record(103, false);
  assert.equal(policies.resolve(1), true);
  assert.equal(policies.resolve(100), true);
  assert.throws(() => policies.resolve(101), RimeResourceLimitError);
  assert.throws(() => policies.resolve(102), RimeResourceLimitError);
  assert.equal(policies.resolve(103), false);
  // Same-policy updates cannot evict the earlier known or unknown intervals.
  for (let revision = 104; revision < 400; revision++)
    policies.record(revision, false);
  assert.equal(policies.resolve(1), true);
  assert.throws(() => policies.resolve(102), RimeResourceLimitError);
  assert.equal(policies.resolve(399), false);
});

test("revisionless servers keep the current setting and recover a known boundary", () => {
  const policies = new InterruptionPolicies();
  policies.record(null, false);
  assert.equal(policies.resolve(undefined), false);
  policies.record(null, true);
  assert.equal(policies.resolve(undefined), true);
  policies.record(4, true);
  assert.throws(() => policies.resolve(3), RimeResourceLimitError);
  assert.equal(policies.resolve(4), true);
});

test("a missing revision without a policy change leaves history known", () => {
  const policies = new InterruptionPolicies();
  policies.record(1, true);
  policies.record(null, true);
  policies.record(500, true);
  assert.equal(policies.resolve(1), true);
  assert.equal(policies.resolve(499), true);
  assert.equal(policies.resolve(500), true);
});
