import { test } from "node:test";
import assert from "node:assert/strict";
import { Rime, RimeInputError, RimeRealtimeError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

const kubectl = {
  spelling: "kubectl",
  pronunciation: '" k u b . k @ n . % t r\\ oU l',
};

/** Confirms each update with the context terms and custom lexicon the last
 * update named, as Prism does, or refuses every update with `refusal`. */
async function confirmingPeer(t) {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const view = {
    id: "session-1",
    voice: "test",
    context_terms: [],
    custom_lexicon: [],
  };
  peer.autoInitialize = false;
  peer.updates = [];
  peer.refusal = null;
  peer.onRequest = (event) => {
    if (event.type !== "session.update") return;
    peer.updates.push(event.session);
    if (peer.refusal) return peer.fault(event, peer.refusal);
    for (const name of ["context_terms", "custom_lexicon"])
      if (name in event.session) view[name] = event.session[name];
    peer.emit("session.updated", {
      prism_request_event_id: event.event_id,
      session: view,
    });
  };
  return { peer, client };
}

test("context terms and a custom lexicon are sent and read back", async (t) => {
  const { peer, client } = await confirmingPeer(t);
  const session = await client.realtime.connect({
    endpoint: peer.endpoint,
    contextTerms: ["Skyvale", "Doctor Chen"],
    customLexicon: [kubectl],
  });
  assert.deepEqual(peer.updates[0].context_terms, ["Skyvale", "Doctor Chen"]);
  assert.deepEqual(peer.updates[0].custom_lexicon, [kubectl]);
  assert.deepEqual(session.info.contextTerms, ["Skyvale", "Doctor Chen"]);
  assert.deepEqual(session.info.customLexicon, [kubectl]);

  let info = await session.update({ contextTerms: ["Velkora"] });
  assert.deepEqual(peer.updates[1], { context_terms: ["Velkora"] });
  assert.deepEqual(info.contextTerms, ["Velkora"]);
  assert.deepEqual(info.customLexicon, [kubectl]);

  info = await session.update({ customLexicon: [] });
  assert.deepEqual(peer.updates[2], { custom_lexicon: [] });
  assert.deepEqual(info.customLexicon, []);
  assert.equal(session.info, info);
});

test("omitted context terms and lexicon stay out of the request", async (t) => {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  t.after(async () => {
    await client.close();
    await peer.close();
  });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  assert.equal("context_terms" in peer.settings, false);
  assert.equal("custom_lexicon" in peer.settings, false);
  // A Prism release without these fields does not publish them.
  assert.equal(session.info.contextTerms, null);
  assert.equal(session.info.customLexicon, null);
});

for (const code of ["invalid_client_event", "invalid_session", "turn_failed"])
  test(`Prism's ${code} reaches the caller unchanged`, async (t) => {
    const { peer, client } = await confirmingPeer(t);
    const session = await client.realtime.connect({
      endpoint: peer.endpoint,
      contextTerms: ["Skyvale"],
    });
    peer.refusal = code;
    await assert.rejects(
      session.update({ contextTerms: ["Velkora"], customLexicon: [kubectl] }),
      (error) =>
        error instanceof RimeRealtimeError &&
        error.fault.code === code &&
        error.fault.scope === "event",
    );
    assert.deepEqual(session.info.contextTerms, ["Skyvale"]);
    assert.deepEqual(session.info.customLexicon, []);
  });

test("malformed values fail before they are sent", async (t) => {
  const { peer, client } = await confirmingPeer(t);
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  for (const options of [
    { contextTerms: "Skyvale" },
    { contextTerms: ["Skyvale", 1] },
    // A hole would otherwise be sent as null.
    { contextTerms: [, "Skyvale"] },
    { contextTerms: ["\ud800"] },
    { customLexicon: [{ spelling: "kubectl" }] },
    { customLexicon: [{ spelling: "\ud800", pronunciation: "k" }] },
  ])
    await assert.rejects(session.update(options), RimeInputError);
  assert.equal(peer.updates.length, 1);
});
