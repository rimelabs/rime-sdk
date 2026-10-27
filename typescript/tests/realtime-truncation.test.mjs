import { test } from "node:test";
import assert from "node:assert/strict";
import { Rime, RimeInputError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";
const cases = [
  ["auto", "auto"],
  ["disabled", "disabled"],
  [{ retentionRatio: 0.9 }, { type: "retention_ratio", retention_ratio: 0.9 }],
  [{ retentionRatio: 1 }, { type: "retention_ratio", retention_ratio: 1 }],
];
for (const [setting, wire] of [[undefined, undefined], ...cases]) {
  test(`explicit truncation connect and update: ${JSON.stringify(setting)}`, async (t) => {
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    const session = await client.realtime.connect({
      endpoint: peer.endpoint,
      truncation: setting,
    });
    if (setting === undefined)
      assert.equal("truncation" in peer.settings, false);
    else assert.deepEqual(peer.settings.truncation, wire);
    assert.equal(session.info.truncation, null);
    peer.autoInitialize = false;
    const pending = session.update({ truncation: setting });
    const q = await peer.next("session.update");
    assert.deepEqual(
      q.session,
      setting === undefined ? {} : { truncation: wire },
    );
    peer.emit("session.updated", {
      prism_request_event_id: q.event_id,
      session: {
        id: "session-1",
        ...(wire === undefined ? {} : { truncation: wire }),
      },
    });
    assert.deepEqual((await pending).truncation, setting ?? null);
    assert.deepEqual(session.info.truncation, setting ?? null);
    assert.deepEqual(peer.invalid, []);
  });
}
for (const value of [
  null,
  true,
  0.9,
  "unknown",
  {},
  { retentionRatio: 0 },
  { retentionRatio: -1 },
  { retentionRatio: 1.1 },
  { retentionRatio: NaN },
  { retentionRatio: Infinity },
  { retentionRatio: "0.9" },
  { retentionRatio: 0.9, extra: true },
]) {
  test(`invalid truncation does not connect: ${JSON.stringify(value)}`, async () => {
    const client = new Rime({ apiKey: "test" });
    try {
      await assert.rejects(
        client.realtime.connect({ truncation: value }),
        RimeInputError,
      );
    } finally {
      await client.close();
    }
  });
}

for (const summary of [
  null,
  { items_left_out: 12 },
  { items_left_out: 12, first_kept_item_id: "item-kept" },
]) {
  for (const status of ["completed", "cancelled", "failed"]) {
    test(
      `response truncation survives cancel acknowledgment: ${status} ${JSON.stringify(summary)}`,
      { timeout: 4000 },
      async (t) => {
        const peer = await new Peer().start();
        const client = new Rime({ apiKey: "test" });
        t.after(async () => {
          await client.close();
          await peer.close();
        });
        const session = await client.realtime.connect({
          endpoint: peer.endpoint,
        });
        peer.emit("prism.typed_input.ready");
        const accepted = session.sendText("hello");
        peer.accepted(await peer.next("response.create"));
        const response = await accepted;
        const pending = session.cancel(response);
        const q = await peer.next("response.cancel");
        peer.emit("response.done", {
          event_id: "done-event",
          prism_request_event_id: q.event_id,
          response: {
            id: response.responseId,
            status,
            metadata: { prism_drain_token: "drain-1" },
            ...(summary === null ? {} : { prism_truncation: summary }),
          },
        });
        await pending;
        const events = session.events;
        for await (const event of events) {
          if (event.payload.kind !== "response.ended") continue;
          assert.deepEqual(
            event.payload.truncation,
            summary === null
              ? null
              : {
                  itemsLeftOut: 12,
                  firstKeptItemId: summary.first_kept_item_id ?? null,
                },
          );
          assert.equal(event.payload.status, status);
          assert.equal(event.eventId, "done-event");
          assert.equal(event.requestId, q.event_id);
          if (summary !== null)
            assert.ok(Object.isFrozen(event.payload.truncation));
          return;
        }
        assert.fail("missing response completion");
      },
    );
  }
}
