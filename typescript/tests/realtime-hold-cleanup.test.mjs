import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import { Rime, RimeInputError } from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

async function observed(peer, events, id) {
  peer.emit("response.created", { response: { id } });
  const { response } = (await events.next()).value.payload;
  peer.ended(id);
  assert.equal((await events.next()).value.payload.kind, "response.ended");
  return response;
}

async function releasedHold(session, peer, events) {
  const response = await observed(peer, events, "released");
  const hold = session.holdResponse(response);
  hold.release();
  hold.release();
  for (let i = 0; i < 140; i++) await observed(peer, events, `later-${i}`);
  assert.throws(() => session.holdResponse(response), RimeInputError);
  return { hold, response: new WeakRef(response) };
}

async function closedHolds() {
  const peer = await new Peer().start();
  const client = new Rime({ apiKey: "test" });
  try {
    const session = await client.realtime.connect({ endpoint: peer.endpoint });
    const response = await observed(peer, session.events, "closed");
    const holds = [
      session.holdResponse(response),
      session.holdResponse(response),
    ];
    await session.close();
    assert.equal(peer.requests.length, 1, "closing holds sends no writes");
    return {
      holds,
      session: new WeakRef(session),
      response: new WeakRef(response),
    };
  } finally {
    await client.close();
    await peer.close();
  }
}

async function assertCollected(reference, message) {
  for (let i = 0; i < 50; i++) {
    await sleep(10);
    global.gc();
    if (reference.deref() === undefined) return;
  }
  assert.fail(message);
}

test(
  "retained hold handles detach response and session ownership",
  { timeout: 15000 },
  async () => {
    if (!global.gc) {
      const env = { ...process.env };
      delete env.NODE_TEST_CONTEXT;
      await promisify(execFile)(
        process.execPath,
        ["--expose-gc", "--test", fileURLToPath(import.meta.url)],
        { timeout: 12000, env },
      );
      return;
    }
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    try {
      const session = await client.realtime.connect({
        endpoint: peer.endpoint,
      });
      const released = await releasedHold(session, peer, session.events);
      await assertCollected(
        released.response,
        "released hold retained expired response metadata",
      );
      released.hold.release();
      assert.equal(peer.requests.length, 1, "releasing holds sends no writes");
    } finally {
      await client.close();
      await peer.close();
    }
    const closed = await closedHolds();
    await assertCollected(
      closed.session,
      "outstanding holds retained the closed session",
    );
    await assertCollected(
      closed.response,
      "outstanding holds retained closed response metadata",
    );
    for (const hold of closed.holds) {
      hold.release();
      hold.release();
    }
  },
);
