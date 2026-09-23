import assert from "node:assert/strict";
import net from "node:net";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Rime,
  RimeCancelledError,
  RimeInputError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../dist/index.js";
import { factory, native } from "../dist/native.js";
import { FakeService } from "./service.mjs";

function clientAt(t, target, policy) {
  const previous = factory.create;
  factory.create = (config) =>
    native.NativeClient.testing(config, target, JSON.stringify(policy));
  const client = new Rime({ apiKey: "test" });
  factory.create = previous;
  t.after(() => client.close());
  return client;
}

for (const namespace of ["voices", "languages"]) {
  test(`queued ${namespace} discovery is cancelled on close`, async () => {
    const client = new Rime({ apiKey: "test" });
    const pending = client[namespace].list();
    const cancelled = assert.rejects(pending, RimeCancelledError);
    await Promise.all([client.close(), cancelled]);
    await assert.rejects(client[namespace].list(), RimeInputError);
  });
}

test("concurrent connection attempts include lock waits in their deadlines", async (t) => {
  const reservation = net.createServer();
  await new Promise((resolve) => reservation.listen(0, "127.0.0.1", resolve));
  const target = `127.0.0.1:${reservation.address().port}`;
  await new Promise((resolve) => reservation.close(resolve));
  const client = clientAt(t, target, { connection_timeout: 0.2 });
  const start = performance.now();
  const elapsed = await Promise.all(
    Array.from({ length: 4 }, async () => {
      await assert.rejects(
        client.tts.stream("Hello.").next(),
        RimeTimeoutError,
      );
      return performance.now() - start;
    }),
  );
  assert.ok(Math.max(...elapsed) < 500, `failure times: ${elapsed}`);
});

for (const operation of ["synthesis", "discovery"]) {
  test(`cached connections recover before sending ${operation} once`, async (t) => {
    const first = await new FakeService().start();
    t.after(() => first.close());
    const client = clientAt(t, first.target, {
      connection_timeout: 2,
      first_audio_timeout: 3,
    });
    for await (const chunk of client.tts.stream("First."))
      assert.ok(chunk.length);
    first.close();
    await sleep(100);
    const second = new FakeService();
    t.after(() => second.close());
    const bind = second.server.bindAsync.bind(second.server);
    second.server.bindAsync = (_, credentials, callback) =>
      bind(first.target, credentials, callback);
    const restart = sleep(200).then(() => second.start());
    try {
      if (operation === "discovery") {
        assert.deepEqual(await client.languages.list(), ["en", "de"]);
        assert.equal(second.discoveryCalls, 1);
      } else {
        const chunks = [];
        for await (const chunk of client.tts.stream("After restart."))
          chunks.push(chunk);
        assert.ok(Buffer.concat(chunks).length);
        assert.equal(second.calls.length, 1);
      }
    } finally {
      await restart;
    }
  });
}

for (const failure of ["timeout", "server error"]) {
  test(`failed streams are released without another read: ${failure}`, async (t) => {
    const service = await new FakeService().start();
    t.after(() => service.close());
    service.mode = "partial_error";
    const client = clientAt(t, service.target, { progress_timeout: 2 });
    const stream = client.tts.stream("Hello.", {
      timeout: failure === "timeout" ? 0.15 : null,
    });
    await stream.next();
    if (failure === "server error") service.release();
    await stream.native.waitStopped();
    // Allow the adapter's terminal notification and source cleanup to finish.
    await sleep(100);
    assert.equal(client.streams.size, 0);
    await assert.rejects(
      stream.next(),
      failure === "timeout" ? RimeTimeoutError : RimeUnavailableError,
    );
  });
}
