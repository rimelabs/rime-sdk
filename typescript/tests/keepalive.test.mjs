import { test } from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import net from "node:net";
import { EventEmitter, once } from "node:events";
import * as grpc from "@grpc/grpc-js";
import { create, toBinary } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { Rime, RimeInputError, RimeTimeoutError } from "../dist/index.js";
import { transport } from "../dist/tts/transport.js";
import { transport as recognitionTransport } from "../dist/stt/transport.js";

const hostedKeepaliveOptions = {
  "grpc.keepalive_time_ms": 60000,
  "grpc.keepalive_timeout_ms": 20000,
  "grpc.keepalive_permit_without_calls": 1,
};

class Peer {
  server = http2.createServer();
  proxy = net.createServer();
  sockets = new Set();
  events = new EventEmitter();
  stalled = false;
  connections = 0;
  discoveries = 0;
  async start() {
    this.server.on("session", (session) => {
      this.sockets.add(session);
      session.on("error", () => {});
      session.on("close", () => this.sockets.delete(session));
    });
    this.server.on("stream", (stream) => {
      this.discoveries++;
      stream.on("error", () => {});
      stream.respond(
        {
          ":status": 200,
          "content-type": "application/grpc",
          "x-request-id": `discovery-${this.discoveries}`,
        },
        { waitForTrailers: true },
      );
      stream.on("wantTrailers", () =>
        stream.sendTrailers({ "grpc-status": "0" }),
      );
      const body = Buffer.from(
        toBinary(
          schema.GetSupportedLanguagesResponseSchema,
          create(schema.GetSupportedLanguagesResponseSchema, {
            languages: ["en"],
          }),
        ),
      );
      const prefix = Buffer.alloc(5);
      prefix.writeUInt32BE(body.length, 1);
      stream.on("end", () => stream.end(Buffer.concat([prefix, body])));
      stream.resume();
    });
    await new Promise((resolve) => this.server.listen(0, "127.0.0.1", resolve));
    this.proxy.on("connection", (client) => {
      this.connections++;
      const backend = net.connect(this.server.address().port, "127.0.0.1");
      let pending = Buffer.alloc(0),
        preface = true;
      for (const socket of [client, backend]) {
        this.sockets.add(socket);
        socket.on("error", () => {});
        socket.on("close", () => this.sockets.delete(socket));
      }
      client.on("data", (data) => {
        pending = Buffer.concat([pending, data]);
        if (preface && pending.length >= 24) {
          pending = pending.subarray(24);
          preface = false;
        }
        while (!preface && pending.length >= 9) {
          const size = 9 + pending.readUIntBE(0, 3);
          if (pending.length < size) break;
          if (pending[3] === 6 && !(pending[4] & 1))
            this.events.emit("ping", performance.now());
          pending = pending.subarray(size);
        }
        if (!this.stalled) backend.write(data);
      });
      backend.on("data", (data) => {
        if (!this.stalled) client.write(data);
      });
      client.on("close", () => {
        backend.destroy();
        this.events.emit("closed");
      });
      backend.on("close", () => client.destroy());
    });
    await new Promise((resolve) => this.proxy.listen(0, "127.0.0.1", resolve));
    return `127.0.0.1:${this.proxy.address().port}`;
  }
  async close() {
    for (const socket of this.sockets) socket.destroy();
    await Promise.all(
      [this.server, this.proxy].map(
        (server) => new Promise((resolve) => server.close(resolve)),
      ),
    );
  }
}

function route(t, targets, captured = [], keepaliveTiming) {
  const makeClient = transport.makeClient;
  t.mock.method(transport, "makeClient", (target, enabled) => {
    const original = makeClient(target, enabled);
    const options = original.getChannel().internalChannel.getOptions();
    original.close();
    captured.push(options);
    if (keepaliveTiming)
      for (const [key, value] of Object.entries(hostedKeepaliveOptions))
        assert.equal(options[key], value);
    return new grpc.Client(
      targets[target] ?? targets.default,
      grpc.credentials.createInsecure(),
      {
        ...options,
        ...(keepaliveTiming
          ? {
              "grpc.keepalive_time_ms": keepaliveTiming.intervalMs,
              "grpc.keepalive_timeout_ms": keepaliveTiming.timeoutMs,
            }
          : {}),
        "grpc.use_local_subchannel_pool": 1,
      },
    );
  });
}

for (const [configuration, enabled] of [
  [{}, false],
  [{ model: "mistv3" }, false],
  [{ enableTTSKeepalive: true }, true],
  [{ model: "mistv3", enableTTSKeepalive: true }, true],
  [{ endpoint: "coda.api.rime.ai:443", enableTTSKeepalive: true }, true],
  [{ endpoint: "mist.api.rime.ai", enableTTSKeepalive: true }, true],
  [{ endpoint: "CODA.API.RIME.AI:00443", enableTTSKeepalive: true }, true],
  [
    { model: "mistv3", endpoint: "coda.api.rime.ai", enableTTSKeepalive: true },
    true,
  ],
  [{ endpoint: "private.example:443", enableTTSKeepalive: true }, false],
  [{ endpoint: "coda.api.rime.ai.example", enableTTSKeepalive: true }, false],
  [{ endpoint: "mist.api.rime.ai:8443", enableTTSKeepalive: true }, false],
  [{ endpoint: "coda.api.rime.ai:443" }, false],
  [{ endpoint: "mist.api.rime.ai:443" }, false],
]) {
  test(`TTS keepalive scope ${JSON.stringify(configuration)}`, async (t) => {
    const peer = new Peer();
    const target = await peer.start();
    const captured = [];
    route(t, { default: target }, captured);
    const client = new Rime({ apiKey: "test-key", ...configuration });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    assert.equal(captured.length, 0);
    assert.deepEqual(await client.languages.list(), ["en"]);
    for (const [key, value] of Object.entries(hostedKeepaliveOptions))
      assert.equal(captured[0][key], enabled ? value : undefined);
    const recognition = recognitionTransport.makeClient("stt.api.rime.ai:443");
    for (const key of Object.keys(hostedKeepaliveOptions))
      assert.equal(
        recognition.getChannel().internalChannel.getOptions()[key],
        undefined,
      );
    recognition.close();
  });
}

for (const value of [null, 0, 1, "true", [], {}])
  test(`keepalive requires boolean ${JSON.stringify(value)}`, () => {
    assert.throws(
      () => new Rime({ apiKey: "test-key", enableTTSKeepalive: value }),
      RimeInputError,
    );
  });

test(
  "native keepalive reuses healthy connections and retires silent stalls",
  { timeout: 20000 },
  async (t) => {
    const healthy = new Peer(),
      stalled = new Peer();
    const targets = {
      "coda.api.rime.ai:443": await healthy.start(),
      "mist.api.rime.ai:443": await stalled.start(),
    };
    route(t, targets, [], { intervalMs: 1000, timeoutMs: 300 });
    const coda = new Rime({ apiKey: "test-key", enableTTSKeepalive: true });
    const mist = new Rime({
      apiKey: "test-key",
      model: "mistv3",
      enableTTSKeepalive: true,
    });
    t.after(async () => {
      await Promise.all([coda.close(), mist.close()]);
      await Promise.all([healthy.close(), stalled.close()]);
    });
    assert.deepEqual(await coda.languages.list(), ["en"]);
    assert.deepEqual(await mist.languages.list(), ["en"]);
    const started = performance.now();
    stalled.stalled = true;
    const healthyPing = once(healthy.events, "ping");
    const stalledPing = once(stalled.events, "ping");
    await assert.rejects(
      mist.languages.list({ timeout: 0.15 }),
      RimeTimeoutError,
    );
    await Promise.all([
      (async () => {
        const [ping] = await healthyPing;
        assert.ok(ping - started >= 800 && ping - started <= 3000);
        assert.deepEqual(await coda.languages.list(), ["en"]);
        assert.equal(healthy.connections, 1);
        const closed = once(healthy.events, "closed");
        await coda.close();
        await closed;
      })(),
      (async () => {
        const [ping] = await stalledPing;
        assert.ok(ping - started >= 800 && ping - started <= 3000);
        await once(stalled.events, "closed");
        assert.ok(performance.now() - ping >= 200);
        assert.equal(stalled.discoveries, 1);
        stalled.stalled = false;
        assert.deepEqual(await mist.languages.list(), ["en"]);
        assert.equal(stalled.discoveries, 2);
        assert.equal(stalled.connections, 2);
      })(),
    ]);
  },
);
