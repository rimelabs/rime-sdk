import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http2";
import { once } from "node:events";
import { createServer as createTcpServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Rime,
  RimePermissionError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../dist/index.js";
import { factory, native } from "../dist/native.js";
import { FakeService } from "./service.mjs";

for (const phase of ["before", "after"])
  for (const operation of ["stream", "voices", "languages"])
    test(`connection drop ${phase} response headers / ${operation}`, async () => {
      const server = createServer();
      let attempts = 0;
      server.on("stream", (stream) => {
        attempts++;
        const session = stream.session;
        if (phase === "after") {
          stream.respond({
            ":status": 200,
            "content-type": "application/grpc",
          });
          // A ping reply confirms that the client received the response headers.
          session.ping(() => session.destroy());
        } else {
          session.destroy();
        }
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const previous = factory.create;
      factory.create = (config) =>
        native.NativeClient.testing(
          config,
          `127.0.0.1:${server.address().port}`,
          "{}",
        );
      const client = new Rime({ apiKey: "test-key", timeout: 2 });
      try {
        await assert.rejects(async () => {
          if (operation === "stream") {
            for await (const _chunk of client.tts.stream("Hello.")) {
              assert.fail("The server must not send audio");
            }
          } else {
            await client[operation].list();
          }
        }, RimeUnavailableError);
        assert.equal(attempts, operation === "stream" ? 1 : 3);
      } finally {
        await client.close();
        await new Promise((resolve) => server.close(resolve));
        factory.create = previous;
      }
    });

test("connection deadline includes waiting for peer HTTP/2 settings", async () => {
  const sockets = new Set();
  const server = createTcpServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.resume();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const previous = factory.create;
  factory.create = (config) =>
    native.NativeClient.testing(
      config,
      `127.0.0.1:${server.address().port}`,
      JSON.stringify({ connection_timeout: 0.1, first_audio_timeout: 0.6 }),
    );
  const client = new Rime({ apiKey: "test-key" });
  let reads = 0;
  async function* source() {
    reads++;
    yield "Hello.";
  }
  try {
    const start = performance.now();
    await assert.rejects(client.tts.stream(source()).next(), (error) => {
      assert.ok(error instanceof RimeTimeoutError);
      assert.match(error.message, /Connection establishment timed out/);
      return true;
    });
    assert.ok(performance.now() - start < 500);
    assert.equal(reads, 0);
  } finally {
    await client.close();
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
    factory.create = previous;
  }
});

test("connection waits until peer settings allow requests", async () => {
  const server = createServer({ settings: { maxConcurrentStreams: 0 } });
  const sessions = new Set();
  server.on("session", (session) => {
    sessions.add(session);
    session.on("close", () => sessions.delete(session));
  });
  server.on("stream", (stream) => {
    stream.respond(
      {
        ":status": 200,
        "content-type": "application/grpc",
        "grpc-status": "7",
      },
      { endStream: true },
    );
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const previous = factory.create;
  factory.create = (config) =>
    native.NativeClient.testing(
      config,
      `127.0.0.1:${server.address().port}`,
      JSON.stringify({ connection_timeout: 1 }),
    );
  const client = new Rime({ apiKey: "test-key" });
  let reads = 0;
  async function* source() {
    reads++;
    yield "Hello.";
  }
  try {
    const sessionReady = once(server, "session");
    const result = assert.rejects(
      client.tts.stream(source()).next(),
      RimePermissionError,
    );
    const [session] = await sessionReady;
    await once(session, "localSettings");
    await sleep(30);
    assert.equal(reads, 0);
    session.settings({ maxConcurrentStreams: 1 });
    await result;
  } finally {
    await client.close();
    for (const session of sessions) session.destroy();
    await new Promise((resolve) => server.close(resolve));
    factory.create = previous;
  }
});

for (const mode of ["odd_chunks", "headers_after_text"])
  test(`audio and wire order / ${mode}`, async () => {
    const service = await new FakeService().start();
    const previous = factory.create;
    factory.create = (config) =>
      native.NativeClient.testing(config, service.target, "{}");
    const client = new Rime({ apiKey: "test-key" });
    try {
      service.mode = mode;
      const chunks = [];
      for await (const chunk of client.tts.stream("Hello.", {
        voice: "voice",
        language: "de",
      }))
        chunks.push(chunk);
      assert.deepEqual(Buffer.concat(chunks), service.payload);
      assert.equal(service.calls.length, 1);
      assert.deepEqual(
        service.calls[0].map((m) => m.payload.case),
        ["header", "textChunk"],
      );
    } finally {
      await client.close();
      service.close();
      factory.create = previous;
    }
  });
