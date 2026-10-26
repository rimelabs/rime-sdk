import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import {
  Rime,
  RimeAuthenticationError,
  RimeResourceLimitError,
  RimeUnavailableError,
} from "../dist/index.js";

const cases = [
  [
    401,
    "Authentication failed",
    RimeAuthenticationError,
    "Authentication failed",
  ],
  [403, "Forbidden", RimeAuthenticationError, "Forbidden"],
  [
    429,
    "Currently at websocket limit",
    RimeResourceLimitError,
    "websocket limit",
  ],
  [429, "", RimeResourceLimitError, "connection or rate limit reached"],
  [
    503,
    "conversation capacity reached",
    RimeUnavailableError,
    "capacity reached",
  ],
  [503, "engine is draining", RimeUnavailableError, "engine is draining"],
  [
    503,
    '{"error":{"message":"graph is not ready"}}',
    RimeUnavailableError,
    "graph is not ready",
  ],
  [503, "<html>proxy error</html>", RimeUnavailableError, null],
  [503, '{"error":', RimeUnavailableError, null],
  [503, '{"error":null}', RimeUnavailableError, null],
  [503, Buffer.from([255]), RimeUnavailableError, null],
  [503, "bad\x00message", RimeUnavailableError, null],
  [429, "x".repeat(2048), RimeResourceLimitError, null],
];

for (const [index, [status, body, errorType, detail]] of cases.entries()) {
  test(
    `HTTP refusal ${index}: category, diagnostics, cleanup`,
    { timeout: 5000 },
    async (t) => {
      let attempts = 0;
      const sockets = new Set();
      const server = createServer((_req, res) => {
        attempts++;
        res.writeHead(status, { "x-request-id": "server-request" });
        res.end(body);
      });
      server.on("connection", (socket) => {
        sockets.add(socket);
        socket.once("close", () => sockets.delete(socket));
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const client = new Rime({ apiKey: "test" });
      t.after(async () => {
        await client.close();
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
      });
      for (let attempt = 0; attempt < 2; attempt++) {
        await assert.rejects(
          client.realtime.connect({
            endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
          }),
          (error) => {
            assert.ok(error instanceof errorType);
            assert.ok(error.message.includes(`HTTP ${status}`));
            assert.equal(error.requestId, "server-request");
            if (detail) assert.ok(error.message.includes(detail));
            else assert.ok(!error.message.includes(": "));
            return true;
          },
        );
        await Promise.all([...sockets].map((socket) => once(socket, "close")));
        assert.equal(sockets.size, 0);
      }
      assert.equal(attempts, 2);
    },
  );
}

test(
  "unfinished error body retains HTTP category and closes",
  { timeout: 5000 },
  async (t) => {
    let closed;
    const server = createServer((_req, res) => {
      closed = once(res.socket, "close");
      res.writeHead(429, {
        "Content-Length": 100,
        "x-request-id": "slow-body",
      });
      res.flushHeaders();
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    });
    await assert.rejects(
      client.realtime.connect({
        endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
      }),
      (error) =>
        error instanceof RimeResourceLimitError &&
        error.message.includes("HTTP 429") &&
        error.requestId === "slow-body",
    );
    await closed;
  },
);
