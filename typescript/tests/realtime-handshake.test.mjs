import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { WebSocketServer } from "ws";
import {
  Rime,
  RimeAuthenticationError,
  RimeResourceLimitError,
  RimeUnavailableError,
  RimeStreamError,
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
            assert.equal(error.statusCode, status);
            assert.equal(error.closeCode, null);
            assert.equal(error.retryAfterSeconds, null);
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

for (const [header, expected] of [
  ["3", 3],
  ["0", 0],
  ["-1", null],
  ["1.5", null],
  ["nan", null],
  ["inf", null],
  ["bad date", null],
  ["9".repeat(400), null],
  [["1", "2"], null],
  ["Wed, 21 Oct 2015 07:28:00 GMT", 0],
  ["Sunday, 06-Nov-94 08:49:37 GMT", 0],
  ["Sun Nov  6 08:49:37 1994", 0],
  ["Thu, 01 Jan 2099 00:00:00", null],
  ["Tue, 01 Jan 2030 00:00:00 +0000", null],
  ["Tue, 01 Jan 2030 00:00:00 UTC", null],
  ["Tue, 01 Jan 2030 00:00:00 GMT trailing", null],
  ["Thu, 31 Feb 2030 00:00:00 GMT", null],
  ["Tue, 01 Jan 2030 24:00:00 GMT", null],
  ["Tue, 01 Jan 2030 00:60:00 GMT", null],
  ["Tue, 01 Jan 2030 00:00:61 GMT", null],
  ["Tue Jan 1 00:00:00 2030", null],
]) {
  test(
    `Retry-After ${JSON.stringify(header).slice(0, 60)}`,
    { timeout: 5000 },
    async (t) => {
      const server = createServer((_req, res) => {
        res.writeHead(503, { "Retry-After": header });
        res.end("busy");
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const client = new Rime({ apiKey: "test" });
      t.after(async () => {
        await client.close();
        await new Promise((resolve) => server.close(resolve));
      });
      await assert.rejects(
        client.realtime.connect({
          endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
        }),
        (error) => {
          assert.equal(error.statusCode, 503);
          assert.equal(error.retryAfterSeconds, expected);
          return true;
        },
      );
    },
  );
}

for (const timezone of ["UTC", "America/Los_Angeles", "Asia/Tokyo"]) {
  for (const format of ["imf", "rfc850", "asctime"]) {
    test(
      `Retry-After ${format} date uses UTC in ${timezone}`,
      { timeout: 5000 },
      async (t) => {
        const previousTimezone = process.env.TZ;
        process.env.TZ = timezone;
        t.after(() => {
          if (previousTimezone === undefined) delete process.env.TZ;
          else process.env.TZ = previousTimezone;
        });
        const server = createServer((_req, res) => {
          const date = new Date(Date.now() + 30000);
          const [weekday, day, month, year, time] = date
            .toUTCString()
            .split(" ");
          const weekdayLong = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
          ][date.getUTCDay()];
          const header =
            format === "imf"
              ? date.toUTCString()
              : format === "rfc850"
                ? `${weekdayLong}, ${day}-${month}-${year.slice(-2)} ${time} GMT`
                : `${weekday.slice(0, -1)} ${month} ${String(date.getUTCDate()).padStart(2, " ")} ${time} ${year}`;
          res.writeHead(503, {
            "Retry-After": header,
          });
          res.end();
        });
        server.listen(0, "127.0.0.1");
        await once(server, "listening");
        const client = new Rime({ apiKey: "test" });
        t.after(async () => {
          await client.close();
          await new Promise((resolve) => server.close(resolve));
        });
        await assert.rejects(
          client.realtime.connect({
            endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
          }),
          (error) => {
            assert.ok(
              error.retryAfterSeconds > 28 && error.retryAfterSeconds <= 30,
            );
            return true;
          },
        );
      },
    );
  }
}

for (const [now, header, expected] of [
  [
    "2026-01-01T00:00:00Z",
    "Tuesday, 01-Jan-75 00:00:00 GMT",
    "2075-01-01T00:00:00Z",
  ],
  [
    "2026-01-01T00:00:00Z",
    "Thursday, 01-Jan-76 00:00:00 GMT",
    "2076-01-01T00:00:00Z",
  ],
  ["2026-01-01T00:00:00Z", "Thursday, 01-Jan-76 00:00:01 GMT", null],
  [
    "2090-01-01T00:00:00Z",
    "Friday, 01-Jan-00 00:00:00 GMT",
    "2100-01-01T00:00:00Z",
  ],
]) {
  test(
    `Retry-After RFC850 year at ${now}: ${header}`,
    { timeout: 5000 },
    async (t) => {
      const fixedNow = Date.parse(now);
      t.mock.method(Date, "now", () => fixedNow);
      const server = createServer((_req, res) => {
        res.writeHead(503, { "Retry-After": header });
        res.end();
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const client = new Rime({ apiKey: "test" });
      t.after(async () => {
        await client.close();
        await new Promise((resolve) => server.close(resolve));
      });
      await assert.rejects(
        client.realtime.connect({
          endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
        }),
        (error) => {
          assert.equal(
            error.retryAfterSeconds,
            expected === null ? 0 : (Date.parse(expected) - fixedNow) / 1000,
          );
          return true;
        },
      );
    },
  );
}

for (const code of [1000, 1001, 1008, 1013, null]) {
  test(`close before initialization ${code}`, { timeout: 5000 }, async (t) => {
    const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
    server.on("connection", (socket) =>
      code === null ? socket.terminate() : socket.close(code),
    );
    await once(server, "listening");
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await new Promise((resolve) => server.close(resolve));
    });
    await assert.rejects(
      client.realtime.connect({
        endpoint: `ws://127.0.0.1:${server.address().port}/v1/realtime`,
      }),
      (error) => {
        assert.ok(error instanceof RimeStreamError);
        assert.equal(error.closeCode, code);
        assert.equal(error.statusCode, null);
        return true;
      },
    );
  });
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
