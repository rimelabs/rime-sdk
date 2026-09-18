import { test } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { Rime, RimeInputError } from "../dist/index.js";
import { authentication, Credentials } from "../dist/auth.js";
import { transport } from "../dist/transport.js";
import { policy } from "../dist/policy.js";
import { FakeService } from "./service.mjs";

for (const endpoint of [
  "",
  "https://coda.api.rime.ai",
  "host/path",
  "user@host",
  "host?query",
  "host#tag",
  " host",
  "host\n",
  "host:",
  "host:0",
  "host:65536",
  "host:-1",
  "host:1:2",
  "host:abc",
  "-host",
  "host..name",
  "a".repeat(64),
  42,
]) {
  test(`invalid endpoint ${JSON.stringify(endpoint)}`, () => {
    assert.throws(
      () => new Rime({ apiKey: "test-key", endpoint }),
      RimeInputError,
    );
  });
}

test("custom endpoint does not enable an unknown model", () => {
  assert.throws(
    () =>
      new Rime({
        apiKey: "test-key",
        model: "mist",
        endpoint: "mist.api.rime.ai",
      }),
    RimeInputError,
  );
});

for (const themis of [false, true])
  for (const endpoint of ["Customer.Example", "Customer.Example:8443"])
    test(`independent speech and discovery endpoints: ${endpoint}, Themis=${themis}`, async (t) => {
      const original = { ...policy };
      const standard = await new FakeService().start();
      const custom = await new FakeService().start();
      t.after(() => {
        standard.close();
        custom.close();
      });
      custom.payload = Buffer.from(Array(2400).fill([2, 0]).flat());
      const routes = {
        "coda.api.rime.ai:443": standard.target,
        [`customer.example:${endpoint.includes(":") ? 8443 : 443}`]:
          custom.target,
      };
      const targets = [],
        audiences = [];
      t.mock.method(transport, "makeClient", (target) => {
        targets.push(target);
        return new grpc.Client(
          routes[target],
          grpc.credentials.createInsecure(),
        );
      });
      t.mock.method(
        authentication,
        "exchangeKey",
        async (key, signal, configuration) => {
          assert.equal(configuration.exchangeUrl, original.exchangeUrl);
          audiences.push(configuration.audience);
          return {
            value: key,
            expiresAt: Date.now() / 1000 + 3600,
            audience: configuration.audience,
          };
        },
      );
      if (themis)
        t.mock.method(
          Credentials.prototype,
          "metadata",
          Credentials.prototype.themisMetadata,
        );
      const first = new Rime({ apiKey: "standard-key" });
      const second = new Rime({ apiKey: "custom-key", endpoint });
      t.after(async () => {
        await Promise.all([first.close(), second.close()]);
      });
      async function use(client) {
        const chunks = [];
        for await (const chunk of client.tts.stream("Hello."))
          chunks.push(chunk);
        assert.deepEqual(await client.languages.list(), ["en", "de"]);
        assert.deepEqual(await client.voices.list(), ["test-speaker"]);
        return Buffer.concat(chunks);
      }
      assert.deepEqual(await Promise.all([use(first), use(second)]), [
        standard.payload,
        custom.payload,
      ]);
      assert.deepEqual(targets.sort(), Object.keys(routes).sort());
      assert.deepEqual(standard.metadata[0].get("authorization"), [
        "Bearer standard-key",
      ]);
      assert.deepEqual(custom.metadata[0].get("authorization"), [
        "Bearer custom-key",
      ]);
      assert.equal(standard.discoveryCalls, 1);
      assert.equal(custom.discoveryCalls, 1);
      assert.deepEqual(
        audiences.sort(),
        themis ? ["coda.api.rime.ai", "customer.example"] : [],
      );
      assert.deepEqual(policy, original);
    });
