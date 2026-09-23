import { factory, native } from "../dist/native.js";
import { test } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { Rime, RimeInputError } from "../dist/index.js";
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

for (const endpoint of ["Customer.Example", "Customer.Example:8443"]) {
  test(`independent speech and discovery endpoints: ${endpoint}`, async () => {
    const standard = await new FakeService().start(),
      custom = await new FakeService().start();
    custom.payload = Buffer.from([2, 0]);
    const previous = factory.create;
    factory.create = (config) =>
      native.NativeClient.testing(
        config,
        JSON.parse(config).endpoint ? custom.target : standard.target,
        "{}",
      );
    const first = new Rime({ apiKey: "standard-key" }),
      second = new Rime({ apiKey: "custom-key", endpoint });
    try {
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
      assert.equal(
        standard.metadata[0].get("authorization")[0],
        "Bearer standard-key",
      );
      assert.equal(
        custom.metadata[0].get("authorization")[0],
        "Bearer custom-key",
      );
    } finally {
      await first.close();
      await second.close();
      standard.close();
      custom.close();
      factory.create = previous;
    }
  });
}
