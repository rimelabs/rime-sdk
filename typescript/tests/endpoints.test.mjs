import { test } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { Rime, RimeInputError } from "../dist/index.js";
import { authentication, Credentials } from "../dist/auth.js";
import { transport } from "../dist/tts/transport.js";
import { policy } from "../dist/tts/policy.js";
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

for (const model of [
  "mist",
  "mistv2",
  "mistv4",
  "arcana",
  "unknown",
  "",
  42,
  [],
])
  for (const endpoint of [undefined, "mist.api.rime.ai"])
    test(`unsupported model ${JSON.stringify(model)}, endpoint=${endpoint}`, () => {
      assert.throws(
        () =>
          new Rime({
            apiKey: "test-key",
            model,
            endpoint,
          }),
        RimeInputError,
      );
    });

for (const themis of [false, true])
  for (const [model, defaultVoice] of [
    ["coda", "clementine"],
    ["mistv3", "astra"],
  ])
    for (const endpoint of ["Customer.Example", "Customer.Example:8443"])
      test(
        `independent speech and discovery endpoints: ${model}, ${endpoint}, Themis=${themis}`,
        { timeout: 10000 },
        async (t) => {
          const original = { ...policy };
          const standard = await new FakeService().start();
          const mist = await new FakeService().start();
          const custom = await new FakeService().start();
          t.after(() => {
            standard.close();
            mist.close();
            custom.close();
          });
          mist.payload = Buffer.from(Array(2400).fill([2, 0]).flat());
          custom.payload = Buffer.from(Array(2400).fill([3, 0]).flat());
          mist.supportedSpeakers = ["astra"];
          custom.supportedSpeakers = ["customer-voice"];
          const routes = {
            "coda.api.rime.ai:50051": standard.target,
            "mist.api.rime.ai:50051": mist.target,
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
          const second = new Rime({ apiKey: "mist-key", model: "mistv3" });
          const third = new Rime({ apiKey: "custom-key", model, endpoint });
          t.after(async () => {
            await Promise.all([first.close(), second.close(), third.close()]);
          });
          async function use(client, service, voice) {
            const chunks = [];
            for await (const chunk of client.tts.stream("Hello."))
              chunks.push(chunk);
            assert.equal(service.calls[0][0].payload.value.speaker, voice);
            assert.equal(
              service.calls[0][0].payload.value.audioParameters.samplingRate,
              24000,
            );
            assert.deepEqual(await client.languages.list(), ["en", "de"]);
            assert.deepEqual(
              await client.voices.list(),
              service.supportedSpeakers,
            );
            let release;
            const resume = new Promise((resolve) => {
              release = resolve;
            });
            async function* source() {
              yield "First sentence. The next sentence ";
              await resume;
              yield "is here.";
            }
            const audio = client.tts.stream(source(), {
              voice: "explicit-voice",
            });
            const first = await audio.next();
            assert.equal(first.done, false);
            release();
            const explicitChunks = [first.value];
            for await (const chunk of audio) explicitChunks.push(chunk);
            assert.deepEqual(
              Buffer.concat(explicitChunks),
              Buffer.concat([service.payload, service.payload]),
            );
            assert.equal(
              service.calls[1][0].payload.value.speaker,
              "explicit-voice",
            );
            return Buffer.concat(chunks);
          }
          assert.deepEqual(
            await Promise.all([
              use(first, standard, "clementine"),
              use(second, mist, "astra"),
              use(third, custom, defaultVoice),
            ]),
            [standard.payload, mist.payload, custom.payload],
          );
          assert.deepEqual(targets.sort(), Object.keys(routes).sort());
          assert.deepEqual(standard.metadata[0].get("authorization"), [
            "Bearer standard-key",
          ]);
          assert.deepEqual(custom.metadata[0].get("authorization"), [
            "Bearer custom-key",
          ]);
          assert.deepEqual(mist.metadata[0].get("authorization"), [
            "Bearer mist-key",
          ]);
          assert.equal(standard.discoveryCalls, 1);
          assert.equal(mist.discoveryCalls, 1);
          assert.equal(custom.discoveryCalls, 1);
          assert.deepEqual(
            audiences.sort(),
            themis
              ? ["coda.api.rime.ai", "customer.example", "mist.api.rime.ai"]
              : [],
          );
          assert.deepEqual(policy, original);
        },
      );
