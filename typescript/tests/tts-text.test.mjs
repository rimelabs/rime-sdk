import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import * as grpc from "@grpc/grpc-js";
import * as schema from "@rimelabs/api";
import { create } from "@bufbuild/protobuf";
import { Rime, RimeInputError } from "../dist/index.js";
import { policy } from "../dist/tts/policy.js";
import { FakeService } from "./service.mjs";
const cases = JSON.parse(
  fs.readFileSync(
    new URL("../../conformance/text-input.json", import.meta.url),
  ),
);
async function setup(t) {
  const service = await new FakeService().start();
  t.mock.method(grpc.credentials, "createSsl", () =>
    grpc.credentials.createInsecure(),
  );
  const client = new Rime({ apiKey: "test-key", endpoint: service.target });
  t.after(async () => {
    await client.close();
    service.close();
  });
  return { service, client };
}
async function collect(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}
function check(service, text) {
  const [header, ...messages] = service.calls[0];
  assert.equal(
    header.payload.value.splitStrategy,
    schema.SplitStrategy.ACCUMULATE_SENTENCE,
  );
  assert.equal(messages.map((m) => m.payload.value).join(""), text);
  assert.ok(
    messages.every(
      (m) =>
        Buffer.byteLength(m.payload.value) > 0 &&
        Buffer.byteLength(m.payload.value) <= 4096,
    ),
  );
}
for (const fixture of cases)
  for (const size of [1, 7, 1024]) {
    test(`preserve text / ${fixture.id} / ${size}`, async (t) => {
      const { service, client } = await setup(t);
      async function* source() {
        for (let offset = 0; offset < fixture.text.length; offset += size) {
          yield fixture.text.slice(offset, offset + size);
          yield "";
        }
      }
      await collect(client.tts.stream(source()));
      check(service, fixture.text);
    });
  }
for (const text of [
  "x".repeat(70000),
  "🚀".repeat(20000),
  "word " + " ".repeat(70000) + "end",
]) {
  test(`bounded text without sentence limit / ${text.length}`, async (t) => {
    const { service, client } = await setup(t);
    await collect(client.tts.synthesize(text));
    check(service, text);
  });
}
test(
  "fragments sent promptly and final audio drained after source pause",
  { timeout: 5000 },
  async (t) => {
    const { service, client } = await setup(t);
    service.mode = "empty_audio";
    service.finalResponses = [
      create(schema.SynthesisResponseStreamSchema, {
        payload: { case: "audio", value: service.payload },
      }),
    ];
    const previous = policy.firstAudioTimeout;
    policy.firstAudioTimeout = 0.1;
    t.after(() => {
      policy.firstAudioTimeout = previous;
    });
    async function* source() {
      yield "The value is 1.";
      await service.textReceived;
      await new Promise((resolve) => setTimeout(resolve, 200));
      yield "7";
      yield " ";
      yield "percent without final punctuation";
    }
    assert.deepEqual(
      await collect(client.tts.stream(source())),
      service.payload,
    );
    assert.deepEqual(
      service.calls[0].slice(1).map((m) => m.payload.value),
      ["The value is 1.", "7", " ", "percent without final punctuation"],
    );
  },
);
for (const fragments of [
  ["\ud800"],
  ["\udfff"],
  ["\ud800", "x"],
  ["ok", "\udfff"],
]) {
  test(`reject malformed Unicode / ${JSON.stringify(fragments)}`, async (t) => {
    const { client } = await setup(t);
    async function* source() {
      yield* fragments;
    }
    await assert.rejects(collect(client.tts.stream(source())), RimeInputError);
    assert.ok((await collect(client.tts.synthesize("valid"))).length);
  });
}
