import test from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { create, toBinary } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { Rime, RimeResourceLimitError } from "../dist/index.js";
import { FakeService } from "./service.mjs";

async function setup(t) {
  const service = await new FakeService().start();
  // Only substitute TLS; the production factory supplies all channel limits.
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

test("serialized request limit and recovery", { timeout: 5000 }, async (t) => {
  const { service, client } = await setup(t);
  const text = "Hello.";
  const customLexicon = Array.from({ length: 500 }, (_, index) => ({
    spelling: `word${index}`,
    pronunciation: '" k { S',
  }));
  const size = () => {
    const request = create(schema.SynthesisRequestSchema, {
      splitStrategy: schema.SplitStrategy.ACCUMULATE_SENTENCE,
      speaker: "clementine",
      language: "en",
      audioParameters: { audioFormat: "audio/pcm", samplingRate: 24000 },
      customLexicon,
    });
    return toBinary(
      schema.StreamingSynthesisRequestSchema,
      create(schema.StreamingSynthesisRequestSchema, {
        payload: { case: "header", value: request },
      }),
    ).length;
  };
  // Padding isolates the wire-size limit from entry-count/linguistic validation.
  let difference;
  while ((difference = 131072 - size()) !== 0) {
    const last = customLexicon.at(-1);
    last.spelling =
      difference > 0
        ? last.spelling + "x".repeat(difference)
        : last.spelling.slice(0, difference);
  }
  assert.deepEqual(
    await collect(client.tts.synthesize(text, { customLexicon })),
    service.payload,
  );
  customLexicon.at(-1).spelling += "x";
  assert.equal(size(), 131073);
  await assert.rejects(async () => {
    for await (const _ of client.tts.synthesize(text, {
      customLexicon,
    }))
      assert.fail("Oversized request produced audio");
  }, RimeResourceLimitError);
  assert.deepEqual(
    await collect(client.tts.synthesize("Again.")),
    service.payload,
  );
  const accepted = service.calls.filter((messages) => messages.length);
  assert.equal(accepted.length, 2);
  const request = accepted[0][0].payload.value;
  assert.equal(request.customLexicon.length, 500);
});
