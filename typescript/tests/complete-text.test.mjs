import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { create, toBinary } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import {
  Rime,
  AudioFormat,
  RimeCancelledError,
  RimeInputError,
  RimeResourceLimitError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../dist/index.js";
import { policy } from "../dist/tts/policy.js";
import { FakeService } from "./service.mjs";

async function setup(t) {
  const service = await new FakeService().start();
  // Only substitute TLS; the production factory supplies all channel limits.
  t.mock.method(grpc.credentials, "createSsl", () =>
    grpc.credentials.createInsecure(),
  );
  const client = new Rime({ apiKey: "test-key", endpoint: service.target });
  const limits = { ...policy };
  t.after(async () => {
    await client.close();
    service.close();
    Object.assign(policy, limits);
  });
  return { service, client };
}

async function collect(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

function delayCompletion(t, service) {
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  service.beforeFinalResponses = () => gate;
  t.after(() => release());
  return release;
}

test(
  "complete text partial failure is not completion or retried",
  { timeout: 5000 },
  async (t) => {
    const { service, client } = await setup(t);
    service.mode = "partial_error";
    const stream = client.tts.stream("Hello.", { completeText: true });
    assert.deepEqual(Buffer.from((await stream.next()).value), service.payload);
    service.release();
    await assert.rejects(
      collect(stream),
      (error) =>
        error instanceof RimeUnavailableError &&
        error.message === service.rejectionMessage &&
        error.requestId === "test-request",
    );
    assert.equal(service.completeCalls.length, 1);
    service.mode = "normal";
    assert.deepEqual(
      await collect(client.tts.stream("Again.", { completeText: true })),
      service.payload,
    );
  },
);

for (const afterAudio of [false, true]) {
  test(
    `complete text stall cancels RPC without overall deadline / afterAudio=${afterAudio}`,
    { timeout: 5000 },
    async (t) => {
      const { service, client } = await setup(t);
      policy.firstAudioTimeout = afterAudio ? 5 : 0.1;
      policy.progressTimeout = afterAudio ? 0.1 : 5;
      if (afterAudio) delayCompletion(t, service);
      else service.mode = "silence";
      const stream = client.tts.stream("Hello.", {
        completeText: true,
        timeout: null,
      });
      if (afterAudio)
        assert.deepEqual(
          Buffer.from((await stream.next()).value),
          service.payload,
        );
      await assert.rejects(
        collect(stream),
        (error) =>
          error instanceof RimeTimeoutError &&
          /stopped making progress/.test(error.message) &&
          error.requestId === "test-request",
      );
      await stream.cancel();
      await service.cancelled;
      assert.equal(client.ttsClient.streams.size, 0);
    },
  );

  test(
    `complete text client close wakes pending reader / afterAudio=${afterAudio}`,
    { timeout: 5000 },
    async (t) => {
      const { service, client } = await setup(t);
      if (afterAudio) delayCompletion(t, service);
      else service.mode = "silence";
      const stream = client.tts.stream("Hello.", { completeText: true });
      if (afterAudio)
        assert.deepEqual(
          Buffer.from((await stream.next()).value),
          service.payload,
        );
      const rejected = assert.rejects(stream.next(), RimeCancelledError);
      await service.headersSent;
      await client.close();
      await rejected;
      await service.cancelled;
      assert.equal(client.ttsClient.streams.size, 0);
    },
  );
}

test(
  "complete text cancellation keeps an active sibling",
  { timeout: 5000 },
  async (t) => {
    const { service, client } = await setup(t);
    const release = delayCompletion(t, service);
    const first = client.tts.stream("First.", { completeText: true });
    const sibling = client.tts.stream("Sibling.", { completeText: true });
    assert.deepEqual(Buffer.from((await first.next()).value), service.payload);
    assert.deepEqual(
      Buffer.from((await sibling.next()).value),
      service.payload,
    );
    const rejected = assert.rejects(first.next(), RimeCancelledError);
    await first.cancel();
    await rejected;
    await service.cancelled;
    release();
    assert.equal((await collect(sibling)).length, 0);
    assert.equal(service.completeCalls.length, 2);
    assert.equal(client.ttsClient.streams.size, 0);
  },
);

for (const audioFormat of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
  test(
    `complete text slow consumer preserves bounded audio / ${audioFormat.encoding}`,
    { timeout: 5000 },
    async (t) => {
      const { service, client } = await setup(t);
      const release = delayCompletion(t, service);
      policy.progressTimeout = 0.05;
      const samples = 3 * (policy.outputBytes + 1);
      service.payload = Buffer.alloc(samples * 2);
      const stream = client.tts.stream("Hello.", {
        completeText: true,
        audioFormat,
        timeout: null,
      });
      const chunks = [(await stream.next()).value];
      await sleep(150);
      assert.ok(stream.queue.size <= policy.outputBytes);
      chunks.push((await stream.next()).value);
      release();
      for await (const chunk of stream) chunks.push(chunk);
      assert.ok(
        chunks.every(
          (chunk) =>
            chunk.length > 0 && chunk.length <= policy.outputChunkBytes,
        ),
      );
      const expected =
        audioFormat === AudioFormat.PCM_24000
          ? service.payload
          : Buffer.alloc(Math.ceil(samples / 3), 255);
      assert.deepEqual(Buffer.concat(chunks), expected);
    },
  );
}

test(
  "complete text overall deadline applies while consumer is paused",
  { timeout: 5000 },
  async (t) => {
    const { service, client } = await setup(t);
    delayCompletion(t, service);
    service.payload = Buffer.alloc(policy.outputBytes * 2);
    const stream = client.tts.stream("Hello.", {
      completeText: true,
      timeout: 0.15,
    });
    assert.ok((await stream.next()).value.length);
    await sleep(250);
    await assert.rejects(
      stream.next(),
      (error) =>
        error instanceof RimeTimeoutError &&
        /Overall synthesis deadline/.test(error.message),
    );
    await stream.cancel();
    await service.cancelled;
  },
);

for (const character of ["a", "é", "🙂"]) {
  test(
    `complete text UTF-8 byte boundary and recovery / ${character}`,
    { timeout: 5000 },
    async (t) => {
      const { service, client } = await setup(t);
      const text = character.repeat(65536 / Buffer.byteLength(character));
      assert.deepEqual(
        await collect(client.tts.stream(text, { completeText: true })),
        service.payload,
      );
      assert.equal(service.completeCalls[0].text, text);
      assert.throws(
        () => client.tts.stream(text + "a", { completeText: true }),
        RimeInputError,
      );
      assert.equal(service.completeCalls.length, 1);
      assert.deepEqual(
        await collect(client.tts.stream("Again.", { completeText: true })),
        service.payload,
      );
    },
  );
}

for (const completeText of [false, true]) {
  test(
    `serialized request limit and recovery / completeText=${completeText}`,
    { timeout: 5000 },
    async (t) => {
      const { service, client } = await setup(t);
      const text = completeText ? "é".repeat(32768) : "Hello.";
      const customLexicon = Array.from({ length: 500 }, (_, index) => ({
        spelling: `word${index}`,
        pronunciation: '" k { S',
      }));
      const size = () => {
        const request = create(schema.SynthesisRequestSchema, {
          speaker: "clementine",
          language: "en",
          text: completeText ? text : "",
          audioParameters: { audioFormat: "audio/pcm", samplingRate: 24000 },
          customLexicon,
        });
        return completeText
          ? toBinary(schema.SynthesisRequestSchema, request).length
          : toBinary(
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
        await collect(client.tts.stream(text, { completeText, customLexicon })),
        service.payload,
      );
      customLexicon.at(-1).spelling += "x";
      assert.equal(size(), 131073);
      await assert.rejects(async () => {
        for await (const _ of client.tts.stream(text, {
          completeText,
          customLexicon,
        }))
          assert.fail("Oversized request produced audio");
      }, RimeResourceLimitError);
      assert.deepEqual(
        await collect(client.tts.stream("Again.", { completeText })),
        service.payload,
      );
      const accepted = service.calls.filter((messages) => messages.length);
      assert.equal(accepted.length, 2);
      const request = completeText
        ? accepted[0][0]
        : accepted[0][0].payload.value;
      assert.equal(request.customLexicon.length, 500);
    },
  );
}
