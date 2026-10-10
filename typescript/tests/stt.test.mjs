import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import test from "node:test";
import * as grpc from "@grpc/grpc-js";
import { fromJson } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import * as sdk from "../dist/index.js";
import { transport, TranscriptionCall } from "../dist/stt/transport.js";
import { TranscriptState } from "../dist/stt/protocol.js";
import { policy } from "../dist/stt/policy.js";
import { RecognitionService } from "./stt-service.mjs";

const cases = JSON.parse(
  readFileSync(
    new URL("../../conformance/stt/transcripts.json", import.meta.url),
    "utf8",
  ),
).cases;
function watch(promise) {
  promise.catch(() => {});
  return promise;
}
function deferred() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
async function* source(...chunks) {
  for (const chunk of chunks.length ? chunks : [Buffer.alloc(2)]) yield chunk;
}
async function setup(t) {
  const service = await new RecognitionService().start();
  t.after(() => service.close());
  const before = { ...policy };
  Object.assign(policy, {
    target: service.target,
    acceptanceTimeout: 0.2,
    completionTimeout: 0.2,
    cleanupTimeout: 0.05,
  });
  t.after(() => Object.assign(policy, before));
  t.mock.method(
    transport,
    "makeClient",
    (target) => new grpc.Client(target, grpc.credentials.createInsecure()),
  );
  const client = new sdk.Rime({ apiKey: "test-key" });
  t.after(() => client.close());
  return { service, client };
}

for (const item of cases)
  test(`shared transcript contract: ${item.name}`, () => {
    const state = new TranscriptState(65536),
      actual = [];
    const run = () => {
      for (const raw of item.messages) {
        const update = state.accept(
          fromJson(schema.StreamingTranscriptionResponseSchema, raw, {
            ignoreUnknownFields: true,
          }),
          item.inputDone ?? true,
        );
        if (update) actual.push(update);
      }
      actual.push(state.finish());
    };
    if (item.error) assert.throws(run, sdk[item.error]);
    else {
      run();
      assert.deepEqual(actual, item.expected);
    }
  });

test("lazy streaming delivers replacement partials before source exhaustion", async (t) => {
  const { service, client } = await setup(t),
    gate = deferred();
  async function* live() {
    yield Buffer.alloc(2);
    await gate.promise;
  }
  const stream = client.stt.stream(live(), {
    language: "en-GB",
    mode: "verbatim",
    contextTerms: [" Super-G ", "uno,dos", "Español"],
  });
  assert.deepEqual(service.calls, []);
  assert.deepEqual((await stream.next()).value, {
    kind: "partial",
    text: "I scream",
  });
  assert.deepEqual((await stream.next()).value, {
    kind: "partial",
    text: "Ice cream",
  });
  assert.equal(service.inputFinished, false);
  gate.resolve();
  assert.deepEqual((await stream.next()).value, {
    kind: "final",
    text: "Ice cream",
    language: "en",
  });
  assert.equal((await stream.next()).done, true);
  assert.equal(stream.requestId, "stt-request");
  const config = service.calls[0][0].payload.value;
  assert.equal(config.language, "en-GB");
  assert.equal(config.mode, schema.TranscriptionMode.VERBATIM);
  assert.deepEqual(config.contextTerms, [" Super-G ", "uno,dos", "Español"]);
  assert.equal(service.metadata[0].get("authorization")[0], "Bearer test-key");
});

test("acceptance deadline never consumes input before acceptance", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "no_acceptance";
  let touched = false;
  async function* live() {
    touched = true;
    yield Buffer.alloc(2);
  }
  const stream = client.stt.stream(live(), { language: "en" });
  await assert.rejects(
    stream.next(),
    (error) =>
      error instanceof sdk.RimeTimeoutError &&
      error.requestId === "stt-request",
  );
  assert.equal(touched, false);
  await stream.cancel();
});

for (const [mode, ErrorType] of [
  ["missing_final", sdk.RimeStreamError],
  ["duplicate_final", sdk.RimeStreamError],
  ["done_then_error", sdk.RimeUnavailableError],
  ["early_final", sdk.RimeStreamError],
  ["no_completion", sdk.RimeTimeoutError],
])
  test(`incomplete transport never yields final: ${mode}`, async (t) => {
    const { service, client } = await setup(t);
    service.mode = mode;
    const audio =
      mode === "early_final"
        ? {
            [Symbol.asyncIterator]() {
              return {
                next: () => new Promise(() => {}),
                return: async () => ({ done: true }),
              };
            },
          }
        : source();
    const stream = client.stt.stream(audio, { language: "en" }),
      updates = [];
    await assert.rejects(
      async () => {
        for await (const update of stream) updates.push(update);
      },
      (error) =>
        error instanceof ErrorType && error.requestId === "stt-request",
    );
    await stream.cancel();
    assert.ok(updates.every((update) => update.kind !== "final"));
    assert.equal(service.calls.length, 1);
  });

for (const [status, ErrorType] of [
  [grpc.status.UNAUTHENTICATED, sdk.RimeAuthenticationError],
  [grpc.status.PERMISSION_DENIED, sdk.RimePermissionError],
  [grpc.status.INVALID_ARGUMENT, sdk.RimeInputError],
  [grpc.status.RESOURCE_EXHAUSTED, sdk.RimeResourceLimitError],
  [grpc.status.UNAVAILABLE, sdk.RimeUnavailableError],
  [grpc.status.DEADLINE_EXCEEDED, sdk.RimeTimeoutError],
  [grpc.status.CANCELLED, sdk.RimeCancelledError],
  [grpc.status.INTERNAL, sdk.RimeStreamError],
])
  test(`server rejection retains status and request ID: ${status}`, async (t) => {
    const { service, client } = await setup(t);
    service.mode = "reject";
    service.rejection = status;
    const stream = client.stt.stream(source(), { language: "und" });
    await assert.rejects(
      stream.next(),
      (error) =>
        error instanceof ErrorType && error.requestId === "stt-rejected",
    );
    assert.equal(stream.requestId, "stt-rejected");
    assert.equal(service.calls[0][0].payload.value.language, "und");
    await stream.cancel();
  });

test("silence completes with an empty transcript", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "silence";
  const stream = client.stt.stream(source(), { language: "en" }),
    updates = [];
  for await (const update of stream) updates.push(update);
  assert.deepEqual(updates, [{ kind: "final", text: "", language: "en" }]);
});

test("arbitrary input byte splits are downmixed before writing", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "silence";
  const stream = client.stt.stream(
    source(Buffer.from([2]), Buffer.from([0, 4]), Buffer.from([0])),
    { language: "en", inputFormat: { channels: 2 } },
  );
  assert.equal((await stream.next()).value.kind, "final");
  assert.deepEqual(
    Buffer.concat(service.calls[0].slice(1).map((m) => m.payload.value)),
    Buffer.from([3, 0]),
  );
});

for (const bad of [Buffer.from([1]), "not bytes"])
  test(`invalid audio cancels without half-close: ${typeof bad}`, async (t) => {
    const { client } = await setup(t);
    const finish = t.mock.method(TranscriptionCall.prototype, "finishInput");
    const stream = client.stt.stream(source(bad), { language: "en" });
    await assert.rejects(stream.next(), sdk.RimeAudioFormatError);
    await stream.cancel();
    assert.equal(finish.mock.calls.length, 0);
  });

test("source failure cancels without half-close", async (t) => {
  const { client } = await setup(t);
  const finish = t.mock.method(TranscriptionCall.prototype, "finishInput");
  const audio = {
    [Symbol.asyncIterator]() {
      return {
        next: async () => {
          throw new Error("source failed");
        },
      };
    },
  };
  const stream = client.stt.stream(audio, { language: "en" });
  await assert.rejects(stream.next(), sdk.RimeInputError);
  await stream.cancel();
  assert.equal(finish.mock.calls.length, 0);
});

test("cancellation releases a stuck source and invokes return", async (t) => {
  const { client } = await setup(t);
  let returned = false;
  const audio = {
    [Symbol.asyncIterator]() {
      let sent = false;
      return {
        next() {
          if (!sent) {
            sent = true;
            return Promise.resolve({ done: false, value: Buffer.alloc(2) });
          }
          return new Promise(() => {});
        },
        return() {
          returned = true;
          return Promise.resolve({ done: true });
        },
      };
    },
  };
  const stream = client.stt.stream(audio, { language: "en" });
  assert.equal((await stream.next()).value.kind, "partial");
  await stream.cancel();
  await stream.cancel();
  assert.ok(returned);
  await assert.rejects(stream.next(), sdk.RimeCancelledError);
});

test(
  "overall timeout runs while consumer is paused and queue is bounded",
  { timeout: 5000 },
  async (t) => {
    const { service, client } = await setup(t);
    service.mode = "burst";
    policy.acceptanceTimeout = 2;
    policy.completionTimeout = 2;
    const stream = client.stt.stream(source(), { language: "en", timeout: 1 });
    const full = deferred();
    const put = stream.queue.put.bind(stream.queue);
    t.mock.method(stream.queue, "put", (update) => {
      if (stream.queue.items.length === policy.queuedUpdates) full.resolve();
      return put(update);
    });
    assert.equal((await stream.next()).value.kind, "partial");
    await full.promise;
    assert.equal(stream.queue.items.length, policy.queuedUpdates);
    await sleep(1100);
    await assert.rejects(
      stream.next(),
      (error) =>
        error instanceof sdk.RimeTimeoutError &&
        error.message === "Overall transcription deadline expired",
    );
    assert.deepEqual(stream.queue.items, []);
    await stream.cancel();
  },
);

test("one concurrent reader and cancellation before start", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "no_acceptance";
  const stream = client.stt.stream(source(), { language: "en" });
  const pending = watch(stream.next());
  await service.configSeen;
  await assert.rejects(stream.next(), sdk.RimeInputError);
  await stream.cancel();
  await assert.rejects(pending, sdk.RimeCancelledError);
  const lazy = client.stt.stream(source(), { language: "en" });
  await lazy.cancel();
  await assert.rejects(lazy.next(), sdk.RimeCancelledError);
  assert.equal(service.calls.length, 1);
});

for (const queueLimit of [1, 16])
  test(
    `completed transcription survives a paused consumer with queue limit ${queueLimit}`,
    { timeout: 5000 },
    async (t) => {
      const { client } = await setup(t);
      policy.queuedUpdates = queueLimit;
      const stream = client.stt.stream(source(), { language: "en" });
      const finalReady = deferred();
      const put = stream.queue.put.bind(stream.queue);
      t.mock.method(stream.queue, "put", async (update) => {
        if (update.kind === "final") finalReady.resolve();
        await put(update);
      });
      assert.equal((await stream.next()).value.text, "I scream");
      await finalReady.promise;
      await sleep(policy.completionTimeout * 1000 + 50);
      const updates = [];
      for await (const update of stream) updates.push(update);
      assert.deepEqual(updates, [
        { kind: "partial", text: "Ice cream" },
        { kind: "final", text: "Ice cream", language: "en" },
      ]);
      assert.equal(stream.requestId, "stt-request");
    },
  );

test("overall deadline still applies to a completed transcription", async (t) => {
  const { client } = await setup(t);
  const stream = client.stt.stream(source(), {
    language: "en",
    timeout: 0.4,
  });
  assert.equal((await stream.next()).value.kind, "partial");
  await stream.worker;
  assert.equal(client.sttClient.streams.has(stream), true);
  await sleep(450);
  assert.equal(client.sttClient.streams.has(stream), false);
  await assert.rejects(
    stream.next(),
    (error) =>
      error instanceof sdk.RimeTimeoutError &&
      error.message === "Overall transcription deadline expired",
  );
  await stream.cancel();
});

test("AbortSignal releases a completed transcription before consumer cleanup", async (t) => {
  const { client } = await setup(t);
  const controller = new AbortController();
  const stream = client.stt.stream(source(), {
    language: "en",
    signal: controller.signal,
  });
  assert.equal((await stream.next()).value.kind, "partial");
  await stream.worker;
  assert.equal(client.sttClient.streams.has(stream), true);
  controller.abort();
  assert.equal(client.sttClient.streams.has(stream), false);
  await assert.rejects(stream.next(), sdk.RimeCancelledError);
});

test("client close cancels started and unstarted operations", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "no_acceptance";
  const started = client.stt.stream(source(), { language: "en" });
  const lazy = client.stt.stream(source(), { language: "en" });
  const pending = watch(started.next());
  await service.configSeen;
  await client.close();
  await assert.rejects(pending, sdk.RimeCancelledError);
  await assert.rejects(lazy.next(), sdk.RimeCancelledError);
});

test("AbortSignal and iterator return cancel only the selected utterance", async (t) => {
  const { client } = await setup(t),
    controller = new AbortController();
  const first = client.stt.stream(source(), {
    language: "en",
    signal: controller.signal,
  });
  controller.abort();
  await assert.rejects(first.next(), sdk.RimeCancelledError);
  const second = client.stt.stream(source(), { language: "en" });
  for await (const update of second) {
    assert.equal(update.kind, "partial");
    break;
  }
  await assert.rejects(second.next(), sdk.RimeCancelledError);
  const third = client.stt.stream(source(), { language: "en" }),
    updates = [];
  for await (const update of third) updates.push(update);
  assert.equal(updates.at(-1).kind, "final");
});

test("STT declaration types narrow partial and final results", async () => {
  const { execFileSync } = await import("node:child_process");
  const { fileURLToPath } = await import("node:url");
  execFileSync(
    process.execPath,
    [
      fileURLToPath(import.meta.resolve("typescript/bin/tsc")),
      "--strict",
      "--noEmit",
      "--skipLibCheck",
      "--target",
      "ES2022",
      "--module",
      "NodeNext",
      fileURLToPath(new URL("fixtures/stt.ts", import.meta.url)),
    ],
    { stdio: "inherit" },
  );
});

test("pre-aborted calls release their source and client registration", async (t) => {
  const client = new sdk.Rime({ apiKey: "test" });
  t.after(() => client.close());
  const controller = new AbortController();
  controller.abort();
  for (let index = 0; index < 10; index++) {
    const stream = client.stt.stream(source(), {
      language: "en",
      signal: controller.signal,
    });
    await assert.rejects(stream.next(), sdk.RimeCancelledError);
    assert.equal(client.sttClient.streams.size, 0);
    assert.equal(stream.source, null);
  }
});

test("AbortSignal interrupts a pending response and preserves sibling operations", async (t) => {
  const { service, client } = await setup(t);
  service.mode = "no_acceptance";
  const controller = new AbortController();
  const stream = client.stt.stream(source(), {
    language: "en",
    signal: controller.signal,
  });
  const pending = watch(stream.next());
  await service.configSeen;
  controller.abort();
  await assert.rejects(pending, sdk.RimeCancelledError);
  await stream.cancel();
  service.mode = "normal";
  const updates = [];
  for await (const update of client.stt.stream(source(), { language: "en" }))
    updates.push(update);
  assert.equal(updates.at(-1).kind, "final");
});

test("cleanup failure still closes every feature and credentials", async (t) => {
  const client = new sdk.Rime({ apiKey: "test" }),
    closed = [];
  t.mock.method(client.realtimeClient, "close", async () => {
    closed.push("realtime");
    throw new Error("cleanup failure");
  });
  t.mock.method(client.ttsClient, "close", async () => {
    closed.push("tts");
  });
  t.mock.method(client.sttClient, "close", async () => {
    closed.push("stt");
  });
  await assert.rejects(client.close(), /cleanup failure/);
  assert.deepEqual(closed.sort(), ["realtime", "stt", "tts"]);
  assert.throws(
    () => client.credentials.authorization(),
    sdk.RimeAuthenticationError,
  );
  await assert.rejects(client.close(), /cleanup failure/);
});

test("closing during connection setup cancels without waiting for readiness", async (t) => {
  const { client } = await setup(t),
    connecting = deferred();
  let closed = false;
  t.mock.method(transport, "makeClient", () => ({
    waitForReady() {
      connecting.resolve();
    },
    close() {
      closed = true;
    },
  }));
  const stream = client.stt.stream(source(), { language: "en" });
  const pending = watch(stream.next());
  await connecting.promise;
  await client.close();
  await assert.rejects(pending, sdk.RimeCancelledError);
  assert.ok(closed);
});

test("STT uses its endpoint independently of the TTS endpoint", async (t) => {
  await setup(t);
  const targets = [];
  t.mock.method(transport, "makeClient", (target) => {
    targets.push(target);
    throw new sdk.RimeUnavailableError("test dial");
  });
  const client = new sdk.Rime({
    apiKey: "test",
    endpoint: "tts.example:123",
    sttEndpoint: "stt.example:456",
  });
  t.after(() => client.close());
  const stream = client.stt.stream(source(), { language: "en" });
  await assert.rejects(stream.next(), sdk.RimeUnavailableError);
  assert.deepEqual(targets, ["stt.example:456"]);
  assert.equal(client.ttsClient.client, null);
});

test("cancelling STT preserves TTS and Prism; owner close cancels all", async (t) => {
  const { client } = await setup(t);
  const { FakeService } = await import("./service.mjs"),
    { Peer } = await import("./realtime-peer.mjs");
  const { transport: ttsTransport } = await import("../dist/tts/transport.js");
  const ttsService = await new FakeService().start(),
    peer = await new Peer().start();
  t.after(() => ttsService.close());
  t.after(() => peer.close());
  t.mock.method(
    ttsTransport,
    "makeClient",
    () => new grpc.Client(ttsService.target, grpc.credentials.createInsecure()),
  );
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  const stream = client.stt.stream(source(), { language: "en" });
  assert.equal((await stream.next()).value.kind, "partial");
  await stream.cancel();
  const audio = [];
  for await (const chunk of client.tts.synthesize("Hello.")) audio.push(chunk);
  assert.deepEqual(Buffer.concat(audio), ttsService.payload);
  peer.emit("prsm.typed_input.ready");
  const reply = watch(session.sendText("hello")),
    request = await peer.next("response.create");
  peer.accepted(request);
  const reference = await reply;
  peer.ended(reference.responseId);
  const lazy = client.stt.stream(source(), { language: "en" });
  await client.close();
  await assert.rejects(lazy.next(), sdk.RimeCancelledError);
  await assert.rejects(session.sendText("closed"));
});

test("transcript queue preserves atomic snapshots and wakes blocked writers", async () => {
  const { TranscriptQueue } = await import("../dist/stt/queue.js");
  const queue = new TranscriptQueue(1),
    first = { kind: "partial", text: "First complete snapshot" },
    second = { kind: "partial", text: "Second complete snapshot" };
  await queue.put(first);
  let finished = false;
  const writer = queue.put(second).then(() => {
    finished = true;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  assert.deepEqual((await queue.get()).value, first);
  await writer;
  assert.deepEqual((await queue.get()).value, second);
  const reader = watch(queue.get());
  queue.fail(new sdk.RimeCancelledError("cancelled"));
  await assert.rejects(reader, sdk.RimeCancelledError);
  const full = new TranscriptQueue(1);
  await full.put(first);
  const blocked = full.put(second);
  full.fail(new sdk.RimeCancelledError("cancelled"));
  await blocked;
});

test("transcript size limit counts UTF-8 bytes", () => {
  const state = new TranscriptState(4);
  state.accept(
    fromJson(schema.StreamingTranscriptionResponseSchema, cases[0].messages[0]),
    false,
  );
  assert.throws(
    () =>
      state.accept(
        fromJson(schema.StreamingTranscriptionResponseSchema, {
          hypothesis: { text: "ééé", revision: "1" },
        }),
        false,
      ),
    sdk.RimeResourceLimitError,
  );
});

test("the documented STT file example prints a final and request ID", async (t) => {
  const { service, client } = await setup(t);
  const { mkdtemp, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const directory = await mkdtemp(join(tmpdir(), "sdk-stt-example-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, "audio.pcm");
  await writeFile(path, Buffer.alloc(3200));
  const logs = [];
  t.mock.method(console, "log", (line) => logs.push(line));
  const example = await import("../examples/stt/stream.mjs");
  await example.main(path, "en", "written", client);
  assert.ok(logs.includes("final: Ice cream"));
  assert.ok(logs.includes("request_id: stt-request"));
  assert.ok(service.inputFinished);
});

for (const mode of ["normal", "no_acceptance"])
  test(`file example defers opening and handles missing input: ${mode}`, async (t) => {
    const { service, client } = await setup(t);
    service.mode = mode;
    const { mkdtemp, rm } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const directory = await mkdtemp(join(tmpdir(), "sdk-stt-missing-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const example = await import("../examples/stt/stream.mjs");
    await assert.rejects(
      example.main(join(directory, "missing.pcm"), "en", "written", client),
      (error) =>
        mode === "normal"
          ? error instanceof sdk.RimeInputError &&
            error.cause?.code === "ENOENT"
          : error instanceof sdk.RimeTimeoutError,
    );
    assert.equal(service.inputFinished, false);
  });
