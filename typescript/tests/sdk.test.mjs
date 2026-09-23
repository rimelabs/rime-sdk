import { factory, native } from "../dist/native.js";
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import * as grpc from "@grpc/grpc-js";
import {
  Rime,
  AudioFormat,
  RimeInputError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeUnavailableError,
  RimeAudioFormatError,
  RimeAuthenticationError,
  RimePermissionError,
  RimeResourceLimitError,
  RimeStreamError,
} from "../dist/index.js";
import { SentenceBuffer, ready } from "../dist/sentences.js";
import { Converter } from "../dist/audio.js";
const policy = {
  outputBytes: 96000,
  outputChunkBytes: 9600,
  progressTimeout: 0.2,
};
import { FakeService } from "./service.mjs";
const fixtures = JSON.parse(
  fs.readFileSync(new URL("../../conformance/sentences.json", import.meta.url)),
);
const contract = JSON.parse(
  fs.readFileSync(new URL("../../conformance/contract.json", import.meta.url)),
);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function collect(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}
async function setup(fn) {
  const service = await new FakeService().start();
  const previous = factory.create;
  factory.create = (config) =>
    native.NativeClient.testing(
      config,
      service.target,
      JSON.stringify({ first_audio_timeout: 0.2, progress_timeout: 0.2 }),
    );
  const client = new Rime({ apiKey: "test-key" });
  try {
    await fn(service, client);
  } finally {
    await client.close();
    service.close();
    factory.create = previous;
  }
}
for (const fixture of fixtures)
  for (const size of contract.chunk_sizes)
    test(`sentences ${fixture.id} / ${size}`, async () => {
      await ready;
      const buffer = new SentenceBuffer(65536);
      const actual = [];
      const points = [...fixture.text];
      for (let i = 0; i < points.length; i += size)
        actual.push(...buffer.feed(points.slice(i, i + size).join("")));
      actual.push(...buffer.feed("", true));
      assert.deepEqual(actual, fixture.sentences);
      assert.equal(actual.join(""), fixture.text);
    });
test("sentences at every UTF-16 split and end of input", async () => {
  await ready;
  for (const fixture of fixtures) {
    for (let split = 0; split <= fixture.text.length; split++) {
      const buffer = new SentenceBuffer(65536);
      const actual = [...buffer.feed(fixture.text.slice(0, split))];
      actual.push(...buffer.feed(""));
      actual.push(...buffer.feed(fixture.text.slice(split), true));
      assert.deepEqual(actual, fixture.sentences, `${fixture.id} / ${split}`);
      assert.deepEqual([...buffer.feed("", true)], []);
    }
  }
});
test("sentences after context rotation", async () => {
  await ready;
  const text =
    "Hello 🚀 world. Dr. Smith paid 3.14 dollars. \u200fمرحبا بالعالم. ".repeat(
      30,
    ) + "Done.";
  const whole = new SentenceBuffer(65536);
  const expected = [...whole.feed(text, true)];
  for (const sizes of [[1], [2, 7, 31, 128, 3], [1024]]) {
    const buffer = new SentenceBuffer(65536);
    const actual = [];
    let offset = 0,
      step = 0;
    while (offset < text.length) {
      const size = sizes[step++ % sizes.length];
      actual.push(...buffer.feed(text.slice(offset, offset + size)));
      offset += size;
    }
    actual.push(...buffer.feed("", true));
    assert.deepEqual(actual, expected);
  }
  assert.equal(expected.join(""), text);
});
test("sentence delivery at the lookahead threshold", async () => {
  await ready;
  for (const spacing of [1, 2, 15, 16, 17, 31, 32, 100]) {
    const buffer = new SentenceBuffer(65536);
    const output = [];
    for (const char of "First." + " ".repeat(spacing) + "The next sentenc")
      output.push(...buffer.feed(char));
    assert.deepEqual(output, ["First."], `spacing ${spacing}`);
  }
});
test("sentence storage does not grow with completed text", async () => {
  await ready;
  const buffer = new SentenceBuffer(256);
  const text = "Hello 🚀 world. This is a short sentence. ".repeat(1000);
  const output = [];
  for (const sentence of buffer.feed(text)) {
    output.push(sentence);
    assert.ok(buffer.retainedBytes < 2048);
  }
  output.push(...buffer.feed("", true));
  assert.equal(output.join(""), text);
  assert.equal(buffer.retainedBytes, 0);
});
test("sentence messages ignore source chunk size", () =>
  setup(async (service, client) => {
    const text = "Hi! Dr. Smith agrees. Hello.";
    for (const size of [null, 1, 7]) {
      async function* source() {
        for (let offset = 0; offset < text.length; offset += size)
          yield text.slice(offset, offset + size);
      }
      await collect(client.tts.stream(size === null ? text : source()));
    }
    const messages = service.calls.map((call) =>
      call.slice(1).map((m) => m.payload.value),
    );
    assert.deepEqual(
      messages,
      Array(3).fill(["Hi!", " Dr.", " Smith agrees.", " Hello."]),
    );
  }));
test("public validation", () => {
  for (const timeout of [...contract.invalid_timeouts, Infinity, NaN])
    assert.throws(() => new Rime({ apiKey: "test", timeout }), RimeInputError);
  assert.throws(() => new Rime({ apiKey: "" }), RimeAuthenticationError);
  assert.throws(
    () => new Rime({ apiKey: "key", model: "mist" }),
    RimeInputError,
  );
  const client = new Rime({ apiKey: "test" });
  assert.equal(client.tts.session, undefined);
  assert.throws(() => client.tts.stream(""), RimeInputError);
  assert.throws(
    () => client.tts.stream("Hi", { audioFormat: "wav" }),
    RimeAudioFormatError,
  );
});
for (const [name, expected] of Object.entries(contract.profiles))
  test(`audio ${name}`, () => {
    const profile = AudioFormat[name];
    assert.deepEqual(
      {
        encoding: profile.encoding,
        sample_rate: profile.sampleRate,
        channels: profile.channels,
      },
      expected,
    );
    const pcm = Buffer.alloc(48000);
    for (let i = 0; i < 24000; i++)
      pcm.writeInt16LE(
        Math.round(12000 * Math.sin((2 * Math.PI * 1000 * i) / 24000)),
        i * 2,
      );
    const whole = new Converter(profile),
      split = new Converter(profile),
      chunks = [];
    const reference = Buffer.concat([
      whole.process(pcm),
      whole.process(Buffer.alloc(0), true),
    ]);
    for (let i = 0; i < pcm.length; i += 137)
      chunks.push(split.process(pcm.subarray(i, i + 137)));
    chunks.push(split.process(Buffer.alloc(0), true));
    assert.deepEqual(Buffer.concat(chunks), reference);
    assert.equal(reference.length, name === "PCM_24000" ? 48000 : 8000);
  });
test("complete input uses streaming RPC", () =>
  setup(async (service, client) => {
    const audio = client.tts.stream("Hello. Final phrase");
    assert.equal(audio.format, AudioFormat.PCM_24000);
    assert.deepEqual(
      await collect(audio),
      Buffer.concat([service.payload, service.payload]),
    );
    assert.equal(audio.requestId, "test-request");
    assert.equal(service.calls.length, 1);
    assert.equal(service.calls[0][0].payload.value.speaker, "clementine");
    assert.equal(
      service.metadata[0].get("authorization")[0],
      "Bearer test-key",
    );
  }));
test("incremental_audio_before_input_end", () =>
  setup(async (service, client) => {
    let release;
    const wait = new Promise((resolve) => (release = resolve));
    async function* source() {
      yield "First sentence. The next sentence ";
      await wait;
      yield "is here.";
    }
    const audio = client.tts.stream(source());
    assert.equal((await audio.next()).done, false);
    release();
    assert.ok((await collect(audio)).length);
    assert.equal(service.calls.length, 1);
  }));
test("partial_audio_then_error", () =>
  setup(async (service, client) => {
    service.mode = "partial_error";
    const audio = client.tts.stream(
      "First sentence. The next sentence is waiting.",
    );
    assert.equal((await audio.next()).done, false);
    service.release();
    await assert.rejects(() => collect(audio), RimeUnavailableError);
    await audio.cancel();
    assert.equal(service.calls.length, 1);
  }));
test("cancel_keeps_sibling", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const first = client.tts.stream("Hello.");
    assert.equal((await first.next()).done, false);
    await first.cancel();
    await assert.rejects(() => first.next(), RimeCancelledError);
    service.mode = "normal";
    assert.ok((await collect(client.tts.stream("Sibling."))).length);
  }));
test("overall_timeout_while_paused", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const audio = client.tts.stream("Hello.", { timeout: 0.08 });
    assert.equal((await audio.next()).done, false);
    await sleep(130);
    await assert.rejects(() => audio.next(), RimeTimeoutError);
    await audio.cancel();
  }));
test("client close preserves cancellation before stream start", () =>
  setup(async (service, client) => {
    let sourceTouched = false;
    async function* source() {
      sourceTouched = true;
      yield "Hello.";
    }
    const audio = client.tts.stream(source());
    await client.close();
    await assert.rejects(() => audio.next(), RimeCancelledError);
    assert.equal(sourceTouched, false);
    assert.equal(service.calls.length, 0);
    assert.throws(() => client.tts.stream("New work."), RimeInputError);
  }));
test("lazy startup and cancellation before start", () =>
  setup(async (service, client) => {
    const audio = client.tts.stream("Hi.", { timeout: 0.01 });
    await sleep(30);
    assert.equal(service.calls.length, 0);
    await audio.cancel();
    await audio.cancel();
    await assert.rejects(() => audio.next(), RimeCancelledError);
  }));

for (const completion of ["resolve", "reject"]) {
  test(`cancellation stops a pending source read / late ${completion}`, () =>
    setup(async (_, client) => {
      const started = Promise.withResolvers();
      const pending = Promise.withResolvers();
      let returned = 0;
      const source = {
        [Symbol.asyncIterator]() {
          return {
            next() {
              started.resolve();
              return pending.promise;
            },
            async return() {
              returned++;
              return { done: true };
            },
          };
        },
      };
      const stream = client.tts.stream(source);
      const checked = assert.rejects(stream.next(), RimeCancelledError);
      await started.promise;
      await stream.cancel();
      await checked;
      assert.equal(returned, 1);
      assert.equal(client.streams.size, 0);
      if (completion === "resolve")
        pending.resolve({ done: false, value: "Too late." });
      else pending.reject(new Error("late source failure"));
      // Give a late rejection time to surface as an unhandled rejection.
      await sleep(0);
    }));
}
test("no_synthesis_replay", () =>
  setup(async (service, client) => {
    service.mode = "error_before_audio";
    const audio = client.tts.stream("Hello.");
    await assert.rejects(() => collect(audio), RimeUnavailableError);
    await audio.cancel();
    assert.equal(service.calls.length, 1);
  }));
test("format validation and odd chunk alignment", () =>
  setup(async (service, client) => {
    service.mode = "odd_chunks";
    assert.deepEqual(
      await collect(client.tts.stream("Hello.")),
      service.payload,
    );
    service.mode = "wrong_format";
    const audio = client.tts.stream("Hello.");
    await assert.rejects(() => collect(audio), RimeAudioFormatError);
    await audio.cancel();
  }));
test("discovery retries with a shared connection", () =>
  setup(async (service, client) => {
    service.discoveryFailures = 1;
    assert.deepEqual(
      await Promise.all([
        client.voices.list({ language: "en" }),
        client.languages.list(),
      ]),
      [["test-speaker"], ["en", "de"]],
    );
    assert.equal(service.discoveryCalls, 2);
  }));

test("UTF-16 chunk boundaries preserve surrogate pairs", async () => {
  await ready;
  const text = "Hello 🚀 world. The next sentence is complete.";
  const buffer = new SentenceBuffer(65536);
  const result = [];
  for (let i = 0; i < text.length; i++)
    result.push(...buffer.feed(text.slice(i, i + 1)));
  result.push(...buffer.feed("", true));
  assert.equal(result.join(""), text);
});
test("timeout inheritance and explicit disablement", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const inheritedClient = new Rime({ apiKey: "test", timeout: 0.03 });
    try {
      const a = inheritedClient.tts.stream("Hi.");
      await a.next();
      await sleep(60);
      await assert.rejects(() => a.next(), RimeTimeoutError);
      await a.cancel();
      const b = inheritedClient.tts.stream("Hello.", { timeout: null });
      await b.next();
      await sleep(60);
      assert.equal((await b.next()).done, false);
      await b.cancel();
    } finally {
      await inheritedClient.close();
    }
  }));
test("early loop exit cancels and readonly format", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const audio = client.tts.stream("Hello.");
    assert.throws(() => {
      audio.format = AudioFormat.MULAW_8000;
    }, TypeError);
    for await (const _ of audio) break;
    await assert.rejects(() => audio.next(), RimeCancelledError);
  }));
test("shared audio bytes", async () => {
  const { readFile } = await import("node:fs/promises");
  const fixture = JSON.parse(
    await readFile(
      new URL("../../conformance/audio.json", import.meta.url),
      "utf8",
    ),
  );
  for (const size of fixture.chunk_bytes) {
    const raw = Buffer.from(fixture.pcm_hex, "hex");
    const converter = new Converter(AudioFormat.MULAW_8000);
    const chunks = [];
    for (let i = 0; i < raw.length; i += size)
      chunks.push(converter.process(raw.subarray(i, i + size)));
    chunks.push(converter.process(new Uint8Array(), true));
    assert.equal(Buffer.concat(chunks).toString("hex"), fixture.mulaw_hex);
  }
});

test("source errors keep their cause and release operation state", async () => {
  await setup(async (_, client) => {
    const cause = Object.assign(new Error("application source failed"), {
      code: "ENOENT",
    });
    async function* source() {
      throw cause;
    }
    const audio = client.tts.stream(source());
    await assert.rejects(
      collect(audio),
      (error) => error instanceof RimeInputError && error.cause === cause,
    );
    await sleep(10);
    assert.equal(client.streams.size, 0);
  });
});

for (const code of [undefined, 14]) {
  test(`iterator creation errors preserve their cause / ${code}`, () =>
    setup(async (_, client) => {
      const cause = Object.assign(new Error("iterator creation failed"), {
        code,
      });
      const source = {
        [Symbol.asyncIterator]() {
          throw cause;
        },
      };
      const audio = client.tts.stream(source);
      await assert.rejects(
        collect(audio),
        (error) => error instanceof RimeInputError && error.cause === cause,
      );
      await audio.cancel();
      assert.equal(client.streams.size, 0);
    }));
}

for (const operation of ["voices", "languages"])
  for (const requestId of ["discovery-id", null])
    for (const mode of ["discovery_timeout", "discovery_error"])
      test(`discovery deadline preserves headers / ${operation} / ${requestId} / ${mode}`, () =>
        setup(async (service, client) => {
          service.mode = mode;
          service.responseMetadata = requestId
            ? { "x-request-id": requestId }
            : {};
          await assert.rejects(
            client[operation].list({ timeout: 0.04 }),
            (error) =>
              error instanceof RimeTimeoutError &&
              error.requestId === requestId,
          );
        }));

for (const size of [1024, 100000])
  for (const [char, count] of [
    ["a", 65533],
    ["a", 65535],
    ["é", 32767],
  ])
    test(`sentence limit excludes lookahead / ${size} / ${char} / ${count}`, async () => {
      await ready;
      const first = char.repeat(count) + ".";
      const text = first + " Short.";
      const buffer = new SentenceBuffer(65536);
      const actual = [];
      for (let offset = 0; offset < text.length; offset += size)
        actual.push(...buffer.feed(text.slice(offset, offset + size)));
      actual.push(...buffer.feed("", true));
      assert.deepEqual(actual, [first, " Short."]);
    });

test("sentence limit still rejects oversized spans", async () => {
  await ready;
  for (const text of [
    "a".repeat(65536) + ". Short.",
    "é".repeat(32769),
    " ".repeat(65537),
  ]) {
    const buffer = new SentenceBuffer(65536);
    assert.throws(() => {
      [...buffer.feed(text)];
      [...buffer.feed("", true)];
    }, RimeResourceLimitError);
  }
});

test("client shutdown stops paused output", async () => {
  await setup(async (service, client) => {
    service.mode = "burst";
    const audio = client.tts.stream("Hello.");
    await audio.next();
    await sleep(30);
    await client.close();
    assert.equal(client.streams.size, 0);
    await assert.rejects(audio.next(), RimeCancelledError);
    assert.throws(() => client.tts.stream("Later."), RimeInputError);
  });
});

for (const profile of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
  test(`large audio is delivered in bounded chunks / ${profile.encoding}`, () =>
    setup(async (service, client) => {
      const samples = policy.outputBytes + 1;
      service.payload = Buffer.alloc(samples * 2);
      const chunks = [];
      for await (const part of client.tts.stream("Hello.", {
        audioFormat: profile,
      }))
        chunks.push(part);
      assert.ok(
        chunks.every(
          (part) => part.length > 0 && part.length <= policy.outputChunkBytes,
        ),
      );
      const expected =
        profile === AudioFormat.PCM_24000
          ? service.payload
          : Buffer.alloc(Math.ceil(samples / 3), 255);
      assert.deepEqual(Buffer.concat(chunks), expected);
    }));
}

for (const queued of [false, true]) {
  test(`overall timeout after producer completion / queued=${queued}`, () =>
    setup(async (service, client) => {
      service.payload = Buffer.alloc(queued ? policy.outputChunkBytes * 2 : 2);
      const audio = client.tts.stream("Hello.", { timeout: 0.1 });
      await audio.next();
      // Production has ended, but the consumer has not observed completion.
      await audio.native.waitProduced();
      await sleep(150);
      await assert.rejects(audio.next(), RimeTimeoutError);
    }));
}

for (const stop of ["complete", "timeout", "cancel"]) {
  test(`EOF waits for source cleanup / ${stop}`, () =>
    setup(async (_service, client) => {
      let release, observedEof;
      let closed = false;
      const cleanup = new Promise((resolve) => {
        release = resolve;
      });
      const eof = new Promise((resolve) => {
        observedEof = resolve;
      });
      const source = {
        sent: false,
        [Symbol.asyncIterator]() {
          return this;
        },
        async next() {
          if (this.sent) return { done: true };
          this.sent = true;
          return { done: false, value: "Hello." };
        },
        async return() {
          await cleanup;
          closed = true;
          return { done: true };
        },
      };
      const audio = client.tts.stream(source, {
        timeout: stop === "timeout" ? 0.1 : null,
      });
      const read = audio.native.read.bind(audio.native);
      audio.native.read = async () => {
        const result = await read();
        if (result.data == null) observedEof();
        return result;
      };
      const pending = collect(audio);
      let cancellation;
      try {
        await eof;
        // Let next() reach the cleanup wait before stopping the stream.
        await sleep(0);
        assert.equal(closed, false);
        if (stop === "timeout") await sleep(150);
        else if (stop === "cancel") cancellation = audio.cancel();
        release();
        if (stop === "complete") {
          assert.ok((await pending).length);
          assert.equal((await audio.next()).done, true);
        } else {
          const error =
            stop === "timeout" ? RimeTimeoutError : RimeCancelledError;
          await assert.rejects(pending, error);
          await assert.rejects(audio.next(), error);
        }
        assert.equal(closed, true);
        assert.equal(client.streams.size, 0);
      } finally {
        release();
        await Promise.allSettled([pending, cancellation]);
      }
    }));
}

test("slow consumer does not trigger stall timeout", () =>
  setup(async (service, client) => {
    const original = policy.progressTimeout;
    policy.progressTimeout = 0.02;
    // Keep the RPC open while a large response waits for output capacity.
    service.mode = "partial_error";
    service.payload = Buffer.alloc(policy.outputBytes * 2);
    const audio = client.tts.stream("Hello.");
    try {
      assert.ok((await audio.next()).value.length);
      await sleep(80);
      assert.ok((await audio.next()).value.length);
    } finally {
      policy.progressTimeout = original;
      await audio.cancel();
    }
  }));

test("incomplete final PCM sample fails", async () => {
  await setup(async (service, client) => {
    service.payload = Buffer.from([1]);
    await assert.rejects(
      collect(client.tts.stream("Hello.")),
      RimeAudioFormatError,
    );
  });
});

for (const queued of [true, false]) {
  test(`cancellation after queue read with queued audio=${queued}`, () =>
    setup(async (service, client) => {
      service.payload = Buffer.alloc(queued ? policy.outputChunkBytes * 2 : 2);
      const audio = client.tts.stream("Hello.");
      await audio.next();
      await audio.native.waitProduced();
      const pending = audio.next();
      const cancelled = audio.cancel();
      await assert.rejects(pending, RimeCancelledError);
      await cancelled;
      await assert.rejects(audio.next(), RimeCancelledError);
    }));
}

for (const location of ["headers", "trailers", "both"]) {
  for (const [status, errorType] of [
    [grpc.status.PERMISSION_DENIED, RimePermissionError],
    [grpc.status.UNAUTHENTICATED, RimeAuthenticationError],
    [grpc.status.UNAVAILABLE, RimeUnavailableError],
    [grpc.status.OUT_OF_RANGE, RimeStreamError],
    [grpc.status.UNKNOWN, RimeStreamError],
  ]) {
    test(`server error without audio metadata: ${status}, ${location}`, () =>
      setup(async (service, client) => {
        service.mode = "no_audio_error";
        service.rejectionStatus = status;
        service.responseMetadata =
          location === "trailers" ? {} : { "x-request-id": "header-id" };
        service.trailingMetadata =
          location === "headers" ? {} : { "x-request-id": "trailer-id" };
        const expectedId = location === "trailers" ? "trailer-id" : "header-id";
        const audio = client.tts.stream("Hello.", { timeout: 1 });
        const result = collect(audio);
        // Attach the error check before the server can reject the request.
        const checked = assert.rejects(
          result,
          (error) =>
            error instanceof errorType && error.requestId === expectedId,
        );
        checked.catch(() => {});
        await service.headersSent;
        // Send the final rejection separately from the initial headers.
        await sleep(10);
        service.release();
        await checked;
        assert.equal(audio.requestId, expectedId);
        assert.equal(service.calls.length, 1);
      }));
  }
  for (const namespace of ["voices", "languages"]) {
    test(`discovery error keeps request ID: ${namespace}, ${location}`, () =>
      setup(async (service, client) => {
        service.mode = "discovery_error";
        service.rejectionStatus = grpc.status.PERMISSION_DENIED;
        service.responseMetadata =
          location === "trailers" ? {} : { "x-request-id": "header-id" };
        service.trailingMetadata =
          location === "headers" ? {} : { "x-request-id": "trailer-id" };
        const expectedId = location === "trailers" ? "trailer-id" : "header-id";
        await assert.rejects(
          client[namespace].list(),
          (error) =>
            error instanceof RimePermissionError &&
            error.requestId === expectedId,
        );
      }));
  }
}

test("oversized audio response preserves the resource-limit error", () =>
  setup(async (service, client) => {
    service.payload = Buffer.alloc(4 * 1024 * 1024);
    const stream = client.tts.stream("Hello.");
    await assert.rejects(
      stream.next(),
      (error) =>
        error instanceof RimeResourceLimitError &&
        error.requestId === "test-request",
    );
  }));

for (const namespace of ["voices", "languages"]) {
  test(`oversized discovery response preserves the resource-limit error / ${namespace}`, () =>
    setup(async (service, client) => {
      service[namespace === "voices" ? "speakers" : "languages"] = [
        "x".repeat(4 * 1024 * 1024),
      ];
      await assert.rejects(client[namespace].list(), RimeResourceLimitError);
    }));

  test(`server OUT_OF_RANGE remains a stream error / ${namespace}`, () =>
    setup(async (service, client) => {
      service.mode = "discovery_error";
      service.rejectionStatus = grpc.status.OUT_OF_RANGE;
      // A server is allowed to use the same message as tonic's local size error.
      service.rejection = () =>
        Object.assign(
          new Error(
            "Error, decoded message length too large: found 4194305 bytes, the limit is: 4194304 bytes",
          ),
          { code: grpc.status.OUT_OF_RANGE },
        );
      await assert.rejects(client[namespace].list(), RimeStreamError);
    }));
}

for (const mode of ["normal", "empty_audio", "empty_no_headers"]) {
  test(`missing audio metadata cannot succeed: ${mode}`, () =>
    setup(async (service, client) => {
      service.mode = mode;
      service.responseMetadata = { "x-request-id": "header-id" };
      const audio = client.tts.stream("Hello.", { timeout: 1 });
      await assert.rejects(audio.next(), RimeAudioFormatError);
    }));
}

test("concurrent reads do not cancel the first read", () =>
  setup(async (service, client) => {
    service.mode = "silence";
    const stream = client.tts.stream("Hello.");
    const first = stream.next();
    await service.received;
    await assert.rejects(stream.next(), RimeInputError);
    service.release();
    assert.equal((await first).done, false);
    await stream.cancel();
  }));

test("Node worker environment can load and release the extension", async () => {
  const { Worker } = await import("node:worker_threads");
  const moduleUrl = new URL("../dist/index.js", import.meta.url).href;
  const worker = new Worker(
    `(async()=>{const {Rime}=await import(${JSON.stringify(moduleUrl)}); const client=new Rime({apiKey:'worker'}); const stream=client.tts.stream('Hello.');await stream.cancel();await client.close();})().catch(error=>{throw error;});`,
    { eval: true },
  );
  await new Promise((resolve, reject) => {
    worker.once("error", reject);
    worker.once("exit", (code) =>
      code ? reject(new Error(`worker exit ${code}`)) : resolve(),
    );
  });
});
