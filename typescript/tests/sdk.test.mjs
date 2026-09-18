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
} from "../dist/index.js";
import { SentenceBuffer, ready } from "../dist/sentences.js";
import { Converter } from "../dist/audio.js";
import { authentication } from "../dist/auth.js";
import { transport, rpcError } from "../dist/transport.js";
import { policy } from "../dist/policy.js";
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
  const oldFactory = transport.makeClient,
    oldExchange = authentication.exchangeKey;
  transport.makeClient = () =>
    new grpc.Client(service.target, grpc.credentials.createInsecure(), {
      "grpc.enable_retries": 0,
    });
  authentication.exchangeKey = async () => ({
    value: "test-token",
    expiresAt: Date.now() / 1000 + 3600,
    audience: policy.audience,
  });
  const client = new Rime({ apiKey: "test-key" });
  try {
    await fn(service, client);
  } finally {
    await client.close();
    service.close();
    transport.makeClient = oldFactory;
    authentication.exchangeKey = oldExchange;
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
    assert.ok(Buffer.byteLength(buffer.pending) < 2048);
  }
  output.push(...buffer.feed("", true));
  assert.equal(output.join(""), text);
  assert.equal(buffer.pending, "");
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
test("error conformance", () => {
  for (const [status, name] of Object.entries(contract.grpc_errors)) {
    const error = rpcError(grpc.status[status], "id");
    assert.equal(error.name, name);
    assert.equal(error.requestId, "id");
    assert.equal(error.grpcStatus, undefined);
  }
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
      "Bearer test-token",
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
test("lazy startup and cancellation before start", () =>
  setup(async (service, client) => {
    const audio = client.tts.stream("Hi.", { timeout: 0.01 });
    await sleep(30);
    assert.equal(service.calls.length, 0);
    await audio.cancel();
    await audio.cancel();
    await assert.rejects(() => audio.next(), RimeCancelledError);
  }));
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
test("shared refresh and discovery retries", () =>
  setup(async (service, client) => {
    let calls = 0;
    authentication.exchangeKey = async () => {
      calls++;
      await sleep(20);
      return {
        value: "token",
        expiresAt: Date.now() / 1000 + 3600,
        audience: policy.audience,
      };
    };
    service.discoveryFailures = 1;
    assert.deepEqual(
      await Promise.all([
        client.voices.list({ language: "en" }),
        client.languages.list(),
      ]),
      [["test-speaker"], ["en", "de"]],
    );
    assert.equal(calls, 1);
  }));

test("private Themis HTTP exchange", async () => {
  const { createServer } = await import("node:http");
  const { exchangeKey } = await import("../dist/auth.js");
  let status = 200;
  let body = {
    access_token: "token",
    expires_in: 60,
    audience: policy.audience,
  };
  const server = createServer(async (request, response) => {
    assert.equal(request.headers.authorization, "Api-Key local-test-secret");
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    assert.deepEqual(JSON.parse(Buffer.concat(chunks).toString()), {
      audience: policy.audience,
    });
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const original = policy.exchangeUrl;
  policy.exchangeUrl = `http://127.0.0.1:${server.address().port}/v1/token`;
  try {
    assert.equal(
      (await exchangeKey("local-test-secret", new AbortController().signal))
        .value,
      "token",
    );
    for (const [code, errorType] of [
      [401, RimeAuthenticationError],
      [403, RimePermissionError],
      [429, RimeResourceLimitError],
      [500, RimeUnavailableError],
      [502, RimeUnavailableError],
      [503, RimeUnavailableError],
      [504, RimeUnavailableError],
    ]) {
      status = code;
      await assert.rejects(
        () => exchangeKey("local-test-secret", new AbortController().signal),
        (error) =>
          error instanceof errorType &&
          !error.message.includes("local-test-secret"),
      );
    }
    status = 200;
    body = { ...body, audience: "wrong" };
    await assert.rejects(
      () => exchangeKey("local-test-secret", new AbortController().signal),
      RimeAuthenticationError,
    );
  } finally {
    policy.exchangeUrl = original;
    await new Promise((resolve) => server.close(resolve));
  }
});

for (const [status, errorType] of [
  [401, RimeAuthenticationError],
  [403, RimePermissionError],
  [429, RimeResourceLimitError],
  [503, RimeUnavailableError],
]) {
  test(`HTTP ${status} closes an unfinished authentication response`, async () => {
    const { createServer } = await import("node:http");
    const { Credentials } = await import("../dist/auth.js");
    let markClosed;
    const closed = new Promise((resolve) => {
      markClosed = resolve;
    });
    const server = createServer((request, response) => {
      request.resume();
      response.on("close", markClosed);
      response.writeHead(status, { "content-type": "text/plain" });
      response.write("local-test-secret");
      // Keep the body open. The SDK must cancel it when rejecting the status.
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const original = policy.exchangeUrl;
    policy.exchangeUrl = `http://127.0.0.1:${server.address().port}/v1/token`;
    const credentials = new Credentials("local-test-secret");
    let timer;
    try {
      await assert.rejects(
        credentials.metadata(new AbortController().signal),
        errorType,
      );
      await Promise.race([
        closed,
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error("Authentication response stayed open")),
            1000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
      await credentials.close();
      policy.exchangeUrl = original;
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    }
  });
}

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
test("cancel during shared refresh", () =>
  setup(async (service, client) => {
    let release;
    const wait = new Promise((resolve) => (release = resolve));
    authentication.exchangeKey = async () => {
      await wait;
      return {
        value: "token",
        expiresAt: Date.now() / 1000 + 3600,
        audience: policy.audience,
      };
    };
    const a = client.tts.stream("A."),
      b = client.tts.stream("B.");
    const first = a.next();
    const check = assert.rejects(() => first, RimeCancelledError);
    const second = collect(b);
    await sleep(5);
    await a.cancel();
    release();
    await check;
    assert.ok((await second).length);
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
          assert.equal(client.discoveryControllers.size, 0);
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
      await audio.worker;
      await sleep(150);
      await assert.rejects(audio.next(), RimeTimeoutError);
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
      await audio.worker;
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

for (const mode of ["normal", "empty_audio", "empty_no_headers"]) {
  test(`missing audio metadata cannot succeed: ${mode}`, () =>
    setup(async (service, client) => {
      service.mode = mode;
      service.responseMetadata = { "x-request-id": "header-id" };
      const audio = client.tts.stream("Hello.", { timeout: 1 });
      await assert.rejects(audio.next(), RimeAudioFormatError);
    }));
}
