import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import * as grpc from "@grpc/grpc-js";
import * as schema from "@rimelabs/api";
import { create } from "@bufbuild/protobuf";
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
import { Converter } from "../dist/tts/audio.js";
import { authentication, Credentials } from "../dist/auth.js";
import { transport, rpcError } from "../dist/tts/transport.js";
import { policy } from "../dist/tts/policy.js";
import { FakeService } from "./service.mjs";
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
  // Keep exercising the retained Themis path while direct auth is temporary.
  const oldMetadata = Credentials.prototype.metadata;
  Credentials.prototype.metadata = Credentials.prototype.themisMetadata;
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
    Credentials.prototype.metadata = oldMetadata;
  }
}
test("temporary bearer auth", async () => {
  const credentials = new Credentials("test-key", policy);
  const signal = new AbortController().signal;
  assert.equal(await credentials.metadata(signal), "Bearer test-key");
  await credentials.close();
  await assert.rejects(credentials.metadata(signal), /closed/);
});
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
  assert.throws(() => client.tts.synthesize(""), RimeInputError);
  assert.throws(
    () => client.tts.synthesize("Hi", { audioFormat: "wav" }),
    RimeAudioFormatError,
  );
});
test("synthesis methods validate input shape", () =>
  setup(async (service, client) => {
    async function* source() {
      yield "Hello.";
    }
    for (const invalid of ["Hello.", "", ["Hello."], null, 123])
      assert.throws(() => client.tts.stream(invalid), {
        name: "RimeInputError",
        message: /async iterable/,
      });
    const chunks = source();
    try {
      for (const invalid of [chunks, ["Hello."], null, 123])
        assert.throws(() => client.tts.synthesize(invalid), {
          name: "RimeInputError",
          message: /must be a string/,
        });
    } finally {
      await chunks.return();
    }
    assert.equal(service.calls.length, 0);
  }));
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
    const audio = client.tts.synthesize("Hello. Final phrase");
    assert.equal(audio.format, AudioFormat.PCM_24000);
    assert.deepEqual(await collect(audio), service.payload);
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
      yield "Hello ";
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
    const audio = client.tts.synthesize(
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
    const first = client.tts.synthesize("Hello.");
    assert.equal((await first.next()).done, false);
    await first.cancel();
    await assert.rejects(() => first.next(), RimeCancelledError);
    service.mode = "normal";
    assert.ok((await collect(client.tts.synthesize("Sibling."))).length);
  }));
test("overall_timeout_while_paused", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const audio = client.tts.synthesize("Hello.", { timeout: 0.08 });
    assert.equal((await audio.next()).done, false);
    await sleep(130);
    await assert.rejects(() => audio.next(), RimeTimeoutError);
    await audio.cancel();
  }));
test("lazy startup and cancellation before start", () =>
  setup(async (service, client) => {
    const audio = client.tts.synthesize("Hi.", { timeout: 0.01 });
    await sleep(30);
    assert.equal(service.calls.length, 0);
    await audio.cancel();
    await audio.cancel();
    await assert.rejects(() => audio.next(), RimeCancelledError);
  }));
test("no_synthesis_replay", () =>
  setup(async (service, client) => {
    service.mode = "error_before_audio";
    const audio = client.tts.synthesize("Hello.");
    await assert.rejects(() => collect(audio), RimeUnavailableError);
    await audio.cancel();
    assert.equal(service.calls.length, 1);
  }));
test("format validation and odd chunk alignment", () =>
  setup(async (service, client) => {
    service.mode = "odd_chunks";
    assert.deepEqual(
      await collect(client.tts.synthesize("Hello.")),
      service.payload,
    );
    service.mode = "wrong_format";
    const audio = client.tts.synthesize("Hello.");
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
  const configuration = {
    exchangeUrl: "",
    audience: "auth.example",
    authTimeout: 10,
  };
  let status = 200;
  let body = {
    access_token: "token",
    expires_in: 60,
    audience: configuration.audience,
  };
  const server = createServer(async (request, response) => {
    assert.equal(request.headers.authorization, "Api-Key local-test-secret");
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    assert.deepEqual(JSON.parse(Buffer.concat(chunks).toString()), {
      audience: configuration.audience,
    });
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  configuration.exchangeUrl = `http://127.0.0.1:${server.address().port}/v1/token`;
  try {
    assert.equal(
      (
        await exchangeKey(
          "local-test-secret",
          new AbortController().signal,
          configuration,
        )
      ).value,
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
        () =>
          exchangeKey(
            "local-test-secret",
            new AbortController().signal,
            configuration,
          ),
        (error) =>
          error instanceof errorType &&
          !error.message.includes("local-test-secret"),
      );
    }
    status = 200;
    body = { ...body, audience: "wrong" };
    await assert.rejects(
      () =>
        exchangeKey(
          "local-test-secret",
          new AbortController().signal,
          configuration,
        ),
      RimeAuthenticationError,
    );
  } finally {
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
    const credentials = new Credentials("local-test-secret", {
      exchangeUrl: `http://127.0.0.1:${server.address().port}/v1/token`,
      audience: "auth.example",
      authTimeout: 10,
    });
    let timer;
    try {
      await assert.rejects(
        credentials.themisMetadata(new AbortController().signal),
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
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    }
  });
}

test("timeout inheritance and explicit disablement", () =>
  setup(async (service, client) => {
    service.mode = "burst";
    const inheritedClient = new Rime({ apiKey: "test", timeout: 0.03 });
    try {
      const a = inheritedClient.tts.synthesize("Hi.");
      await a.next();
      await sleep(60);
      await assert.rejects(() => a.next(), RimeTimeoutError);
      await a.cancel();
      const b = inheritedClient.tts.synthesize("Hello.", { timeout: null });
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
    const audio = client.tts.synthesize("Hello.");
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
    const a = client.tts.synthesize("A."),
      b = client.tts.synthesize("B.");
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
    assert.equal(client.ttsClient.streams.size, 0);
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
      assert.equal(client.ttsClient.streams.size, 0);
    }));
}

for (const operation of ["voices", "languages"])
  test(`close waits for discovery cleanup / ${operation}`, () =>
    setup(async (service, client) => {
      service.mode = "discovery_timeout";
      const discovery = assert.rejects(
        client[operation].list(),
        RimeCancelledError,
      );
      await service.headersSent;
      const connection = client.ttsClient.client;
      const close = connection.close.bind(connection);
      let activeAtClose;
      let closeCalls = 0;
      connection.close = () => {
        activeAtClose = client.ttsClient.discoveryOperations.size;
        closeCalls++;
        close();
      };

      await Promise.all([client.close(), client.close()]);
      await discovery;

      assert.equal(activeAtClose, 0);
      assert.equal(closeCalls, 1);
    }));

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
          assert.equal(client.ttsClient.discoveryOperations.size, 0);
        }));

test("client shutdown stops paused output", async () => {
  await setup(async (service, client) => {
    service.mode = "burst";
    const audio = client.tts.synthesize("Hello.");
    await audio.next();
    await sleep(30);
    await client.close();
    assert.equal(client.ttsClient.streams.size, 0);
    await assert.rejects(audio.next(), RimeCancelledError);
    assert.throws(() => client.tts.synthesize("Later."), RimeInputError);
  });
});

for (const profile of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
  test(`large audio is delivered in bounded chunks / ${profile.encoding}`, () =>
    setup(async (service, client) => {
      const samples = policy.outputBytes + 1;
      service.payload = Buffer.alloc(samples * 2);
      const chunks = [];
      for await (const part of client.tts.synthesize("Hello.", {
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
      const audio = client.tts.synthesize("Hello.", { timeout: 0.1 });
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
    const audio = client.tts.synthesize("Hello.");
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
      collect(client.tts.synthesize("Hello.")),
      RimeAudioFormatError,
    );
  });
});

for (const queued of [true, false]) {
  test(`cancellation after queue read with queued audio=${queued}`, () =>
    setup(async (service, client) => {
      service.payload = Buffer.alloc(queued ? policy.outputChunkBytes * 2 : 2);
      const audio = client.tts.synthesize("Hello.");
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
        const audio = client.tts.synthesize("Hello.", { timeout: 1 });
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
      const audio = client.tts.synthesize("Hello.", { timeout: 1 });
      await assert.rejects(audio.next(), RimeAudioFormatError);
    }));
}

for (const entry of contract.output_cases) {
  for (const incremental of [false, true]) {
    test(`output ${entry.format}/${entry.sample_rate}, incremental=${incremental}`, () =>
      setup(async (service, client) => {
        service.mode = "odd_chunks";
        service.responseMetadata["x-rime-audio-content-type"] =
          entry.wire_format;
        service.payload = ["WAV", "MP3"].includes(entry.format)
          ? Buffer.from("encoded")
          : Buffer.from([1, 0, 2, 0, 3, 0]);
        const options = {
          audioFormat: AudioFormat[entry.format],
          sampleRate: entry.sample_rate,
          timeScaleFactor: 0.8,
        };
        const audio = incremental
          ? client.tts.stream(
              (async function* () {
                yield "Hello.";
              })(),
              options,
            )
          : client.tts.synthesize("Hello.", options);
        assert.equal(audio.sampleRate, entry.sample_rate);
        const result = await collect(audio);
        if (entry.format !== "MULAW_8000")
          assert.deepEqual(result, service.payload);
        else assert.equal(result.length, 1);
        const params = service.calls[0][0].payload.value.audioParameters;
        assert.equal(params.audioFormat, entry.wire_format);
        assert.equal(params.samplingRate, entry.wire_rate);
        assert.ok(Math.abs(params.timeScaleFactor - 0.8) < 1e-6);
      }));
  }
}

test("buffer output validation happens before an RPC", () =>
  setup(async (service, client) => {
    for (const sampleRate of [
      ...contract.invalid_sample_rates,
      true,
      "8000",
      8000.5,
    ])
      assert.throws(
        () => client.tts.synthesize("Hello.", { sampleRate }),
        RimeAudioFormatError,
      );
    for (const timeScaleFactor of [
      ...contract.invalid_time_scale_factors,
      NaN,
      Infinity,
      true,
      "1",
    ])
      assert.throws(
        () => client.tts.synthesize("Hello.", { timeScaleFactor }),
        RimeInputError,
      );
    for (const [audioFormat, sampleRate] of [
      [AudioFormat.PCM_24000, 8000],
      [AudioFormat.MULAW_8000, 16000],
    ])
      assert.throws(
        () => client.tts.synthesize("Hello.", { audioFormat, sampleRate }),
        RimeAudioFormatError,
      );
    assert.equal(service.calls.length, 0);
  }));

for (const entry of contract.wav_cases) {
  test(`buffered WAV ${entry.name}`, () =>
    setup(async (service, client) => {
      service.mode = "odd_chunks";
      service.responseMetadata["x-rime-audio-content-type"] = "audio/wav";
      service.payload = Buffer.from(entry.input_hex, "hex");
      const result = client.tts.synthesizeToBuffer("Hello.", {
        audioFormat: AudioFormat.WAV,
        sampleRate: 16000,
      });
      if (entry.error)
        await assert.rejects(result, {
          name: "RimeAudioFormatError",
          requestId: "test-request",
        });
      else assert.equal((await result).toString("hex"), entry.output_hex);
      assert.equal(
        service.calls[0][0].payload.value.audioParameters.timeScaleFactor,
        undefined,
      );
    }));
}

test("buffered MP3 preserves odd-length encoded output", () =>
  setup(async (service, client) => {
    service.mode = "odd_chunks";
    service.responseMetadata["x-rime-audio-content-type"] = "audio/mpeg";
    service.payload = Buffer.from("odd-mp3");
    assert.deepEqual(
      await client.tts.synthesizeToBuffer("Hello.", {
        audioFormat: AudioFormat.MP3,
      }),
      service.payload,
    );
  }));

for (const [mode, options, name] of [
  ["error_after_trailer", {}, "RimeUnavailableError"],
  ["silence", { timeout: 0.1 }, "RimeTimeoutError"],
  ["wrong_format", {}, "RimeAudioFormatError"],
]) {
  test(`buffer failure ${name} preserves request ID and client`, () =>
    setup(async (service, client) => {
      service.mode = mode;
      await assert.rejects(client.tts.synthesizeToBuffer("Hello.", options), {
        name,
        requestId: "test-request",
      });
      service.mode = "normal";
      assert.deepEqual(
        await client.tts.synthesizeToBuffer("Still works."),
        service.payload,
      );
    }));
}

test("client close cancels pending buffered synthesis", () =>
  setup(async (service, client) => {
    service.mode = "silence";
    const pending = client.tts.synthesizeToBuffer("Hello.");
    const rejected = assert.rejects(pending, RimeCancelledError);
    await service.textReceived;
    await client.close();
    await rejected;
  }));

for (const [sampleRate, timeScaleFactor] of [
  [8000, 0.4],
  [16000, 1],
  [24000, 2.5],
]) {
  test(`sample rate without format and factor boundary ${sampleRate}/${timeScaleFactor}`, () =>
    setup(async (service, client) => {
      const audio = client.tts.synthesize("Hello.", {
        sampleRate,
        timeScaleFactor,
      });
      assert.throws(() => {
        audio.sampleRate = 48000;
      }, TypeError);
      assert.deepEqual(await collect(audio), service.payload);
      const params = service.calls[0][0].payload.value.audioParameters;
      assert.equal(params.samplingRate, sampleRate);
      assert.ok(Math.abs(params.timeScaleFactor - timeScaleFactor) < 1e-6);
    }));
}

for (const extraBytes of [0, 2]) {
  test(`fixed 64 MiB buffer limit, extra bytes=${extraBytes}`, () =>
    setup(async (service, client) => {
      service.payload = Buffer.alloc(1024 * 1024);
      service.finalResponses = Array(63).fill(
        create(schema.SynthesisResponseStreamSchema, {
          payload: { case: "audio", value: service.payload },
        }),
      );
      if (extraBytes)
        service.finalResponses.push(
          create(schema.SynthesisResponseStreamSchema, {
            payload: { case: "audio", value: Buffer.alloc(extraBytes) },
          }),
        );
      const result = client.tts.synthesizeToBuffer("Hello.");
      if (extraBytes)
        await assert.rejects(result, {
          name: "RimeResourceLimitError",
          requestId: "test-request",
          message: /64 MiB/,
        });
      else assert.equal((await result).length, 64 * 1024 * 1024);
      service.finalResponses = [];
      service.payload = Buffer.from([1, 0]);
      assert.deepEqual(
        await client.tts.synthesizeToBuffer("Still works."),
        service.payload,
      );
    }));
}
