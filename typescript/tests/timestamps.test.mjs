import test from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import {
  Rime,
  AudioFormat,
  RimeInputError,
  RimeStreamError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../dist/index.js";
import { transport } from "../dist/tts/transport.js";
import { Converter } from "../dist/tts/audio.js";
import { FakeService } from "./service.mjs";

const words = [
  { text: "twenty", start: { nanos: 90000000 }, end: { nanos: 325000000 } },
  {
    text: "two",
    start: { seconds: 1n, nanos: 125000000 },
    end: { seconds: 2n },
  },
];
const expected = [
  { text: "twenty", start: 0.09, end: 0.325 },
  { text: "two", start: 1.125, end: 2 },
];
function trailer(code = 0, spans = []) {
  return create(schema.SynthesisResponseStreamSchema, {
    payload: {
      case: "trailer",
      value: {
        timestamps: {
          status: { code, message: code === 0 ? "" : "alignment unavailable" },
          spans,
        },
      },
    },
  });
}
async function collect(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}
async function setup(fn) {
  const service = await new FakeService().start();
  const factory = transport.makeClient;
  transport.makeClient = () =>
    new grpc.Client(service.target, grpc.credentials.createInsecure());
  const client = new Rime({
    apiKey: "test-key",
    model: "mistv3",
    endpoint: service.target,
  });
  try {
    await fn(service, client);
  } finally {
    await client.close();
    service.close();
    transport.makeClient = factory;
  }
}

for (const profile of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
  test(`timestamps preserve audio and synthesis offsets / ${profile.encoding}`, () =>
    setup(async (service, client) => {
      service.finalResponses = [trailer(0, words)];
      async function* text() {
        yield "First. ";
        yield "Twenty two.";
      }
      const stream = client.tts.stream(text(), {
        timestamps: true,
        audioFormat: profile,
      });
      const audio = await collect(stream);
      const converter = new Converter(profile);
      const raw = Buffer.concat(
        service.calls[0].slice(1).map(() => service.payload),
      );
      assert.deepEqual(audio, Buffer.from(converter.process(raw, true)));
      assert.equal(service.calls[0][0].payload.value.timestamps.enable, true);
      const result = await stream.timestamps();
      assert.deepEqual(result, {
        status: { code: 0, message: "" },
        spans: expected,
      });
      assert.ok(Object.isFrozen(result.spans[0]));
      await stream.cancel();
      await client.close();
      assert.deepEqual(await stream.timestamps(), result);
    }));
}

for (const code of [1, 3, 4, 8, 12, 14]) {
  test(`alignment failure keeps successful audio / ${code}`, () =>
    setup(async (service, client) => {
      service.finalResponses = [trailer(code)];
      const stream = client.tts.stream("Hello.", { timestamps: true });
      assert.deepEqual(await collect(stream), service.payload);
      assert.deepEqual(await stream.timestamps(), {
        status: { code, message: "alignment unavailable" },
        spans: [],
      });
    }));
}

for (const [name, responses] of [
  ["missing", []],
  [
    "empty trailer",
    [
      create(schema.SynthesisResponseStreamSchema, {
        payload: { case: "trailer", value: {} },
      }),
    ],
  ],
  [
    "missing status",
    [
      create(schema.SynthesisResponseStreamSchema, {
        payload: { case: "trailer", value: { timestamps: {} } },
      }),
    ],
  ],
  ["duplicate", [trailer(), trailer()]],
  [
    "audio after trailer",
    [
      trailer(),
      create(schema.SynthesisResponseStreamSchema, {
        payload: { case: "audio", value: new Uint8Array(2) },
      }),
    ],
  ],
  ["failed with spans", [trailer(14, words)]],
  ["missing durations", [trailer(0, [{ text: "missing" }])]],
  [
    "negative",
    [trailer(0, [{ text: "negative", start: { nanos: -1 }, end: {} }])],
  ],
  [
    "invalid nanos",
    [trailer(0, [{ text: "invalid", start: {}, end: { nanos: 1000000000 } }])],
  ],
  [
    "reversed",
    [
      trailer(0, [
        { text: "reversed", start: { seconds: 2n }, end: { seconds: 1n } },
      ]),
    ],
  ],
]) {
  test(`bad timestamp result does not fail audio / ${name}`, () =>
    setup(async (service, client) => {
      service.finalResponses = responses;
      const stream = client.tts.stream("Hello.", { timestamps: true });
      const audio = await collect(stream);
      assert.deepEqual(
        audio.subarray(0, service.payload.length),
        service.payload,
      );
      await assert.rejects(
        stream.timestamps(),
        (error) =>
          error instanceof RimeStreamError &&
          error.requestId === "test-request",
      );
    }));
}

test("timestamps opt-in and model validation", () =>
  setup(async (service, client) => {
    const coda = new Rime({ apiKey: "test-key" });
    try {
      assert.throws(
        () => coda.tts.stream("Hello.", { timestamps: true }),
        /mistv3/,
      );
      assert.equal(service.calls.length, 0);
    } finally {
      await coda.close();
    }
    for (const timestamps of [1, null, "true"])
      assert.throws(
        () => client.tts.stream("Hello.", { timestamps }),
        /boolean/,
      );
    for (const options of [{}, { timestamps: false }]) {
      const stream = client.tts.stream("Hello.", options);
      await collect(stream);
      assert.equal(service.calls.at(-1)[0].payload.value.timestamps, undefined);
      await assert.rejects(stream.timestamps(), /Enable/);
    }
  }));

test("audio arrives before timestamps; early access never waits", () =>
  setup(async (service, client) => {
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    service.beforeFinalResponses = () => gate;
    service.finalResponses = [trailer(0, words)];
    const stream = client.tts.stream("Hello.", {
      timestamps: true,
      timeout: 2,
    });
    try {
      await assert.rejects(stream.timestamps(), RimeInputError);
      assert.deepEqual(
        Buffer.from((await stream.next()).value),
        service.payload,
      );
      await assert.rejects(stream.timestamps(), RimeInputError);
      release();
      await collect(stream);
      assert.deepEqual((await stream.timestamps()).spans, expected);
    } finally {
      release();
      await stream.cancel();
    }
  }));

for (const mode of ["stream", "client", "before start"]) {
  test(`cancelled timestamps fail promptly / ${mode}`, () =>
    setup(async (service, client) => {
      let release;
      const gate = new Promise((resolve) => {
        release = resolve;
      });
      service.beforeFinalResponses = () => gate;
      const stream = client.tts.stream("Hello.", { timestamps: true });
      try {
        if (mode !== "before start") await stream.next();
        if (mode === "client") await client.close();
        else await stream.cancel();
        await assert.rejects(stream.timestamps(), RimeCancelledError);
      } finally {
        release();
      }
    }));
}

test("deadline fails audio and timestamp access", () =>
  setup(async (service, client) => {
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    service.beforeFinalResponses = () => gate;
    const stream = client.tts.stream("Hello.", {
      timestamps: true,
      timeout: 0.1,
    });
    try {
      await assert.rejects(collect(stream), RimeTimeoutError);
      await assert.rejects(stream.timestamps(), RimeTimeoutError);
    } finally {
      release();
    }
  }));

test("final RPC failure overrides a timestamp trailer", () =>
  setup(async (service, client) => {
    service.mode = "error_after_trailer";
    service.finalResponses = [trailer(0, words)];
    const stream = client.tts.stream("Hello.", { timestamps: true });
    await assert.rejects(collect(stream), RimeUnavailableError);
    await assert.rejects(stream.timestamps(), RimeUnavailableError);
  }));

for (const audioFormat of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
  test(`full-text synthesis keeps timestamp completion and audio profiles / ${audioFormat.encoding}`, () =>
    setup(async (service, client) => {
      service.finalResponses = [trailer(0, words)];
      const stream = client.tts.stream("Twenty two.", {
        completeText: true,
        timestamps: true,
        audioFormat,
      });
      await assert.rejects(stream.timestamps(), RimeInputError);
      const audio = await collect(stream);
      const converter = new Converter(audioFormat);
      assert.deepEqual(
        audio,
        Buffer.from(converter.process(service.payload, true)),
      );
      assert.deepEqual((await stream.timestamps()).spans, expected);
      assert.equal(service.completeCalls[0].timestamps.enable, true);
    }));
}
