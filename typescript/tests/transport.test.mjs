import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { SynthesisCall, discover } from "../dist/transport.js";
import {
  RimeAudioFormatError,
  RimeAuthenticationError,
  RimePermissionError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../dist/errors.js";
import { FakeService } from "./service.mjs";

async function setup(fn) {
  const server = await new FakeService().start();
  const client = new grpc.Client(
    server.target,
    grpc.credentials.createInsecure(),
    {
      "grpc.enable_retries": 0,
    },
  );
  const metadata = new grpc.Metadata();
  metadata.set("authorization", "Bearer test-token");
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(new RimeTimeoutError("Test timed out")),
    2000,
  );
  try {
    await fn(server, { client, metadata }, controller.signal);
  } finally {
    clearTimeout(timer);
    client.close();
    server.close();
  }
}

for (const [status, errorType] of [
  [grpc.status.UNAVAILABLE, RimeUnavailableError],
  [grpc.status.UNAUTHENTICATED, RimeAuthenticationError],
  [grpc.status.PERMISSION_DENIED, RimePermissionError],
]) {
  test(`write after rejection without reader / ${status}`, () =>
    setup(async (server, prepared, signal) => {
      server.mode = "error_before_audio";
      server.rejectionStatus = status;
      server.trailingMetadata = { "x-request-id": "rejected-request" };
      const call = new SynthesisCall(prepared, signal);
      try {
        await call.start("clementine", "en");
        while (!call.done) await sleep(1, undefined, { signal });
        await assert.rejects(
          call.write("Hello."),
          (error) =>
            error instanceof errorType &&
            error.requestId === "rejected-request",
        );
        assert.equal(call.requestId, "rejected-request");
        assert.equal(server.calls.length, 1);
      } finally {
        call.cancel();
      }
    }));
}

for (const mode of ["odd_chunks", "headers_after_text"]) {
  test(`audio and wire order / ${mode}`, () =>
    setup(async (server, prepared, signal) => {
      server.mode = mode;
      server.trailingMetadata = { "x-request-id": "trailer-id" };
      const call = new SynthesisCall(prepared, signal);
      try {
        await call.start("voice", "de");
        await call.write("Hello.");
        call.finishInput();
        const chunks = [];
        for await (const part of call.audio()) chunks.push(part);
        assert.deepEqual(Buffer.concat(chunks), server.payload);
        if (mode === "odd_chunks")
          assert.deepEqual(chunks, [
            server.payload.subarray(0, 1),
            server.payload.subarray(1),
          ]);
        assert.equal(call.requestId, "test-request");
        assert.equal(server.calls.length, 1);
        assert.deepEqual(
          server.calls[0].map((m) => m.payload.case),
          ["header", "textChunk"],
        );
        const header = server.calls[0][0].payload.value;
        assert.equal(header.speaker, "voice");
        assert.equal(header.language, "de");
        assert.equal(header.audioParameters.audioFormat, "audio/pcm");
        assert.equal(
          server.metadata[0].get("authorization")[0],
          "Bearer test-token",
        );
      } finally {
        call.cancel();
      }
    }));
}

for (const mode of ["empty_no_headers", "empty_audio"]) {
  test(`empty success requires audio metadata / ${mode}`, () =>
    setup(async (server, prepared, signal) => {
      server.mode = mode;
      server.responseMetadata = {};
      const call = new SynthesisCall(prepared, signal);
      try {
        await call.start("voice", "en");
        call.finishInput();
        await assert.rejects(call.audio().next(), RimeAudioFormatError);
      } finally {
        call.cancel();
      }
    }));
}

test("discovery transport does not retry", () =>
  setup(async (server, prepared, signal) => {
    server.discoveryFailures = 1;
    await assert.rejects(
      discover(prepared, "languages", undefined, signal),
      RimeUnavailableError,
    );
    assert.equal(server.discoveryCalls, 1);
  }));

for (const [name, payload] of [
  ["empty payload", { case: undefined }],
  [
    "trailer",
    { case: "trailer", value: { timestamps: { status: { code: 0 } } } },
  ],
  [
    "failed timestamps",
    { case: "trailer", value: { timestamps: { status: { code: 14 } } } },
  ],
]) {
  test(`non-audio responses do not enter audio stream / ${name}`, () =>
    setup(async (server, prepared, signal) => {
      server.finalResponses = [
        create(schema.SynthesisResponseStreamSchema, { payload }),
      ];
      const call = new SynthesisCall(prepared, signal);
      try {
        await call.start("voice", "en");
        await call.write("Hello.");
        call.finishInput();
        const chunks = [];
        for await (const part of call.audio()) chunks.push(part);
        assert.deepEqual(
          chunks.map((chunk) => Buffer.from(chunk)),
          [server.payload],
        );
        assert.equal(server.calls[0][0].payload.value.timestamps, undefined);
      } finally {
        call.cancel();
      }
    }));
}
