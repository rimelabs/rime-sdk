import test from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { Rime, AudioFormat, RimeInputError } from "../dist/index.js";
import { transport } from "../dist/tts/transport.js";
import { rpcError } from "../dist/grpc.js";
import { FakeService } from "./service.mjs";

async function setup(fn) {
  const service = await new FakeService().start();
  const factory = transport.makeClient;
  transport.makeClient = () =>
    new grpc.Client(service.target, grpc.credentials.createInsecure());
  const client = new Rime({
    apiKey: "test-key",
    endpoint: service.target,
    timeout: 2,
  });
  try {
    await fn(service, client);
  } finally {
    await client.close();
    service.close();
    transport.makeClient = factory;
  }
}

async function collect(stream) {
  const parts = [];
  for await (const part of stream) parts.push(part);
  return Buffer.concat(parts);
}

for (const incremental of [false, true]) {
  for (const audioFormat of [AudioFormat.PCM_24000, AudioFormat.MULAW_8000]) {
    test(`lexicon is snapshotted once per stream / ${incremental} / ${audioFormat.encoding}`, () =>
      setup(async (service, client) => {
        const entries = [
          { spelling: "Hello", pronunciation: 'h @ . " l oU' },
          { spelling: "cafe\u0301 au lait", pronunciation: '" k { S' },
          { spelling: "Hello", pronunciation: '" k { S' },
        ];
        const expected = structuredClone(entries);
        async function* text() {
          yield "Hello. ";
          yield "Hello again.";
        }
        const stream = client.tts.stream(
          incremental ? text() : "Hello. Hello again.",
          { customLexicon: entries, audioFormat },
        );
        entries[0].spelling = "changed";
        entries[1].pronunciation = "changed";
        entries.length = 0;
        assert.ok((await collect(stream)).length);
        assert.equal(service.calls.length, 1);
        const messages = service.calls[0];
        assert.deepEqual(
          messages.map((message) => message.payload.case),
          ["header", "textChunk", "textChunk"],
        );
        assert.deepEqual(
          messages[0].payload.value.customLexicon.map(
            ({ spelling, pronunciation }) => ({ spelling, pronunciation }),
          ),
          expected,
        );
        assert.deepEqual(
          await collect(client.tts.stream("Hello.")),
          service.payload,
        );
        assert.deepEqual(service.calls[1][0].payload.value.customLexicon, []);
      }));
  }
}

for (const entries of [
  null,
  "hello",
  {},
  [null],
  [{}],
  [{ spelling: 1, pronunciation: "h" }],
  [{ spelling: "hello", pronunciation: null }],
  Array(1),
]) {
  test(`bad lexicon shape fails before network / ${JSON.stringify(entries)}`, () =>
    setup(async (service, client) => {
      assert.throws(
        () => client.tts.stream("Hello.", { customLexicon: entries }),
        RimeInputError,
      );
      assert.equal(service.calls.length, 0);
    }));
}

for (const completeText of [false, true]) {
  test(`ill-formed lexicon Unicode fails before network / completeText=${completeText}`, () =>
    setup(async (service, client) => {
      for (const field of ["spelling", "pronunciation"]) {
        for (const value of [
          "\ud800",
          "\udfff",
          "before\ud800after",
          "before\udfffafter",
          "\ud800\ud800",
          "\udfff\udfff",
          "\udfff\ud800",
        ]) {
          const entry = {
            spelling: "hello",
            pronunciation: "h",
            [field]: value,
          };
          assert.throws(
            () =>
              client.tts.stream("Hello.", {
                completeText,
                customLexicon: [entry],
              }),
            RimeInputError,
            `${field}: ${JSON.stringify(value)}`,
          );
        }
      }
      assert.throws(
        () =>
          client.tts.stream("Hello.", {
            completeText,
            customLexicon: [{ spelling: "\ud800", pronunciation: "\udc00" }],
          }),
        RimeInputError,
      );
      assert.equal(service.calls.length, 0);
      assert.equal(service.completeCalls.length, 0);
    }));

  test(`well-formed lexicon Unicode reaches the service unchanged / completeText=${completeText}`, () =>
    setup(async (service, client) => {
      const entries = [
        { spelling: "cafe\u0301 \u{1f600}", pronunciation: "h \u{1f600}" },
        { spelling: "\ufffd", pronunciation: "\ufffd" },
        { spelling: "\ud7ff\ue000", pronunciation: "\ud7ff\ue000" },
      ];
      await collect(
        client.tts.stream("Hello.", { completeText, customLexicon: entries }),
      );
      const request = completeText
        ? service.completeCalls[0]
        : service.calls[0][0].payload.value;
      assert.deepEqual(
        request.customLexicon.map(({ spelling, pronunciation }) => ({
          spelling,
          pronunciation,
        })),
        entries,
      );
    }));
}

const rejections = [
  'custom-lexicon entry "hello": "h @ . l oU" is not well-formed (no-primary-stress)',
  'custom-lexicon entry "hello": "q" is not well-formed (unknown-phone); custom-lexicon entry "": "h" is not well-formed (empty-spelling)',
  "custom lexicon is not supported by this model",
  'custom lexicon is not supported for language "ja"',
  "custom lexicon has 501 entries; the maximum is 500",
];
for (const mode of ["error_before_audio", "no_audio_error"]) {
  for (const message of rejections) {
    test(`pronunciation error preserves message and request ID / ${mode} / ${message}`, () =>
      setup(async (service, client) => {
        service.mode = mode;
        service.rejectionStatus = grpc.status.INVALID_ARGUMENT;
        service.rejectionMessage = message;
        service.trailingMetadata = { "x-request-id": "rejected-request" };
        let returned = false;
        const source = {
          [Symbol.asyncIterator]() {
            return this;
          },
          next() {
            return new Promise(() => {});
          },
          async return() {
            returned = true;
            return { done: true };
          },
        };
        const stream = client.tts.stream(source, {
          customLexicon: [{ spelling: "hello", pronunciation: "h @ . l oU" }],
        });
        const reading = collect(stream);
        const assertion = assert.rejects(reading, (error) => {
          assert.ok(error instanceof RimeInputError);
          assert.equal(error.message, message);
          assert.equal(
            error.requestId,
            mode === "error_before_audio" ? "rejected-request" : "test-request",
          );
          return true;
        });
        if (mode === "no_audio_error") {
          await service.headersSent;
          service.release();
        }
        await assertion;
        await stream.cancel();
        if (mode === "no_audio_error") assert.ok(returned);
        assert.equal(service.calls.length, 1);
        assert.equal(
          service.calls[0][0].payload.value.customLexicon[0].pronunciation,
          "h @ . l oU",
        );
      }));
  }
}

for (const details of [undefined, "", "   "]) {
  test(`missing service message has status fallback / ${JSON.stringify(details)}`, () => {
    const error = rpcError(grpc.status.INVALID_ARGUMENT, "id", details);
    assert.ok(error instanceof RimeInputError);
    assert.equal(error.message, "Rime operation failed: INVALID_ARGUMENT");
    assert.equal(error.requestId, "id");
  });
}

test("complete text uses Synthesize with the exact text and a lexicon snapshot", () =>
  setup(async (service, client) => {
    const entries = [{ spelling: "read", pronunciation: '" r\\ E d' }];
    const stream = client.tts.stream("Hello. Please read the pages.", {
      completeText: true,
      customLexicon: entries,
    });
    entries[0].spelling = "changed";
    assert.deepEqual(await collect(stream), service.payload);
    assert.equal(service.completeCalls.length, 1);
    const request = service.completeCalls[0];
    assert.equal(request.text, "Hello. Please read the pages.");
    assert.equal(request.customLexicon[0].spelling, "read");
    assert.equal(request.audioParameters.samplingRate, 24000);
    assert.equal(stream.requestId, "test-request");
    assert.equal(
      service.metadata[0].get("authorization")[0],
      "Bearer test-key",
    );
  }));

for (const mode of ["error_before_audio", "no_audio_error"]) {
  test(`complete text keeps pronunciation errors and recovers / ${mode}`, () =>
    setup(async (service, client) => {
      service.mode = mode;
      service.rejectionStatus = grpc.status.INVALID_ARGUMENT;
      service.rejectionMessage =
        'custom-lexicon entry "hello": no-primary-stress';
      service.trailingMetadata = { "x-request-id": "rejected-request" };
      await assert.rejects(
        collect(client.tts.stream("Hello.", { completeText: true })),
        (error) => {
          assert.ok(error instanceof RimeInputError);
          assert.match(error.message, /no-primary-stress/);
          assert.ok(error.requestId);
          return true;
        },
      );
      assert.equal(service.completeCalls.length, 1);
      service.mode = "normal";
      assert.deepEqual(
        await collect(client.tts.stream("Hello.", { completeText: true })),
        service.payload,
      );
    }));
}

test("complete text rejects sources and oversize text; cancellation stops a pending response", () =>
  setup(async (service, client) => {
    const source = (async function* () {
      yield "Hello.";
    })();
    for (const [text, completeText] of [
      [source, true],
      ["é".repeat(32769), true],
      ["Hello.", "yes"],
      ["Hello.", null],
    ]) {
      assert.throws(
        () => client.tts.stream(text, { completeText }),
        RimeInputError,
      );
    }
    assert.equal(service.calls.length, 0);
    service.mode = "silence";
    const stream = client.tts.stream("Hello.", { completeText: true });
    const rejected = assert.rejects(collect(stream), {
      name: "RimeCancelledError",
    });
    await service.headersSent;
    await stream.cancel();
    await rejected;
  }));
