import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { Rime } from "../src/index.js";
import { transport as synthesisTransport } from "../src/tts/transport.js";
import { transport as recognitionTransport } from "../src/stt/transport.js";
import { FakeService } from "./service.mjs";
import { RecognitionService } from "./stt-service.mjs";
import {
  argumentsFor,
  respond,
  turn,
  type Message,
} from "../../examples/typescript/agent/voice.js";

function completed(text = "Hello world.") {
  return {
    status: "completed",
    output: [
      { type: "reasoning" },
      { type: "message", content: [{ type: "output_text", text }] },
    ],
  };
}
function model(reply = completed(), status = 200) {
  const requests: { input: Message[]; store: boolean }[] = [];
  const request: typeof fetch = async (_url, init) => {
    requests.push(JSON.parse(String(init?.body)));
    return new Response(JSON.stringify(reply), {
      status,
      headers: { "x-request-id": "llm-request" },
    });
  };
  return { request, requests };
}
async function setup(t: TestContext, model = "coda") {
  const synthesis = await new FakeService().start();
  const recognition = await new RecognitionService().start();
  const outputDir = await mkdtemp(join(tmpdir(), "rime-agent-test-"));
  const key = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "test-openai-key";
  t.after(async () => {
    if (key === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = key;
    synthesis.close();
    recognition.close();
    await rm(outputDir, { recursive: true, force: true });
  });
  t.mock.method(
    synthesisTransport,
    "makeClient",
    () => new grpc.Client(synthesis.target!, grpc.credentials.createInsecure()),
  );
  t.mock.method(
    recognitionTransport,
    "makeClient",
    () =>
      new grpc.Client(recognition.target!, grpc.credentials.createInsecure()),
  );
  const client = new Rime({ apiKey: "test-key", model });
  t.after(() => client.close());
  const options = {
    ...argumentsFor(["--model", model, "--no-playback"])!,
    outputDir,
  };
  t.mock.method(console, "log", () => {});
  t.mock.method(console, "error", () => {});
  return {
    synthesis,
    recognition,
    client,
    options,
    signal: new AbortController().signal,
  };
}

test("LLM uses bounded history and extracts message text after reasoning", async (t) => {
  const { options, signal } = await setup(t);
  const llm = model();
  const history: Message[] = Array.from({ length: 24 }, (_, i) => ({
    role: i % 2 ? "assistant" : "user",
    content: String(i),
  }));
  assert.deepEqual(await respond(options, history, "Hi", signal, llm.request), {
    text: "Hello world.",
    requestId: "llm-request",
  });
  assert.equal(llm.requests[0].input.length, 21);
  assert.equal(llm.requests[0].store, false);
});

test("conversation replies go through the SDK, preserve history and reload lexicon", async (t) => {
  const { client, synthesis, options, signal } = await setup(t);
  const path = join(options.outputDir, "lexicon.json");
  options.lexicon = path;
  await writeFile(
    path,
    JSON.stringify([{ spelling: "hello", pronunciation: 'h @ . " l oU' }]),
  );
  const llm = model(),
    history: Message[] = [];
  assert.ok(
    await turn(client, options, history, 1, signal, { ask: "Hi" }, llm.request),
  );
  await writeFile(path, "[]");
  assert.ok(
    await turn(
      client,
      options,
      history,
      2,
      signal,
      { ask: "Again" },
      llm.request,
    ),
  );
  assert.deepEqual(llm.requests[1].input, history.slice(0, 3));
  assert.equal(
    synthesis.calls[0][0].payload.value.customLexicon[0].pronunciation,
    'h @ . " l oU',
  );
  assert.deepEqual(synthesis.calls[1][0].payload.value.customLexicon, []);
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-002.json"), "utf8"),
  );
  assert.equal(report.status, "ok");
  assert.equal(report.llm_request_id, "llm-request");
  assert.ok(report.tts_request_id);
  const wav = await readFile(report.audio_file);
  assert.equal(wav.toString("ascii", 0, 4), "RIFF");
  assert.equal(wav.readUInt32LE(24), 24000);
  assert.deepEqual(wav.subarray(44), synthesis.payload);
});

test("recorded turn sends only the final STT snapshot to the LLM", async (t) => {
  const { client, synthesis, recognition, options, signal } = await setup(t);
  const path = process.env.PATH;
  await writeFile(
    join(options.outputDir, "sox"),
    "#!/usr/bin/env node\nprocess.stdout.write(Buffer.alloc(1280));\n",
    { mode: 0o755 },
  );
  process.env.PATH = `${options.outputDir}:${path}`;
  t.after(() => {
    process.env.PATH = path;
  });
  options.input = "fixture.wav";
  const llm = model();
  assert.ok(await turn(client, options, [], 1, signal, {}, llm.request));
  assert.deepEqual(llm.requests[0].input, [
    { role: "user", content: "Ice cream" },
  ]);
  assert.ok(recognition.inputFinished);
  assert.equal(synthesis.calls.length, 1);
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
  );
  assert.equal(report.stt_request_id, "stt-request");
});

test("STT failure closes capture blocked on its next audio chunk", async (t) => {
  const { client, synthesis, recognition, options } = await setup(t);
  recognition.mode = "partial_error";
  const path = process.env.PATH;
  const pidFile = join(options.outputDir, "capture.pid");
  await writeFile(
    join(options.outputDir, "sox"),
    `#!/usr/bin/env node
require('node:fs').writeFileSync(${JSON.stringify(pidFile)}, String(process.pid));
process.stdout.write(Buffer.alloc(1280));
setInterval(() => {}, 1000);
`,
    { mode: 0o755 },
  );
  process.env.PATH = `${options.outputDir}:${path}`;
  t.after(() => {
    process.env.PATH = path;
  });
  const controller = new AbortController();
  const deadline = setTimeout(() => controller.abort(), 3000);
  t.after(() => {
    clearTimeout(deadline);
    controller.abort();
  });
  options.input = "fixture.wav";
  const llm = model();
  const operation = turn(
    client,
    options,
    [],
    1,
    controller.signal,
    {},
    llm.request,
  );
  while ((recognition.calls[0]?.length ?? 0) < 2) {
    controller.signal.throwIfAborted();
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  recognition.release();
  assert.equal(await operation, false);
  assert.equal(controller.signal.aborted, false, "capture cleanup stalled");
  assert.equal(llm.requests.length, 0);
  assert.equal(synthesis.calls.length, 0);
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
  );
  assert.equal(report.stage, "stt");
  assert.equal(report.error.type, "RimeUnavailableError");
  const pid = Number(await readFile(pidFile, "utf8"));
  assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
});

test("pronunciation errors retain request ID, save no WAV, and do not call OpenAI", async (t) => {
  const { client, synthesis, options, signal } = await setup(t);
  delete process.env.OPENAI_API_KEY;
  synthesis.mode = "error_before_audio";
  synthesis.rejectionStatus = grpc.status.INVALID_ARGUMENT;
  synthesis.rejectionMessage =
    'custom-lexicon entry "hello": no-primary-stress';
  synthesis.trailingMetadata = { "x-request-id": "rejected-request" };
  const llm = model();
  assert.equal(
    await turn(client, options, [], 1, signal, { say: "Hello." }, llm.request),
    false,
  );
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
  );
  assert.equal(report.error.message, synthesis.rejectionMessage);
  assert.equal(report.error.request_id, "rejected-request");
  await assert.rejects(readFile(join(options.outputDir, "turn-001.wav")), {
    code: "ENOENT",
  });
  assert.equal(llm.requests.length, 0);
  synthesis.mode = "normal";
  assert.ok(
    await turn(client, options, [], 2, signal, { say: "Hello." }, llm.request),
  );
});

for (const code of [0, 12])
  test(`timestamp report retains audio / ${code}`, async (t) => {
    const { client, synthesis, options, signal } = await setup(t, "mistv3");
    options.timestamps = true;
    synthesis.finalResponses = [
      create(schema.SynthesisResponseStreamSchema, {
        payload: {
          case: "trailer",
          value: {
            timestamps: {
              status: { code, message: code ? "unavailable" : "" },
              spans: code
                ? []
                : [{ text: "Hello", start: {}, end: { seconds: 1n } }],
            },
          },
        },
      }),
    ];
    assert.ok(
      await turn(
        client,
        options,
        [],
        1,
        signal,
        { say: "Hello." },
        model().request,
      ),
    );
    const report = JSON.parse(
      await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
    );
    assert.equal(report.timestamps.status.code, code);
    assert.ok((await readFile(report.audio_file)).length);
  });

test("bad timestamp trailer retains completed WAV", async (t) => {
  const { client, options, signal } = await setup(t, "mistv3");
  options.timestamps = true;
  assert.equal(
    await turn(
      client,
      options,
      [],
      1,
      signal,
      { say: "Hello." },
      model().request,
    ),
    false,
  );
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
  );
  assert.equal(report.stage, "timestamps");
  assert.ok((await readFile(report.audio_file)).length);
});

test("incomplete LLM response never starts synthesis", async (t) => {
  const { client, synthesis, options, signal } = await setup(t);
  const llm = model({ status: "incomplete", output: [] });
  assert.equal(
    await turn(client, options, [], 1, signal, { ask: "Hi" }, llm.request),
    false,
  );
  assert.equal(synthesis.calls.length, 0);
});

test("provider errors redact API keys", async (t) => {
  const { options, signal } = await setup(t);
  const request: typeof fetch = async () =>
    new Response(
      JSON.stringify({ error: { message: "Invalid test-openai-key" } }),
      { status: 401 },
    );
  await assert.rejects(
    respond(options, [], "Hi", signal, request),
    (error) =>
      error instanceof Error &&
      error.message.includes("[redacted]") &&
      !error.message.includes("test-openai-key"),
  );
});

test("cancellation during TTS deletes partial audio", async (t) => {
  const { client, synthesis, options } = await setup(t);
  synthesis.mode = "silence";
  const controller = new AbortController();
  const work = turn(
    client,
    options,
    [],
    1,
    controller.signal,
    { say: "Hello." },
    model().request,
  );
  await synthesis.headersSent;
  controller.abort();
  assert.equal(await work, false);
  const report = JSON.parse(
    await readFile(join(options.outputDir, "turn-001.json"), "utf8"),
  );
  assert.equal(report.status, "cancelled");
  await assert.rejects(
    readFile(join(options.outputDir, "turn-001.partial.wav")),
    { code: "ENOENT" },
  );
});
