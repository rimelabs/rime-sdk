import assert from "node:assert/strict";
import {
  mkdtempSync,
  writeFileSync,
  readFileSync,
  existsSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import * as grpc from "@grpc/grpc-js";
import { Rime, RimeInputError, RimeUnavailableError } from "../dist/index.js";
import { transport as recognitionTransport } from "../dist/stt/transport.js";
import { transport as synthesisTransport } from "../dist/tts/transport.js";
import { RecognitionService } from "./stt-service.mjs";
import { FakeService } from "./service.mjs";
import * as voice from "../examples/stt/voice.mjs";

async function setup(t, deviceMode = "normal", platform = "darwin") {
  const directory = mkdtempSync(join(tmpdir(), "rime-terminal-test-"));
  const log = join(directory, "processes.jsonl"),
    output = join(directory, "played.pcm");
  const path = process.env.PATH;
  const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform");
  Object.defineProperty(process, "platform", { value: platform });
  const release = join(directory, "release");
  process.env.PATH = `${directory}:${path}`;
  t.after(() => {
    process.env.PATH = path;
    Object.defineProperty(process, "platform", originalPlatform);
    rmSync(directory, { recursive: true, force: true });
  });
  writeFileSync(
    join(directory, "sox"),
    `#!/usr/bin/env node
const fs = require('node:fs');
const capture = process.argv.at(-1) === '-';
fs.appendFileSync(${JSON.stringify(log)}, JSON.stringify({pid:process.pid,capture})+'\\n');
if (capture) {
  process.on('SIGINT', () => process.exit(0));
  process.stdout.write(Buffer.alloc(1280));
  if (${JSON.stringify(deviceMode)} === 'fail') process.exitCode = 7;
  else if (process.argv[5] === '-d') setInterval(() => {}, 1000);
} else {
  const chunks=[];
  process.stdin.on('data', chunk => chunks.push(chunk));
  process.stdin.on('end', () => fs.writeFileSync(process.argv.at(-1) === '-d' ? ${JSON.stringify(output)} : process.argv.at(-1), Buffer.concat(chunks)));
}
`,
    { mode: 0o755 },
  );
  writeFileSync(
    join(directory, "afplay"),
    `#!/usr/bin/env node
const fs = require('node:fs');
const reply = process.argv[2];
fs.appendFileSync(${JSON.stringify(log)}, JSON.stringify({pid:process.pid,reply})+'\\n');
fs.copyFileSync(reply, ${JSON.stringify(output)});
if (${JSON.stringify(deviceMode)} === 'playback_fail') process.exitCode = 7;
if (${JSON.stringify(deviceMode)} === 'hold') {
  const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) clearInterval(timer); }, 10);
}
`,
    { mode: 0o755 },
  );
  const recognition = await new RecognitionService().start();
  const synthesis = await new FakeService().start();
  t.after(() => {
    recognition.close();
    synthesis.close();
  });
  t.mock.method(
    recognitionTransport,
    "makeClient",
    () =>
      new grpc.Client(recognition.target, grpc.credentials.createInsecure()),
  );
  t.mock.method(
    synthesisTransport,
    "makeClient",
    () => new grpc.Client(synthesis.target, grpc.credentials.createInsecure()),
  );
  const client = new Rime({ apiKey: "test-key" });
  t.after(() => client.close());
  const processes = () =>
    existsSync(log)
      ? readFileSync(log, "utf8").trim().split("\n").map(JSON.parse)
      : [];
  const assertStopped = () => {
    for (const { pid } of processes())
      assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  };
  t.mock.method(console, "log", () => {});
  return {
    recognition,
    synthesis,
    client,
    output,
    release,
    processes,
    assertStopped,
  };
}
const options = {
  language: "en",
  mode: "written",
  term: ["Rime"],
  input: "fixture.wav",
};

test("terminal echo repeats turns and speaks only final snapshots", async (t) => {
  const state = await setup(t);
  for (let index = 0; index < 2; index++)
    await voice.turn(
      state.client,
      options,
      undefined,
      new AbortController().signal,
    );
  assert.equal(state.synthesis.calls.length, 2);
  for (const call of state.synthesis.calls)
    assert.equal(
      call
        .filter((m) => m.payload.case === "textChunk")
        .map((m) => m.payload.value)
        .join(""),
      "Ice cream",
    );
  assert.ok(readFileSync(state.output).length);
  state.assertStopped();
});

for (const cancel of [false, true])
  test(`native playback waits for device and cleans up: cancellation=${cancel}`, async (t) => {
    const state = await setup(t, "hold");
    const stop = new AbortController();
    const operation = voice.speak(state.client, "Hello.", options, stop.signal);
    t.after(async () => {
      stop.abort();
      await operation.catch(() => {});
    });
    let completed = false;
    operation.then(
      () => {
        completed = true;
      },
      () => {
        completed = true;
      },
    );
    let player;
    const deadline = Date.now() + 3000;
    while (!(player = state.processes().find((p) => p.reply))) {
      assert.ok(Date.now() < deadline, "native player did not start");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(
      completed,
      false,
      "generation completion is not playback completion",
    );
    assert.equal(existsSync(player.reply), true);
    if (cancel) {
      stop.abort(new Error("cancelled"));
      await assert.rejects(operation, /cancelled/);
    } else {
      writeFileSync(state.release, "");
      await operation;
      assert.deepEqual(readFileSync(state.output), state.synthesis.payload);
    }
    assert.equal(existsSync(player.reply), false);
    state.assertStopped();
  });

test("native playback reports player failure", async (t) => {
  const state = await setup(t, "playback_fail");
  await assert.rejects(
    voice.speak(state.client, "Hello.", options, new AbortController().signal),
    /Audio playback failed/,
  );
  assert.equal(existsSync(state.processes().find((p) => p.reply).reply), false);
  state.assertStopped();
});

test("rejected TTS never starts playback", async (t) => {
  const state = await setup(t);
  state.synthesis.mode = "error_before_audio";
  await assert.rejects(
    voice.speak(state.client, "Hello.", options, new AbortController().signal),
    RimeUnavailableError,
  );
  assert.equal(
    state.processes().some((p) => p.reply),
    false,
  );
  state.assertStopped();
});

test(
  "abort cancels TTS while waiting for the first audio chunk",
  { timeout: 5000 },
  async (t) => {
    const state = await setup(t);
    state.synthesis.mode = "silence";
    const stop = new AbortController();
    const operation = voice.speak(state.client, "Hello.", options, stop.signal);
    const failed = assert.rejects(operation, /cancelled/);
    failed.catch(() => {});
    await state.synthesis.headersSent;
    stop.abort(new Error("cancelled"));
    await failed;
    assert.equal(state.synthesis.calls.length, 1);
    state.assertStopped();
  },
);

test("explicit output does not open a speaker", async (t) => {
  const state = await setup(t);
  await voice.speak(
    state.client,
    "Hello.",
    { ...options, output: state.output },
    new AbortController().signal,
  );
  assert.deepEqual(readFileSync(state.output), state.synthesis.payload);
  assert.equal(
    state.processes().some((p) => p.reply),
    false,
  );
  state.assertStopped();
});

test("Linux streams directly to SoX playback", async (t) => {
  const state = await setup(t, "normal", "linux");
  await voice.speak(
    state.client,
    "Hello.",
    options,
    new AbortController().signal,
  );
  assert.deepEqual(readFileSync(state.output), state.synthesis.payload);
  assert.equal(
    state.processes().some((p) => p.reply),
    false,
  );
  state.assertStopped();
});

test("terminal silence never starts playback", async (t) => {
  const state = await setup(t);
  state.recognition.mode = "silence";
  await voice.turn(
    state.client,
    options,
    undefined,
    new AbortController().signal,
  );
  assert.equal(state.synthesis.calls.length, 0);
  assert.equal(state.processes().length, 1);
  state.assertStopped();
});

test("terminal recorder failure never commits or speaks", async (t) => {
  const state = await setup(t, "fail");
  await assert.rejects(
    voice.turn(state.client, options, undefined, new AbortController().signal),
    RimeInputError,
  );
  assert.equal(state.recognition.inputFinished, false);
  assert.equal(state.synthesis.calls.length, 0);
  state.assertStopped();
});

for (const cancel of [false, true])
  test(`terminal microphone stops: cancellation=${cancel}`, async (t) => {
    const state = await setup(t);
    const stop = new AbortController();
    let enter;
    const lines = {
      question: (_, { signal }) =>
        new Promise((resolve, reject) => {
          enter = resolve;
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        }),
    };
    const source = voice.capture(undefined, lines, stop.signal);
    assert.ok((await source.next()).value.length);
    if (cancel) {
      stop.abort(new Error("cancelled"));
      await assert.rejects(source.next(), /cancelled/);
    } else {
      enter("");
      assert.equal((await source.next()).done, true);
    }
    await source.return();
    state.assertStopped();
  });

test("terminal rejected STT never opens microphone", async (t) => {
  const state = await setup(t);
  state.recognition.mode = "reject";
  await assert.rejects(
    voice.turn(state.client, options, undefined, new AbortController().signal),
    RimeUnavailableError,
  );
  assert.deepEqual(state.processes(), []);
});

test("terminal STT failure stops a microphone blocked on its next chunk", async (t) => {
  const state = await setup(t);
  state.recognition.mode = "partial_error";
  let entered;
  const ready = new Promise((resolve) => {
    entered = resolve;
  });
  const lines = {
    question: (_, { signal }) =>
      new Promise((_, reject) => {
        entered();
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      }),
  };
  const operation = voice.turn(
    state.client,
    { ...options, input: undefined },
    lines,
    new AbortController().signal,
  );
  const failed = assert.rejects(operation, RimeUnavailableError);
  failed.catch(() => {});
  await ready;
  const deadline = Date.now() + 3000;
  while (state.recognition.calls[0].length < 2) {
    assert.ok(
      Date.now() < deadline,
      "recognition did not receive microphone audio",
    );
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  state.recognition.release();
  await failed;
  assert.equal(state.synthesis.calls.length, 0);
  state.assertStopped();
});
