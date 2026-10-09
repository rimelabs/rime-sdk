import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline/promises";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { Rime } from "@rimelabs/sdk";

/** Headerless signed little-endian PCM16, mono, at the given frame rate. */
function pcmOptions(rate) {
  return [
    "-t",
    "raw",
    "-e",
    "signed-integer",
    "-b",
    "16",
    "-L",
    "-r",
    String(rate),
    "-c",
    "1",
  ];
}

function audioProcess(command, args, input) {
  const process = spawn(command, args, {
    stdio: input
      ? ["ignore", "pipe", "inherit"]
      : ["pipe", "ignore", "inherit"],
  });
  const closed = new Promise((resolve) => {
    process.once("error", (error) => resolve({ error }));
    process.once("close", (code, signal) => resolve({ code, signal }));
  });
  // Writes report their own error; prevent a second unhandled stream event.
  process.stdin?.on("error", () => {});
  return { process, closed };
}

async function closeProcess({ process, closed }) {
  if (process.exitCode === null && process.signalCode === null) {
    process.kill("SIGTERM");
    const timer = setTimeout(() => process.kill("SIGKILL"), 2000);
    try {
      await closed;
    } finally {
      clearTimeout(timer);
    }
  } else {
    await closed;
  }
}

export async function* capture(inputPath, lines, signal) {
  const child = audioProcess(
    "sox",
    [
      "-q",
      "--buffer",
      "1280",
      inputPath ? resolve(inputPath) : "-d",
      ...pcmOptions(16000),
      "-",
    ],
    true,
  );
  const stop = new AbortController();
  const waiting = AbortSignal.any([signal, stop.signal]);
  const abort = () => child.process.kill("SIGKILL");
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  let stopping = false;
  let forced = false;
  let stopTimer;
  const finish = lines
    ? (async () => {
        await lines.question("Listening. Press Enter to finish.\n", {
          signal: waiting,
        });
        stopping = true;
        child.process.kill("SIGINT");
        stopTimer = setTimeout(() => {
          forced = true;
          child.process.kill("SIGKILL");
        }, 2000);
      })()
    : Promise.resolve();
  // A capture failure can end the process while an Enter prompt is pending.
  finish.catch(() => {});
  try {
    for await (const chunk of child.process.stdout) {
      signal.throwIfAborted();
      yield chunk;
    }
    signal.throwIfAborted();
    const result = await child.closed;
    if (result.error) throw result.error;
    if (
      forced ||
      (result.code !== 0 && !(stopping && result.signal === "SIGINT"))
    )
      throw new Error(
        `SoX capture failed (${result.code ?? result.signal}); check its error above`,
      );
    if (lines && !stopping)
      throw new Error("Microphone stopped before the turn was finished");
  } finally {
    stop.abort();
    clearTimeout(stopTimer);
    signal.removeEventListener("abort", abort);
    await Promise.allSettled([finish]);
    await closeProcess(child);
  }
}

export async function speak(client, text, options, signal) {
  signal = AbortSignal.any([signal, AbortSignal.timeout(125000)]);
  signal.throwIfAborted();
  // A completed WAV lets the macOS player manage Bluetooth device format changes.
  const directory =
    process.platform === "darwin" && !options.output
      ? await mkdtemp(join(tmpdir(), "rime-voice-"))
      : undefined;
  const output = directory ? join(directory, "reply.wav") : options.output;
  let child;
  const abort = () => child?.process.kill("SIGKILL");
  signal.addEventListener("abort", abort, { once: true });
  let audio;
  try {
    signal.throwIfAborted();
    child = audioProcess(
      "sox",
      ["-q", ...pcmOptions(24000), "-", output ? resolve(output) : "-d"],
      false,
    );
    audio = client.tts.stream(text, {
      language: options.language,
      voice: options.voice,
      timeout: 60,
    });
    for await (const chunk of audio) {
      signal.throwIfAborted();
      await new Promise((resolve, reject) =>
        child.process.stdin.write(chunk, (error) =>
          error ? reject(error) : resolve(),
        ),
      );
    }
    child.process.stdin.end();
    const result = await child.closed;
    signal.throwIfAborted();
    if (result.error) throw result.error;
    if (result.code !== 0)
      throw new Error(
        `SoX playback failed (${result.code ?? result.signal}); check its error above`,
      );
    if (directory) {
      child = audioProcess("afplay", [output], false);
      child.process.stdin.end();
      const playback = await child.closed;
      signal.throwIfAborted();
      if (playback.error) throw playback.error;
      if (playback.code !== 0)
        throw new Error(
          `Audio playback failed (${playback.code ?? playback.signal})`,
        );
    }
    console.log(`TTS request: ${audio.requestId}`);
  } finally {
    signal.removeEventListener("abort", abort);
    await audio?.cancel();
    if (child) await closeProcess(child);
    if (directory) await rm(directory, { recursive: true, force: true });
  }
}

export async function turn(client, options, lines, signal) {
  const stopCapture = new AbortController();
  const source = capture(
    options.input,
    lines,
    AbortSignal.any([signal, stopCapture.signal]),
  );
  const stream = client.stt.stream(source, {
    language: options.language,
    mode: options.mode,
    contextTerms: options.term,
    timeout: 120,
    signal,
  });
  let transcript;
  try {
    for await (const update of stream) {
      console.log(`${update.kind}: ${update.text}`);
      if (update.kind === "final") transcript = update.text;
    }
    console.log(`STT request: ${stream.requestId}`);
  } finally {
    stopCapture.abort();
    await stream.cancel();
    await source.return();
  }
  if (transcript === undefined)
    throw new Error("Transcription ended without a final result");
  if (!transcript.trim()) {
    console.log("No speech recognized.");
    return;
  }
  // The response step echoes the final transcript so recognition stays visible.
  await speak(client, transcript, options, signal);
}

export async function main(options, client = new Rime()) {
  const stop = new AbortController();
  const lines = options.input
    ? undefined
    : createInterface({ input: process.stdin, output: process.stdout });
  const abort = () => {
    stop.abort(new Error("Stopped"));
    void client.close();
  };
  process.once("SIGINT", abort);
  lines?.once("close", abort);
  try {
    if (options.input) {
      await turn(client, options, undefined, stop.signal);
      return;
    }
    console.log("Voice echo: microphone → STT → TTS. Ctrl+C stops everything.");
    while (!stop.signal.aborted) {
      const command = await lines.question(
        "\nPress Enter to talk, or type q then Enter to quit.\n",
        { signal: stop.signal },
      );
      if (command.trim().toLowerCase() === "q") break;
      await turn(client, options, lines, stop.signal);
    }
  } catch (error) {
    if (!stop.signal.aborted) throw error;
    console.log("\nStopped.");
  } finally {
    process.removeListener("SIGINT", abort);
    lines?.removeListener("close", abort);
    lines?.close();
    await client.close();
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const { values } = parseArgs({
    options: {
      language: { type: "string" },
      mode: { type: "string", default: "written" },
      term: { type: "string", multiple: true, default: [] },
      voice: { type: "string" },
      input: { type: "string" },
      output: { type: "string" },
      help: { type: "boolean" },
    },
  });
  if (values.help || !values.language || (values.output && !values.input)) {
    console.log(
      "Usage: voice.mjs --language en [--mode written|verbatim] [--term Rime] [--voice clementine] [--input audio.wav --output reply.wav]",
    );
    process.exit(values.help ? 0 : 1);
  }
  try {
    await main(values);
  } catch (error) {
    console.error(`Error: ${error.message}`);
    if (error.cause) console.error(`Cause: ${error.cause.message}`);
    process.exitCode = 1;
  }
}
