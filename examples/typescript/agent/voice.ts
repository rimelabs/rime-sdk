/** Local Rime STT → OpenAI → Rime TTS agent and SDK feature checks. */
import { spawn } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface, type Interface } from "node:readline/promises";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import {
  Rime,
  RimeError,
  type AudioStream,
  type PronunciationEntry,
  type TimestampResult,
} from "@rimelabs/sdk";
import { capture } from "../stt/voice.mjs";
import { WavWriter } from "../common/wav.js";

export const instructions =
  "You are a concise voice assistant. Reply in one or two short sentences, without markdown. When asked to repeat text, repeat it exactly. Reply in the user's language.";
export interface Options {
  model: "coda" | "mistv3";
  language: string;
  voice?: string;
  timestamps: boolean;
  completeText: boolean;
  lexicon?: string;
  llmModel: string;
  instructions: string;
  mode: "written" | "verbatim";
  term: string[];
  endpoint?: string;
  sttEndpoint?: string;
  input?: string;
  say?: string;
  noPlayback: boolean;
  outputDir?: string;
}
export type Message = { role: "user" | "assistant"; content: string };
interface Report {
  model: string;
  language: string;
  voice?: string;
  llm_model: string;
  stage: string;
  status: string;
  custom_lexicon?: readonly PronunciationEntry[];
  timestamps_requested?: boolean;
  complete_text?: boolean;
  transcript?: string;
  reply?: string;
  stt_request_id?: string | null;
  llm_request_id?: string | null;
  tts_request_id?: string | null;
  timestamps?: TimestampResult;
  audio_file?: string;
  error?: { type: string; message: string; request_id: string | null };
}

export function redact(message: string): string {
  for (const name of ["RIME_API_KEY", "OPENAI_API_KEY"]) {
    const key = process.env[name];
    if (key) message = message.replaceAll(key, "[redacted]");
  }
  return message;
}

export async function loadLexicon(
  path?: string,
): Promise<PronunciationEntry[]> {
  if (!path) return [];
  const entries: unknown = JSON.parse(await readFile(path, "utf8"));
  if (
    !Array.isArray(entries) ||
    entries.some(
      (entry) =>
        !entry ||
        typeof entry !== "object" ||
        Object.keys(entry).sort().join(",") !== "pronunciation,spelling" ||
        typeof entry.spelling !== "string" ||
        typeof entry.pronunciation !== "string",
    )
  )
    throw new Error(
      'Lexicon must be a JSON array of {"spelling": string, "pronunciation": string}',
    );
  return entries;
}

export async function respond(
  options: Options,
  history: Message[],
  text: string,
  signal: AbortSignal,
  request: typeof fetch = fetch,
): Promise<{ text: string; requestId: string | null }> {
  const key = process.env.OPENAI_API_KEY;
  if (!key)
    throw new Error(
      "Set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY",
    );
  const response = await request("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: options.llmModel,
      instructions: options.instructions,
      input: [...history.slice(-20), { role: "user", content: text }],
      max_output_tokens: 512,
      store: false,
    }),
    signal: AbortSignal.any([signal, AbortSignal.timeout(30000)]),
    redirect: "error",
  });
  const requestId = response.headers.get("x-request-id");
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(
      redact(
        `OpenAI HTTP ${response.status}: ${body?.error?.message ?? response.statusText}; request_id=${requestId}`,
      ),
    );
  }
  const body = (await response.json()) as {
    status?: string;
    output?: { type: string; content?: { type: string; text?: string }[] }[];
  };
  if (body.status !== "completed")
    throw new Error(
      `OpenAI response did not complete: ${body.status}; request_id=${requestId}`,
    );
  const reply = (body.output ?? [])
    .filter((item) => item.type === "message")
    .flatMap((item) => item.content ?? [])
    .filter((part) => part.type === "output_text")
    .map((part) => part.text ?? "")
    .join("")
    .trim();
  if (!reply)
    throw new Error(
      `OpenAI returned no speakable text; request_id=${requestId}`,
    );
  return { text: reply, requestId };
}

export async function playback(path: string, signal: AbortSignal) {
  const native = process.platform === "darwin";
  const child = spawn(
    native ? "afplay" : "sox",
    native ? [path] : ["-q", path, "-d"],
    {
      stdio: ["ignore", "inherit", "inherit"],
      signal: AbortSignal.any([signal, AbortSignal.timeout(65000)]),
      killSignal: "SIGKILL",
    },
  );
  await new Promise<void>((done, reject) => {
    let failure: Error | undefined;
    child.once("error", (error) => {
      failure = error;
    });
    child.once("close", (code) =>
      failure
        ? reject(failure)
        : code === 0
          ? done()
          : reject(new Error(`Playback failed (exit ${code})`)),
    );
  });
}

export async function turn(
  client: Rime,
  options: Options & { outputDir: string },
  history: Message[],
  number: number,
  signal: AbortSignal,
  input: { lines?: Interface; say?: string; ask?: string } = {},
  request: typeof fetch = fetch,
): Promise<boolean> {
  const stem = join(
    options.outputDir,
    `turn-${String(number).padStart(3, "0")}`,
  );
  const partial = `${stem}.partial.wav`;
  const report: Report = {
    model: options.model,
    language: options.language,
    voice: options.voice,
    llm_model: options.llmModel,
    stage: "configuration",
    status: "pending",
  };
  let audio: AudioStream | undefined;
  const cancel = () => {
    void audio?.cancel();
  };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    signal.throwIfAborted();
    const lexicon = await loadLexicon(options.lexicon);
    report.custom_lexicon = lexicon;
    report.timestamps_requested = options.timestamps;
    report.complete_text = options.completeText;
    let transcript = input.ask;
    let reply = input.say;
    if (reply === undefined) {
      if (!process.env.OPENAI_API_KEY)
        throw new Error(
          "Set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY",
        );
      if (transcript === undefined) {
        report.stage = "stt";
        const stopCapture = new AbortController();
        const source = capture(
          options.input,
          input.lines,
          AbortSignal.any([signal, stopCapture.signal]),
        );
        const stream = client.stt.stream(source, {
          language: options.language,
          mode: options.mode,
          contextTerms: options.term,
          timeout: 120,
          signal,
        });
        try {
          for await (const update of stream) {
            console.log(`${update.kind}: ${update.text}`);
            if (update.kind === "final") transcript = update.text;
          }
          report.stt_request_id = stream.requestId;
          console.log(`STT request: ${stream.requestId}`);
        } finally {
          stopCapture.abort();
          await stream.cancel();
          await source.return();
        }
        if (transcript === undefined)
          throw new Error("Transcription ended without a final result");
      }
      report.transcript = transcript;
      if (!transcript.trim()) {
        report.status = "silence";
        console.log("No speech recognized.");
        return true;
      }
      report.stage = "llm";
      const answer = await respond(
        options,
        history,
        transcript,
        signal,
        request,
      );
      reply = answer.text;
      report.llm_request_id = answer.requestId;
      console.log(`OpenAI request: ${answer.requestId}`);
    }
    report.reply = reply;
    console.log(`Assistant: ${reply}`);
    signal.throwIfAborted();
    report.stage = "tts";
    audio = client.tts.stream(reply, {
      language: options.language,
      voice: options.voice,
      timestamps: options.timestamps,
      completeText: options.completeText,
      customLexicon: lexicon,
      timeout: 60,
    });
    const writer = new WavWriter(partial);
    try {
      for await (const chunk of audio) {
        signal.throwIfAborted();
        writer.write(chunk);
      }
    } finally {
      writer.close();
    }
    report.tts_request_id = audio.requestId;
    console.log(`TTS request: ${audio.requestId}`);
    await rename(partial, `${stem}.wav`);
    report.audio_file = `${stem}.wav`;
    if (options.timestamps) {
      report.stage = "timestamps";
      const result = await audio.timestamps();
      report.timestamps = result;
      console.log(
        `Timestamp status: ${result.status.code} ${result.status.message}`,
      );
      for (const word of result.spans)
        console.log(
          `  ${word.start.toFixed(3).padStart(8)}–${word.end.toFixed(3).padStart(8)}  ${word.text}`,
        );
    }
    if (!options.noPlayback) {
      report.stage = "playback";
      await playback(`${stem}.wav`, signal);
    }
    if (input.say === undefined) {
      history.push(
        { role: "user", content: transcript! },
        { role: "assistant", content: reply },
      );
      history.splice(0, Math.max(0, history.length - 20));
    }
    report.stage = "done";
    report.status = "ok";
    return true;
  } catch (error) {
    report.status = signal.aborted ? "cancelled" : "error";
    report.error = {
      type: error instanceof Error ? error.name : "Error",
      message: redact(error instanceof Error ? error.message : String(error)),
      request_id: error instanceof RimeError ? error.requestId : null,
    };
    console.error(`${report.stage} error:`, report.error);
    return false;
  } finally {
    signal.removeEventListener("abort", cancel);
    await audio?.cancel();
    await rm(partial, { force: true });
    await writeFile(`${stem}.json`, JSON.stringify(report, null, 2) + "\n");
    console.log(`Report: ${stem}.json`);
  }
}

export function argumentsFor(
  argv = process.argv.slice(2),
): Options | undefined {
  const { values } = parseArgs({
    args: argv,
    options: {
      model: { type: "string", default: "coda" },
      language: { type: "string", default: "en" },
      voice: { type: "string" },
      timestamps: { type: "boolean", default: false },
      "complete-text": { type: "boolean", default: false },
      lexicon: { type: "string" },
      "llm-model": {
        type: "string",
        default: process.env.OPENAI_MODEL ?? "gpt-4.1-mini",
      },
      instructions: { type: "string", default: instructions },
      mode: { type: "string", default: "written" },
      term: { type: "string", multiple: true, default: [] },
      endpoint: { type: "string" },
      "stt-endpoint": { type: "string" },
      input: { type: "string" },
      say: { type: "string" },
      "no-playback": { type: "boolean", default: false },
      "output-dir": { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help) {
    console.log(
      "Rime STT → OpenAI → Rime TTS. Enter to talk; /say TEXT; /ask TEXT; /reset; /quit.\nOptions: --model coda|mistv3 --language en --voice NAME --timestamps --complete-text --lexicon FILE\n  --llm-model MODEL --instructions TEXT --mode written|verbatim --term TERM\n  --endpoint HOST:PORT --stt-endpoint HOST:PORT\n  --input WAV | --say TEXT --no-playback --output-dir DIRECTORY\nEnvironment: RIME_API_KEY; OPENAI_API_KEY for conversation turns; optional OPENAI_MODEL.",
    );
    return;
  }
  if (values.model !== "coda" && values.model !== "mistv3")
    throw new Error("--model must be coda or mistv3");
  if (values.mode !== "written" && values.mode !== "verbatim")
    throw new Error("--mode must be written or verbatim");
  if (values.input !== undefined && values.say !== undefined)
    throw new Error("Use either --input or --say");
  return {
    model: values.model,
    language: values.language,
    voice: values.voice,
    timestamps: values.timestamps,
    completeText: values["complete-text"],
    lexicon: values.lexicon,
    llmModel: values["llm-model"],
    instructions: values.instructions,
    mode: values.mode,
    term: values.term,
    endpoint: values.endpoint,
    sttEndpoint: values["stt-endpoint"],
    input: values.input,
    say: values.say,
    noPlayback: values["no-playback"],
    outputDir: values["output-dir"],
  };
}

export async function main(options: Options) {
  const controller = new AbortController();
  const stop = () => controller.abort(new Error("Stopped"));
  process.once("SIGINT", stop);
  const parent = options.outputDir ? resolve(options.outputDir) : tmpdir();
  await mkdir(parent, { recursive: true });
  const settings = {
    ...options,
    outputDir: await mkdtemp(join(parent, "rime-agent-typescript-")),
  };
  console.log(`Artifacts: ${settings.outputDir}`);
  const client = new Rime({
    model: options.model,
    endpoint: options.endpoint,
    sttEndpoint: options.sttEndpoint,
  });
  let lines: Interface | undefined;
  const history: Message[] = [];
  try {
    if (options.input || options.say !== undefined)
      return await turn(client, settings, history, 1, controller.signal, {
        say: options.say,
      });
    lines = createInterface({ input: process.stdin, output: process.stdout });
    lines.once("close", stop);
    console.log(
      "Enter to talk; /say TEXT tests TTS; /ask TEXT talks to the LLM; /reset; /quit. Ctrl+C exits.",
    );
    let number = 0;
    while (!controller.signal.aborted) {
      const command = (
        await lines.question("\nReady > ", { signal: controller.signal })
      ).trim();
      if (command === "q" || command === "/quit") return true;
      if (command === "/reset") {
        history.length = 0;
        console.log("Conversation cleared.");
        continue;
      }
      const say = command.startsWith("/say ")
        ? command.slice(5).trim()
        : undefined;
      const ask = command.startsWith("/ask ")
        ? command.slice(5).trim()
        : undefined;
      if (command && say === undefined && ask === undefined) {
        console.log("Use Enter, /say TEXT, /ask TEXT, /reset, or /quit.");
        continue;
      }
      await turn(client, settings, history, ++number, controller.signal, {
        lines: command ? undefined : lines,
        say,
        ask,
      });
    }
    return true;
  } catch (error) {
    if (!controller.signal.aborted) throw error;
    console.log("Stopped.");
    return true;
  } finally {
    lines?.close();
    await client.close();
    process.removeListener("SIGINT", stop);
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  try {
    const options = argumentsFor();
    if (options)
      void main(options).then(
        (success) => {
          process.exitCode = success ? 0 : 1;
        },
        (error) => {
          console.error(
            redact(`Error: ${error instanceof Error ? error.message : error}`),
          );
          process.exitCode = 1;
        },
      );
  } catch (error) {
    console.error(
      redact(`Error: ${error instanceof Error ? error.message : error}`),
    );
    process.exitCode = 1;
  }
}
