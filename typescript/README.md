# Rime SDK for Node.js

Generate speech with TTS, or send speech to Prism and receive a spoken reply.
Both APIs use the `Rime` client. Your application handles audio capture and playback.

## Install

Requires Node.js 22 or later. This alpha package uses ESM and includes TypeScript types.
Run it on a server. Browser use is not supported.

```sh
mkdir rime-quickstart
cd rime-quickstart
npm init -y
npm install @rimelabs/sdk wavefile
export RIME_API_KEY="your-api-key"
```

Get a TTS API key from the [Rime dashboard](https://app.rime.ai/).
An explicit `new Rime({ apiKey: "..." })` overrides `RIME_API_KEY`. The SDK does not load `.env` files.
The commands below use a macOS or Linux shell.

These examples use JavaScript `.mjs` files so you can run them without a TypeScript build.
The additional [wavefile package](https://github.com/rochars/wavefile) reads and writes WAV files
for the examples. It is not required by the SDK.

## TTS: save speech

Save this as `tts.mjs`. It writes a playable `speech.wav` file with no audio-device dependencies.

```javascript
import { writeFileSync } from "node:fs";
import wavefile from "wavefile";
import { Rime } from "@rimelabs/sdk";

const client = new Rime({ timeout: 60 });
try {
  const chunks = [];
  for await (const chunk of client.tts.stream(
    "Your appointment is confirmed for tomorrow.",
  )) {
    chunks.push(Buffer.from(chunk));
  }
  const pcm = Buffer.concat(chunks);
  const samples = Array.from({ length: pcm.length / 2 }, (_, i) =>
    pcm.readInt16LE(i * 2),
  );
  const wav = new wavefile.WaveFile();
  wav.fromScratch(1, 24000, "16", samples);
  writeFileSync("speech.wav", wav.toBuffer());
  console.log("Saved speech.wav");
} finally {
  await client.close();
}
```

Run `node tts.mjs`, then open `speech.wav` in your audio player.
This short example collects the audio in memory before writing the file.
For long responses, write audio chunks as they arrive.

Coda is the default model. Use `new Rime({ model: "mistv3" })` for Mist v3, or pass
`{ voice: "your-voice" }` as the second argument to `client.tts.stream(...)` to select a voice.
The default output is mono 24 kHz PCM16. The example adds its WAV header.
To supply text from an LLM, pass an async iterable of strings instead of a string.
The SDK handles sentence boundaries.

## Prism: send recorded speech

Prism needs a deployment that serves its realtime API. Obtain the endpoint,
credentials, and voice from your deployment operator. The TTS endpoint does not serve Prism.
Set `RIME_API_KEY` to the credential for that deployment if it differs from your TTS key.

```sh
export PRISM_URL="wss://your-prism-host/v1/realtime"
export PRISM_VOICE="your-deployment-voice"
```

You can omit `PRISM_VOICE` if the deployment has a default voice.
Record a short question and export it as `question.wav`, using **16 kHz, mono, signed 16-bit PCM WAV**.
Save the following as `prism.mjs` beside that file. It sends the recording at capture speed,
then sends silence so Prism can detect the end of speech. The spoken reply goes to `reply.wav`.

```javascript
import { readFileSync, writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import wavefile from "wavefile";
import { Rime, RimeRealtimeError } from "@rimelabs/sdk";

const input = new wavefile.WaveFile(readFileSync("question.wav"));
if (
  input.container !== "RIFF" ||
  input.fmt.audioFormat !== 1 ||
  input.fmt.numChannels !== 1 ||
  input.fmt.sampleRate !== 16000 ||
  input.bitDepth !== "16"
) {
  throw new Error("question.wav must be 16 kHz mono PCM16");
}
const data = Buffer.from(input.data.samples);
if (!data.length || data.length % 2)
  throw new Error("question.wav has no complete PCM16 audio");
const endpoint = process.env.PRISM_URL;
if (!endpoint)
  throw new Error("Set PRISM_URL to your deployment's /v1/realtime URL");

const client = new Rime();
const stop = new AbortController();
const deadline = setTimeout(() => {
  stop.abort();
  void client.close();
}, 60_000);
let sending;
let reading;
try {
  const session = await client.realtime.connect({
    endpoint,
    voice: process.env.PRISM_VOICE,
    instructions: "Give short, clear answers.",
  });
  sending = (async () => {
    for (let offset = 0; ; offset += 1280) {
      const frame = Buffer.alloc(1280);
      if (offset < data.length)
        data.copy(frame, 0, offset, Math.min(offset + 1280, data.length));
      await session.sendAudio({ data: frame });
      await sleep(40, undefined, { signal: stop.signal });
    }
  })();
  const chunks = [];
  reading = (async () => {
    for await (const { payload } of session.events) {
      if (payload.kind === "audio.delta")
        chunks.push(Buffer.from(payload.audio.data));
      if (payload.kind === "error") throw new RimeRealtimeError(payload.error);
      if (payload.kind === "response.ended") {
        if (payload.status !== "completed") {
          throw new Error(`Response ${payload.status}: ${payload.reason}`);
        }
        return;
      }
    }
    throw new Error("Session closed before the reply ended");
  })();
  await Promise.race([sending, reading]);
  const pcm = Buffer.concat(chunks);
  const samples = Array.from({ length: pcm.length / 2 }, (_, i) =>
    pcm.readInt16LE(i * 2),
  );
  const wav = new wavefile.WaveFile();
  wav.fromScratch(1, 24000, "16", samples);
  writeFileSync("reply.wav", wav.toBuffer());
  console.log("Saved reply.wav");
} finally {
  stop.abort();
  clearTimeout(deadline);
  await client.close();
  await Promise.allSettled([sending, reading]);
}
```

Run `node prism.mjs`, then open `reply.wav`. This example has a 60-second deadline
and collects the reply in memory. It saves audio without playing it, so it sends no playback receipt.
For live conversations, your application must capture microphone audio, play replies,
stop playback on interruption, and report the actual played position to Prism.
Keep the API key on your server when you add a browser interface.
TTS settings do not configure Prism sessions.

## Help and licenses

See [Rime documentation](https://docs.rime.ai/) for API keys, TTS models, and deployment information.
The examples above run with the installed package. No source checkout is required.

SDK code uses the MIT license. The Prism protocol module uses Apache 2.0.
BlingFire retains its MIT license. License texts and notices are included in the package.
Its WASM runtime ships with the package. The SDK downloads no code or models at runtime.
