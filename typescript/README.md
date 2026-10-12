# Rime SDK for TypeScript and Node.js

Generate speech with Coda or Mist v3, transcribe audio with STT, and build spoken
conversations with Prism through the `Rime` client. Your application handles
audio capture and playback.

## Installation

Requires **Node.js 22 or later**. Install the alpha package from npm. It uses
ESM, includes TypeScript types, and runs on a server; browsers are not supported.

```sh
mkdir rime-quickstart
cd rime-quickstart
npm init -y
npm install @rimelabs/sdk wavefile
export RIME_API_KEY="your-api-key"
```

For an existing project, skip the first three commands. Get an API key from the
[Rime dashboard](https://app.rime.ai/). A key is required: `new Rime()` reads
`RIME_API_KEY`, or you can pass `new Rime({ apiKey: "..." })` to override it.
The SDK does not load `.env` files. Shell commands below assume macOS or Linux.

Examples use JavaScript `.mjs` files so you can run them without a TypeScript build.
The additional `wavefile` dependency is only needed by the Prism example; TTS and
STT use the SDK and Node.js built-ins.

## Prism: send recorded speech

Prism needs a deployment that serves its realtime API. Obtain the endpoint,
credentials, and voice from your deployment operator. The TTS endpoint does not serve Prism.
Set `RIME_API_KEY` to the credential for that deployment if it differs from your TTS key.

```sh
export PRISM_URL="wss://your-prism-host/v1/realtime"
export PRISM_VOICE="your-deployment-voice"
```

The announced public Prism host is `prism.api.rime.ai:443`. The service is not yet
available. Confirm availability and the `/v1/realtime` route with your deployment
operator before setting `PRISM_URL` to `wss://prism.api.rime.ai/v1/realtime`.

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

## TTS

Use `client.tts.synthesize(text, options)` for a complete string, or
`client.tts.stream(textChunks, options)` for an async iterable of strings.
Both return an `AudioStream` immediately; consume it with `for await`, without
awaiting the method call. For complete audio in memory, await
`client.tts.synthesizeToBuffer(text, options)` to receive a Node.js `Buffer`.
Text is required and must contain non-whitespace characters.

### Client options

These optional `new Rime({...})` fields configure TTS. They do not configure STT or Prism.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `model` | Select `"coda"` or `"mistv3"`. | No | `"coda"` |
| `endpoint` | Override the TTS TLS hostname and optional port, such as `"coda.api.rime.ai:443"`; no scheme or path. | No | `coda.api.rime.ai:443` for Coda; `mist.api.rime.ai:443` for Mist v3. |
| `timeout` | Overall TTS timeout in positive seconds, such as `60`; `null` disables it. | No | No overall timeout. |

### Synthesis options

All three TTS methods accept the same `SynthesisOptions` fields. Import
`AudioFormat` from `@rimelabs/sdk` when selecting an encoding.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `voice` | Voice name for the selected model, such as `"clementine"` or `"astra"`. | No | `"clementine"` for Coda; `"astra"` for Mist v3. |
| `language` | Spoken language code, such as `"en"` or `"es"`; the service validates support. | No | `"en"` |
| `audioFormat` | Output encoding: `AudioFormat.PCM`, `.WAV`, `.MP3`, `.PCM_24000`, or `.MULAW_8000`. | No | Raw PCM16 at 24 kHz (`PCM_24000`). |
| `sampleRate` | Requested output rate in Hz: `8000`, `16000`, or `24000`. | No | `24000`; `8000` for `MULAW_8000`. |
| `timeScaleFactor` | Duration multiplier from `0.4` to `2.5`; below `1` speeds speech up, above `1` slows it down. | No | Omitted; normal duration (`1`). |
| `timestamps` | `true` requests word timings; supported only with Mist v3. | No | `false` |
| `customLexicon` | Array of `{ spelling, pronunciation }` entries for Coda; pronunciations use space-separated X-SAMPA strings. | No | Empty array. |
| `timeout` | Override the overall timeout with positive seconds, or `null` to disable it. | No | Inherits the client timeout. |

Discover valid values with `await client.voices.list({ language: "en" })` and
`await client.languages.list()`. Omit the voice language filter to list all voices
for the selected model. Both calls accept an options object with `timeout` in
seconds; discovery has an additional ten-second maximum.

### Example: save an IVR greeting

Save as `tts.mjs`. This writes a complete 16 kHz WAV file, ready for playback or upload.

```javascript
import { writeFile } from "node:fs/promises";
import { AudioFormat, Rime } from "@rimelabs/sdk";

const client = new Rime({ timeout: 60 });
try {
  const audio = await client.tts.synthesizeToBuffer(
    "Thank you for calling. Your appointment is confirmed for tomorrow.",
    {
      audioFormat: AudioFormat.WAV,
      sampleRate: 16000,
      timeScaleFactor: 0.9,
    },
  );
  await writeFile("greeting.wav", audio);
  console.log("Saved greeting.wav");
} finally {
  await client.close();
}
```

Run `node tts.mjs`, then open `greeting.wav` in an audio player.

### Audio and stream behavior

All output is mono. PCM is signed 16-bit little-endian audio without a header;
WAV contains PCM16; MP3 is encoded by the service. PCM/WAV rates are requested
from the server. MP3 headers determine the actual encoded rate. The fixed
`PCM_24000` and `MULAW_8000` presets reject conflicting sample rates; mu-law is
converted locally from 24 kHz PCM. Read `audio.sampleRate` on an `AudioStream`
for the requested rate.

The buffer helper waits for successful completion and finalizes WAV lengths.
Its fixed limit is **64 MiB per call**, including headers; exceeding it throws
`RimeResourceLimitError` without returning partial audio. WAV/MP3 stream chunks
are fragments of one file. Use the buffer helper when you need a complete WAV.

For incremental text, send fragments as they arrive; the service combines them
into sentences. Consume streaming audio to the end to confirm success, since an
error can follow earlier audio. Use `await audio.cancel()` when stopping early
and `await client.close()` in a `finally` block to release the client. Synthesis
is not retried automatically, and overall timeouts include pauses by the caller;
connection and progress limits also apply.

For timestamps, enable `timestamps: true`, consume the audio stream fully, then
call `await audio.timestamps()`. The result has `status` and `spans` with word
text and start/end seconds. Nonzero alignment status does not fail the audio.
The buffer helper returns only bytes, so use an audio stream to access timings.
Custom lexicons apply to the whole request; the service validates pronunciations
and model/language support.

## STT

### Transcribe a complete WAV recording

For a complete recording, use the native unary `Transcribe` RPC through the SDK.
Pass **complete WAV bytes, including the header**, up to **16 MiB (16,777,216
bytes)**. PCM16 mono 16 kHz WAV is the portable format. The SDK sends the file
unchanged; it does not decode, resample, or add a header. The service validates
its encoding and the supplied language and recognition hints.

The result is a `Transcription` containing final text, resolved language, and a
request ID when available. Silence may return empty text. There are no partial
updates or automatic retries. TTS client settings do not apply. An optional
overall timeout includes connection and upload; the SDK also bounds connection
time and the unary request (120 seconds). Timeout, cancellation, and service
errors use the existing SDK error categories. A request ID is included when the
transport has made it available.

```typescript
import { readFile } from "node:fs/promises";
import { Rime } from "@rimelabs/sdk";

const client = new Rime();
try {
  const result = await client.stt.transcribe(await readFile("utterance.wav"), {
    language: "en",
    mode: "written",
    contextTerms: ["Super-G", "USOC"],
    timeout: 120,
  });
  console.log(result.text, result.language, result.requestId);
} finally {
  await client.close();
}
```

`TranscribeOptions` also accepts an `AbortSignal` as `signal`. Aborting it or
closing the client cancels pending requests with `RimeCancelledError`.

### Stream an utterance

Use `client.stt.stream(audioChunks, options)` to transcribe one utterance.
The required source is an async iterable of headerless signed PCM16 little-endian
`Uint8Array` chunks (`Buffer` also works). Ending the source ends the utterance;
your application decides when to stop.

STT uses gRPC over TLS to `stt.api.rime.ai:443`, with the client's API
key. There is no STT model, endpoint, or protocol selector. TTS client settings,
including its timeout, do not apply to STT.

### Transcription options

Pass these fields in the required `TranscriptionOptions` object.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `language` | Spoken BCP-47 language tag, such as `"en"` or `"es"`; passed to the service. | Yes | None; no automatic detection. |
| `mode` | `"written"` normalizes written text; `"verbatim"` prefers spoken wording. | No | `"written"` |
| `contextTerms` | Array of recognition hints, such as `["Rime"]`; hints do not force a match. | No | Empty array. |
| `inputFormat` | Object with `sampleRate` (`8000`, `16000`, `24000`, `48000`), `channels` (`1` or `2`), and `encoding` (only `"pcm_s16le"`). | No | 16 kHz mono PCM16; each field is optional. |
| `timeout` | Overall timeout in positive seconds, including source/consumer pauses; `null` disables it. | No | No overall timeout; independent of the client timeout. |
| `signal` | An `AbortSignal`, such as `controller.signal`, to cancel this utterance. | No | No signal. |

### Example: transcribe a recording

Save a short recording as `utterance.pcm`: **16 kHz, mono, signed PCM16
little-endian, with no WAV header**. Save this as `stt.mjs`.

```javascript
import { createReadStream } from "node:fs";
import { Rime } from "@rimelabs/sdk";

async function* audioChunks() {
  yield* createReadStream("utterance.pcm");
}

const client = new Rime();
try {
  const transcript = client.stt.stream(audioChunks(), {
    language: "en",
    timeout: 120,
  });
  for await (const update of transcript) {
    console.log(update.kind, update.text);
  }
} finally {
  await client.close();
}
```

Run `node stt.mjs`. Each partial update replaces the **complete current
transcript**; do not append snapshots. A final update confirms successful
completion and may be empty for silence. Errors do not turn partials into finals.

The SDK converts supported input formats to 16 kHz mono. Stereo must be
interleaved. WAV headers, MP3, and mu-law input are not accepted. Chunks may split
sample frames, but an incomplete final frame fails the request. The SDK does not
provide microphone capture, automatic end-of-speech detection, word timings, or
diarization. Recognition is not retried automatically.

Use `await transcript.cancel()` or the supplied `AbortSignal` to stop one
utterance. Closing the client cancels all its operations. Audio sources must
yield without blocking the event loop. Connection, acceptance, and completion
limits still apply when the overall timeout is disabled.

## Help and licenses

See the [TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/tts.md),
[STT guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/stt.md), and
[Prism guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/realtime.md)
for more examples. Visit [Rime documentation](https://docs.rime.ai/) for service
information, or report SDK issues on [GitHub](https://github.com/rimelabs/rime-sdk/issues).
Include the SDK version and the error's `requestId` when available; omit API keys.

SDK code uses the [MIT license](LICENSE). The Prism protocol module uses
[Apache 2.0](LICENSE-PRISM); see its [notices](NOTICE-PRISM).
License texts and notices are included in the distribution.
