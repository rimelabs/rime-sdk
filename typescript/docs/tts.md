# TTS for Node.js

You supply text; the SDK handles sentence boundaries and streams audio back.
Your application owns playback. Start with the complete WAV example in the
[package quick start](../README.md#tts-save-speech).

## Complete text

Use `client.tts.synthesize(text)` for one nonblank string. It returns an audio
stream; it does not wait for all audio to be generated. Do not await this call.
Use the audio loop and cleanup shown in the package quick start.

## Stream incoming text

Use `client.tts.stream(chunks)` for an async iterable of strings. A plain string
belongs in `synthesize`, not `stream`. The source can supply partial sentences:

```javascript
async function* text() {
  yield "Hello. ";
  yield "This text arrives in separate chunks.";
}
```

Use `client.tts.stream(text())` with the same file write and cleanup.
The SDK handles sentence boundaries. Do not await `tts.stream()`; the first
iterator read starts work. The quick start collects chunks before writing a WAV file.

## Configuration

Select Mist v3 with `new Rime({ model: "mistv3" })`. Mist v1 and v2 are not supported.

| Model | Standard endpoint | Default voice |
| --- | --- | --- |
| `coda` | `coda.api.rime.ai:443` | `clementine` |
| `mistv3` | `mist.api.rime.ai:443` | `astra` |

### Client options

| `new Rime({ ... })` option | Default | Meaning |
| --- | --- | --- |
| `apiKey` | `RIME_API_KEY` | API key; an explicit value overrides the environment |
| `model` | `"coda"` | Model for speech and discovery |
| `endpoint` | Model's standard endpoint | Custom hostname and optional port for speech and discovery |
| `timeout` | No overall deadline | Positive seconds; `null` disables the overall deadline |

Authentication uses bearer tokens over TLS. The SDK does not load `.env` files.

Custom deployments use `new Rime({ model: "coda", endpoint: "host:8443" })`.
Omit the scheme and path. TLS is required; the default port is `443`.
Model defaults still apply.

### Synthesis options

Both methods require text input: a nonblank string for `synthesize`, or an
async iterable of strings for `stream`. Both accept the following optional settings:

| Option | Default | Meaning |
| --- | --- | --- |
| `voice` | Model's default voice | Voice name from `client.voices.list()` |
| `language` | `"en"` | Language code |
| `audioFormat` | `AudioFormat.PCM_24000` | Output profile, imported from `@rimelabs/sdk` |
| `timeout` | Client setting | Seconds; pass `null` to disable the overall deadline |

### Audio formats

Both profiles return raw audio without a file header.

| `AudioFormat` profile | Encoding | Sample rate | Channels |
| --- | --- | --- | --- |
| `PCM_24000` | Signed 16-bit little-endian PCM | 24 kHz | Mono |
| `MULAW_8000` | G.711 mu-law | 8 kHz | Mono |

Chunks are `Uint8Array` values with complete sample frames; sizes vary. Inspect the
read-only `audio.format.encoding`, `audio.format.sampleRate`, and `audio.format.channels`.

### Timeouts

An overall deadline includes pauses between reads. Disabling it leaves internal
connection and progress limits active. Discovery allows at most 10 seconds;
a shorter supplied timeout takes precedence. Omit an operation's `timeout` or
pass `undefined` to inherit the client setting.

## Discover voices and languages

List voices and languages while the client is open:

```javascript
const voices = await client.voices.list({ language: "en" });
const languages = await client.languages.list();
console.log(voices, languages);
```

Both return string arrays and accept `timeout`. Omit `language` to disable filtering.

## Errors and cancellation

Report SDK errors and request IDs while closing the client on failure:

```javascript
import { writeFile } from "node:fs/promises";
import { Rime, RimeError } from "@rimelabs/sdk";

let client;
try {
  client = new Rime();
  await writeFile("speech.pcm", client.tts.synthesize("Hello. This is Rime."));
} catch (error) {
  if (error instanceof RimeError) {
    console.error(`Speech failed: ${error.message}; requestId=${error.requestId}`);
  }
  throw error;
} finally {
  await client?.close();
}
```

Each SDK error inherits from `RimeError`:

| Error | Meaning |
| --- | --- |
| `RimeAuthenticationError` | Missing or rejected credentials |
| `RimePermissionError` | Access denied |
| `RimeInputError` | Invalid input or client use |
| `RimeResourceLimitError` | Service resource limit reached |
| `RimeUnavailableError` | Service unavailable |
| `RimeTimeoutError` | Overall or internal deadline reached |
| `RimeAudioFormatError` | Unsupported or unexpected audio format |
| `RimeCancelledError` | Operation cancelled |
| `RimeStreamError` | Other stream or transport failure |

Partial audio can arrive before an error. Output is complete only after successful
iteration. Synthesis is not retried automatically.

Keep cleanup when stopping early. A `for await` loop exit cancels the stream.
For manual reads, call `await audio.cancel()` in `finally`. `await client.close()`
stops all client work. Both calls are safe to repeat; both objects support
`Symbol.asyncDispose`. Cancellation stops the RPC and limits cleanup time, but
cannot force your text source to finish an unrelated promise.
