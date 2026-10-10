# TTS for Node.js

You supply text; the SDK handles sentence boundaries and streams audio back.
Your application owns playback. Start with the complete WAV example in the
[package quick start](../README.md#tts-save-speech).

## Stream incoming text

An async text source can supply text as it arrives:

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

The `text` argument is required and accepts a nonblank string or an async text
source. All options in `client.tts.stream(text, { ... })` are optional:

| Option | Default | Meaning |
| --- | --- | --- |
| `voice` | Model's default voice | Voice name from `client.voices.list()` |
| `language` | `"en"` | Language code |
| `audioFormat` | `AudioFormat.PCM_24000` | Output profile, imported from `@rimelabs/sdk` |
| `timestamps` | `false` | Request final word timestamps; requires `model: "mistv3"` |
| `customLexicon` | `[]` | Array of pronunciation overrides for this request; requires Coda |
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

## Custom pronunciations (Coda)

Pass words or phrases and their space-separated X-SAMPA pronunciations:

```javascript
import { writeFile } from "node:fs/promises";
import { Rime, RimeInputError } from "@rimelabs/sdk";

const lexicon = [{ spelling: "hello", pronunciation: 'h @ . " l oU' }];
const client = new Rime({ model: "coda", timeout: 60 });
try {
  await writeFile("speech.pcm", client.tts.stream("Hello world.", {
    language: "en",
    customLexicon: lexicon,
  }));
} catch (error) {
  if (error instanceof RimeInputError) {
    console.error(`Invalid synthesis request: ${error.message}; requestId=${error.requestId}`);
  }
  throw error;
} finally {
  await client.close();
}
```

TypeScript callers can import the `PronunciationEntry` type from `@rimelabs/sdk`.
The SDK copies the array and entries when `tts.stream()` is called. The same
lexicon applies to the entire request, including an async text source;
subsequent requests have their own options. Omit the option or pass an empty
array for default pronunciation.

The service matches whole words or phrases case-insensitively and applies the
pronunciation after text normalization. Longest matches take precedence; the
last entry for a repeated spelling wins. Inline `pronounce(...)` directives are
not supported.

Coda accepts custom pronunciations in German, English, Spanish, French,
Italian, and Portuguese, with up to 500 entries per request. The service validates
supported languages and pronunciation rules. Mist rejects nonempty lexicons.

Malformed option types and invalid Unicode in lexicon fields throw locally.
The service rejects unsupported models or languages, oversized lexicons, and
ill-formed entries with `RimeInputError`
before producing audio. Catch it around consuming the stream. Its `message`
preserves the offending spelling and checks such as `no-primary-stress` or
`unknown-phone`, and `requestId` identifies the request. The whole request fails;
entries are never silently skipped or repaired.

## Word timestamps (Mist v3)

Enable timestamps on a synthesis, consume all audio, then read its result:

```javascript
import { writeFile } from "node:fs/promises";
import { Rime } from "@rimelabs/sdk";

const client = new Rime({ model: "mistv3", timeout: 60 });
try {
  const audio = client.tts.stream("Hello world.", { timestamps: true });
  await writeFile("speech.pcm", audio);
  const result = await audio.timestamps();
  if (result.status.code === 0) {
    for (const word of result.spans) {
      console.log(word.text, word.start, word.end);
    }
  } else {
    console.log("Timestamps unavailable:", result.status.code, result.status.message);
  }
} finally {
  await client.close();
}
```

`TimestampResult`, `TimestampStatus`, and `WordTimestamp` are exported types from
`@rimelabs/sdk`. The result is immutable. `spans` is an array of words with `text`,
`start`, and `end`; times are seconds from the beginning of the entire synthesis,
including when text arrives incrementally. Resampling to `MULAW_8000` does not
change these times. Words reflect normalized spoken text, so a number may become
several words. The model supplies the timings; the SDK does not estimate them.

Timestamps arrive once, after generation finishes. They are not incremental
events for live interruption handling. `timestamps()` never consumes audio or
waits for you to consume it: calling before successful iteration finishes, or
without enabling timestamps, rejects with `RimeInputError`. Results remain
accessible after a successfully consumed stream or its client is closed.

`status.code` is a numeric `google.rpc.Code`: `0` is success. Nonzero codes such as
`12` (unimplemented) or `4` (deadline exceeded) carry an explanation and no spans;
they do not fail successful audio. Missing or malformed timestamp responses reject
with `RimeStreamError` only when reading timestamps. A synthesis error or
cancellation also prevents reading a successful timestamp result. Enabling
timestamps with Coda throws `RimeInputError` before any request is sent.

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
  await writeFile("speech.pcm", client.tts.stream("Hello. This is Rime."));
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

Service errors retain their diagnostic message and request ID when provided.
Use the exception type for programmatic handling; message wording may change.

Partial audio can arrive before an error. Output is complete only after successful
iteration. Synthesis is not retried automatically.

Keep cleanup when stopping early. A `for await` loop exit cancels the stream.
For manual reads, call `await audio.cancel()` in `finally`. `await client.close()`
stops all client work. Both calls are safe to repeat; both objects support
`Symbol.asyncDispose`. Cancellation stops the RPC and limits cleanup time, but
cannot force your text source to finish an unrelated promise.

## Complete text input

`completeText: true` selects the `Synthesize` RPC, which sends the full input in one request
and streams audio back. The default still uses `SynthesizeStreaming` with
incremental sentence input, including when given a string. Full-text mode accepts
only a string of at most 65,536 UTF-8 bytes; collect an incremental source in the
application first. Server-side text splitting
applies. Lexicon overrides, error details and request IDs, cancellation, audio
profiles, and Mist v3 timestamps use the same SDK behavior in both modes.

```ts
const audio = client.tts.stream("Please read this.", {
  completeText: true,
  customLexicon: [{ spelling: "read", pronunciation: '" r\\ E d' }],
});
for await (const chunk of audio) {
  // Play or save each PCM chunk.
}
```
