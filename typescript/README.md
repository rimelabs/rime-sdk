# Rime SDK for Node.js

Requires Node.js 22 or later. The package uses ESM and includes TypeScript types.
Build and install the local package:

```sh
npm ci
npm run build
# In your application:
npm install /path/to/rime-sdk/typescript
```

```javascript
import { writeFile } from "node:fs/promises";
import { Rime, AudioFormat } from "@rimelabs/sdk";

const client = new Rime({ apiKey: "your-api-key" });
try {
  const audio = client.tts.stream("Hello. This is Rime.", {
    voice: "clementine",
    audioFormat: AudioFormat.PCM_24000,
  });
  await writeFile("speech.pcm", audio);
  console.log(audio.requestId);
} finally {
  await client.close();
}
```

Omit `apiKey` to read `RIME_API_KEY`. The default model is `coda`.
The default voice is `clementine`; the default language is `en`.
`tts.stream()` also accepts `AsyncIterable<string>`. Do not await the factory.
The first iterator read starts work. A `for await` loop exit cancels the stream.
Use `await audio.cancel()` in a `finally` block if you read the iterator manually.

`await client.voices.list({ language: "en" })` returns voice names.
`await client.languages.list()` returns language codes.
Both accept an options object with `timeout` in seconds.

`new Rime({ timeout: 30 })` sets an overall timeout. An operation can replace
it with `{ timeout: 10 }` or disable it with `{ timeout: null }`.
An omitted or `undefined` option inherits the client setting. Internal connection
and progress limits still apply. An overall timeout continues during caller pauses.

`audio.format` is a read-only `AudioFormat` with `encoding`, `sampleRate`, and
`channels`. `PCM_24000` is raw signed 16-bit little-endian mono PCM at 24 kHz.
`MULAW_8000` is raw mono G.711 mu-law at 8 kHz. Neither includes a file header.
The iterator yields `Uint8Array` chunks with complete sample frames.
Chunk sizes are not fixed.

`await audio.cancel()` cancels one operation. `await client.close()` cancels all
client work. Both are idempotent. Both objects support `Symbol.asyncDispose`.
Normal iterator completion means that the final service status was successful.
Partial audio can precede a typed error. Synthesis is never replayed.

Catch `RimeError` or one of `RimeAuthenticationError`, `RimePermissionError`,
`RimeInputError`, `RimeResourceLimitError`, `RimeUnavailableError`,
`RimeTimeoutError`, `RimeAudioFormatError`, `RimeCancelledError`, or
`RimeStreamError`. Errors expose a message and optional `requestId`.
Cancellation cannot stop application code that waits on an unrelated promise.
The SDK stops its RPC and limits how long it waits for source cleanup.

The API-key exchange currently follows the assumed private contract in
[authentication](../docs/authentication.md). It has local tests, but no live
service validation. Browser use is not supported.

## Development

```sh
npm ci
npm run check
npm run lint
npm test
npm pack
```

The package includes a pinned Microsoft BlingFire WASM binary and its license.
It does not download code at runtime. See [vendor provenance](vendor/README.md).
