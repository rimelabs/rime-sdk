# Rime SDK for Node.js

Requires Node.js 22 or later. The package uses ESM and includes TypeScript types.
Browser use is not supported. This is an alpha release.
Install it from npm using the `next` tag:

```sh
npm install @rimelabs/sdk@next
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
The SDK selects the endpoint and default voice for the model:

| Model | Standard endpoint | Default voice |
| --- | --- | --- |
| `coda` | `coda.api.rime.ai:443` | `clementine` |
| `mistv3` | `mist.api.rime.ai:443` | `astra` |

Use `new Rime({ model: "mistv3" })` for Mist v3. Both models accept complete text
or an async text source and support voice and language discovery. The SDK rejects
`mist` and `mistv2`, which name older models in the existing Rime API.

For a custom deployment, select its model and set `endpoint`:

```javascript
const client = new Rime({ model: "coda", endpoint: "coda.api.customer-name.rime.ai" });
```

Use a hostname with an optional port, such as `host:8443`. Omit the scheme and path.
Connections always use TLS; the default port is `443`.
The endpoint applies to speech, voices, and languages for this client.

The default language is `en`. Set `voice` to replace the model's default voice.
Use `client.voices.list()` to find voices for the selected deployment.
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

The SDK sends the API key as a bearer token over TLS.

## Development for contributors

These instructions require access to the source repository.
Run the commands from its `typescript/` directory:

```sh
npm ci
npm run check
npm run lint
npm test
npm pack
```

The package includes a pinned Microsoft BlingFire WASM binary and its license.
It does not download code at runtime. See [vendor provenance](vendor/README.md).

## License

The SDK is licensed under the [MIT License](LICENSE).
BlingFire retains its [upstream license](vendor/LICENSE.blingfire).
