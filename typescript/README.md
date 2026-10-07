# Rime SDK for Node.js

Stream speech from a string or async text source. The SDK handles sentence
boundaries, authentication, and audio conversion; your application owns playback.

## Install

Requires Node.js 22 or later. This alpha package uses ESM and includes TypeScript
types. Browser use is not supported.

```sh
npm install @rimelabs/sdk@next
export RIME_API_KEY="your-api-key"
```

An explicit `new Rime({ apiKey: "..." })` overrides the environment. The SDK
does not load `.env` files.

## TTS quick start

Save this as `speech.mjs` and run `node speech.mjs`:

```javascript
import { writeFile } from "node:fs/promises";
import { Rime } from "@rimelabs/sdk";

const client = new Rime();
try {
  const audio = client.tts.stream("Hello. This is Rime.");
  await writeFile("speech.pcm", audio);
  console.log(audio.requestId);
} finally {
  await client.close();
}
```

Output is raw mono 24 kHz signed little-endian PCM16, with no WAV header.
The default model is Coda. Select Mist v3 with `new Rime({ model: "mistv3" })`.

For streamed text, voices, audio formats, and cancellation, read the
[TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/tts.md).
Runnable scripts are in the [examples](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md).

## Realtime / Prism

Prism is currently available in the [Python SDK](https://github.com/rimelabs/rime-sdk/blob/main/python/README.md).
This package does not yet expose `client.realtime`.

## License

The SDK uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/LICENSE).
BlingFire retains its [upstream license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/LICENSE.blingfire).
Its WASM runtime ships with the package; the SDK downloads no code or models at
runtime. See [vendor provenance](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/README.md).
