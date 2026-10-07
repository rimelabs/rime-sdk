# Rime SDK for Node.js

Stream speech from a string or async text source. The SDK handles sentence
boundaries, authentication, and audio conversion; your application owns playback.
Use Prism for conversations with audio input, typed turns, and tools.

## Install

Requires Node.js 22 or later. This alpha package uses ESM and includes TypeScript
types. Browser use is not supported.

```sh
npm install @rimelabs/sdk
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

## Prism quick start

Set `PRISM_URL` to your deployment's full `wss://host/v1/realtime` endpoint.
Supply `PRISM_VOICE` unless the deployment has a default voice.

```javascript
import { Rime, RimeRealtimeError } from "@rimelabs/sdk";

const client = new Rime();
const deadline = setTimeout(() => void client.close(), 60_000);
let reading;
try {
  const session = await client.realtime.connect({
    endpoint: process.env.PRISM_URL,
    voice: process.env.PRISM_VOICE,
  });
  reading = (async () => {
    for await (const { payload } of session.events) {
      if (payload.kind === "text.delta") process.stdout.write(payload.delta);
      if (payload.kind === "error") throw new RimeRealtimeError(payload.error);
      if (payload.kind === "response.ended") {
        if (payload.status !== "completed")
          throw new Error(`Response ${payload.status}`);
        return;
      }
    }
    throw new Error("Session closed before the response ended");
  })();
  await Promise.all([reading, session.sendText("Hello!")]);
} finally {
  clearTimeout(deadline);
  await client.close();
  await reading?.catch(() => {});
}
```

This example prints text and discards audio. The
[Realtime guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/realtime.md)
covers audio input, tools, playback reports, and cancellation. Runnable
[Prism examples](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md#nodejs)
save audio and handle tools. TTS settings do not apply to Prism sessions.

## License

SDK code uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/LICENSE).
The Prism protocol module uses the [Apache 2.0 license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/LICENSE-PRISM).
BlingFire retains its [upstream license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/LICENSE.blingfire).
Its WASM runtime ships with the package; the SDK downloads no code or models at
runtime. See [vendor provenance](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/README.md).
