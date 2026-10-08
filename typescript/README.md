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

Save playable speech with the
[WAV example](https://github.com/rimelabs/rime-sdk/blob/main/examples/typescript/tts/save.ts).
After the [example setup](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md#nodejs), run from the repository root:

```sh
npm --prefix examples/typescript run tts:save
```

Open `examples/typescript/speech.wav` in your audio player. To hear streamed text
as it arrives, run `npm --prefix examples/typescript run tts:play`, open the printed
local URL, and press **Start**. **Stop** cancels generation and playback.

Replace the example's async text source with your LLM's text stream. The SDK
handles sentence boundaries. Coda is the default; select Mist v3 with
`new Rime({ model: "mistv3" })`.
The [TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/tts.md)
covers voices, formats, and cancellation.

## Prism quick start

Start a voice conversation in your browser. The Node.js server uses the SDK;
the browser captures your microphone and plays replies. Use headphones, and
speak during a reply to interrupt it. The API key stays on the local server.

Set `PRISM_URL` to your deployment's full WebSocket URL ending in `/v1/realtime`.
Set `PRISM_VOICE` unless that deployment has a default. Obtain these values from
your deployment operator; the TTS endpoint does not serve Prism.

After the [example setup](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md#nodejs), run from the repository root:

```sh
npm --prefix examples/typescript run prism:voice
```

Open the printed local URL and press **Start**. Without a microphone, send the
included speech recording and save `reply.wav`:

```sh
npm --prefix examples/typescript run prism:recorded
```

Run `prism:voice-tools` for the same conversation with a demo order lookup.
The [Realtime guide](https://github.com/rimelabs/rime-sdk/blob/main/typescript/docs/realtime.md)
covers audio, tools, playback reports, errors, and optional typed input.
TTS settings do not configure Prism sessions.

## License

SDK code uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/LICENSE).
The Prism protocol module uses the [Apache 2.0 license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/LICENSE-PRISM).
BlingFire retains its [upstream license](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/LICENSE.blingfire).
Its WASM runtime ships with the package; the SDK downloads no code or models at
runtime. See [vendor provenance](https://github.com/rimelabs/rime-sdk/blob/main/typescript/vendor/README.md).
