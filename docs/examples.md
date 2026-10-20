# SDK examples

Each SDK keeps its examples in its own directory. Inside this repository, all
examples use the local SDK. Python and TypeScript declare separate example
dependencies and share their SDK's workspace installation.

Run the commands below from the repository root. Live examples need a Rime API
key. Prism requires only the API key and uses the public endpoint and server
default voice. The endpoint and voice are optional overrides.

| Use case | Python | TypeScript | Go | Rust |
| --- | --- | --- | --- | --- |
| TTS: save speech | [save.py](../python/examples/tts/save.py) | [save.ts](../typescript/examples/tts/save.ts) | [save](../go/examples/save/main.go) | [save.rs](../rust/examples/save.rs) |
| TTS: play streamed text | [play.py](../python/examples/tts/play.py) | [play.ts](../typescript/examples/tts/play.ts) | | |
| STT: transcribe audio | [stream.py](../python/examples/stt/stream.py) | [stream.mjs](../typescript/examples/stt/stream.mjs) | [transcribe](../go/examples/transcribe/main.go) | [transcribe.rs](../rust/examples/transcribe.rs) |
| STT: terminal voice loop | [voice.py](../python/examples/stt/voice.py) | [voice.mjs](../typescript/examples/stt/voice.mjs) | [voice](../go/examples/voice/main.go) | |
| Prism: recorded speech | [recorded.py](../python/examples/realtime/recorded.py) | [recorded.ts](../typescript/examples/realtime/recorded.ts) | | |
| Prism: voice conversation | [voice.py](../python/examples/realtime/voice.py) | [voice.ts](../typescript/examples/realtime/voice.ts) | | |
| Prism: voice with tools | [voice_tools.py](../python/examples/realtime/voice_tools.py) | [voice-tools.ts](../typescript/examples/realtime/voice-tools.ts) | | |

Blank cells mean that this repository has no example for that combination.

The Prism browser starter in [`examples/prism-web/`](../examples/prism-web/) is
the exception to the layout above. It stands alone and pins the published Python
and Node.js packages, because the Prism SDK quickstart asks readers to copy it
and run it outside this repository. Its [README](../examples/prism-web/README.md)
has the run commands.
Shared recordings live in [`fixtures/audio/`](../fixtures/audio/).

The live audio paths have automated device simulations and protocol tests.
Physical microphone and speaker validation is still required before release.
See [manual checks](#manual-checks).

## Terminal STT voice loop

Use the [terminal voice guide](terminal-voice.md) to speak, inspect partial/final
transcripts, and hear the final text through TTS in Python, JavaScript or Go.
It uses Enter to start/end a turn and requires only a Rime API key and SoX.
The guide includes setup and device checks.

## Coda and Mist WebSocket streaming from the terminal

With `RIME_API_KEY` set and SoX installed (`brew install sox` on macOS), run:

```sh
uv run --directory python/examples --locked -m tts.coda_ws
```

This connects directly to Coda at `wss://coda.api.rime.ai/ws`.
For Mist v3, add `--model mistv3`:

```sh
uv run --directory python/examples --locked -m tts.coda_ws --model mistv3
```

This connects to `wss://mist.api.rime.ai/ws` with voice `astra`. Coda uses voice
`lyra` by default. Both models use the same JSON WebSocket protocol and support
the input and playback options below. Use `--url URL` to override the selected
model's address.

Enter complete sentences or stable clauses, one per line. Each line becomes a
`text` message in the same synthesis context, and audio plays as it arrives.
Enter `/end` to finish that turn. The WebSocket stays open for the next turn.
An unfinished input can pause synthesis mid-sentence until more text or `/end`
arrives, even when the last chunk ends in punctuation. Send `/end` as soon as
you have supplied all text for that turn. With an LLM, send it when the LLM's
text stream finishes, rather than waiting for audio to finish.

For listening to complete typed turns, add `--auto-end`:

```sh
uv run --directory python/examples --locked -m tts.coda_ws --auto-end
```

Each entered line sends a `text` message followed immediately by `end`. Audio
still streams back, and the same WebSocket serves subsequent turns. This mode
tests streaming audio with a single text chunk per turn. Omit `--auto-end` to
manually test multiple text chunks within one turn.

`/quit` finishes any active turn and exits; Ctrl+C immediately stops the client
and playback. Each completed turn saves a WAV in the printed temporary directory.
Use `--output-dir PATH` to choose the directory (turn filenames are reused on
subsequent runs), `--voice NAME` to select a voice, or `--no-playback` to save only.

A single command can also send multiple chunks with a pause between them:

```sh
uv run --directory python/examples --locked -m tts.coda_ws \
  --text "Hello from Coda." \
  --text "This sentence arrives two seconds later." --chunk-delay 2
```

The tester prints each sent chunk, the request ID, and elapsed time from `start`
to the first received audio byte. This timing excludes speaker buffering.

For Coda, compare initial text lookahead with the same text and voice:

```sh
# Streaming input: empty start, one text message, then immediate end.
uv run --directory python/examples --locked -m tts.coda_ws \
  --text "Your appointment is confirmed for tomorrow at ten."
# Same streaming input with additional initial text context.
uv run --directory python/examples --locked -m tts.coda_ws \
  --lookahead-tokens 8 --text "Your appointment is confirmed for tomorrow at ten."
```

`--lookahead-tokens` applies only to Coda and defaults to zero. More lookahead can trade
first-audio latency for additional text context. Compare saved WAVs for speech
quality and live playback for delivery gaps, and repeat samples because model
generation can vary between requests.

It uses the [Coda JSON WebSocket protocol](https://rimelabs-docs-coda-websocket-reference.mintlify.site/api-reference/coda/websockets)
to exercise incremental text input and streaming audio output.

## Python

Python 3.11+ and `uv` are required. The workspace installs the local SDK in
editable mode. Changes to Python source are available on the next run.

```sh
uv sync --project python/examples --locked
export RIME_API_KEY="your-api-key"
uv run --directory python/examples --locked -m tts.save
```

For microphone capture or playback, install the audio extra:

```sh
uv sync --project python/examples --locked --extra audio
uv run --directory python/examples --locked --extra audio -m tts.play
```

The audio extra uses PortAudio through `sounddevice`. If PortAudio is missing,
follow the [sounddevice installation instructions](https://python-sounddevice.readthedocs.io/en/latest/installation.html).
Device support depends on your OS. Grant microphone access to your terminal.

Prism examples use the public endpoint and server default voice. With
`RIME_API_KEY` set, run:

```sh
uv run --directory python/examples --locked -m realtime.recorded
uv run --directory python/examples --locked --extra audio -m realtime.voice
# After stopping that conversation:
uv run --directory python/examples --locked --extra audio -m realtime.voice_tools
```

Set `PRISM_URL` or `PRISM_VOICE` only to override the endpoint or voice in these
examples. The SDK itself does not read these two environment variables.

Use headphones. The Python example does not remove speaker echo. Keep the
microphone active during replies so Prism can hear interruptions. Ctrl-C closes
capture, playback, and the SDK connection. Use `--list-devices`, `--input-device N`,
or `--output-device N` with either voice script to choose devices.

## Node.js

Node.js 22+ is required. Install the npm workspace dependencies and build the
local SDK. Rebuild after changes to SDK source:

```sh
npm ci
npm --prefix typescript run build
export RIME_API_KEY="your-api-key"
npm --prefix typescript/examples run tts:save
npm --prefix typescript/examples run tts:play
```

For playback, open `http://127.0.0.1:3000` in a browser and press **Start**.
**Stop** cancels the request and clears playback. Ctrl-C stops the local server.

With `RIME_API_KEY` set, run one Prism example at a time. `PRISM_URL` and
`PRISM_VOICE` are optional overrides:

```sh
npm --prefix typescript/examples run prism:recorded
npm --prefix typescript/examples run prism:voice
# Or start the conversation with a demo order tool:
npm --prefix typescript/examples run prism:voice-tools
```

The SDK runs in Node.js. The browser supplies microphone audio and playback;
it never receives your API key. Use headphones and grant microphone permission.
The browser needs AudioWorklet and
[`AudioContext.getOutputTimestamp`](https://developer.mozilla.org/en-US/docs/Web/API/AudioContext/getOutputTimestamp)
to report the output device's playback position. Select
audio devices in your browser or OS settings. Set `PORT` to change port 3000.
The server accepts one local connection and is a demo, not a deployment template.

## Inputs, outputs, and behavior

The included [speech fixture](../fixtures/audio/README.md) says "The capital of France is Paris."
Use `--input path/to/recording.wav --output answer.wav` to supply your own 16 kHz
mono PCM16 WAV file. With npm, put these arguments after `--`.
The recorded example sends audio at capture speed and continues sending silence
until the response ends. It has a 60-second deadline.

The Python commands above save files under `python/examples/`. npm scripts save
files under `typescript/examples/`. A failed run can leave a partial WAV file; only a
successful exit confirms completion. File examples do not claim that saved
audio was played.

Voice examples share their capture and playback code with the tool examples.
They keep one event consumer active while sending audio and running tools.
Caller interruption stops queued speech and reports the played position.
Generation completion does not mean playback has finished. A new user turn can
supersede a tool answer; the example records the result without speaking over it.
Tool data is local and performs no external operation.

The TTS text generator is a small stand-in for an LLM stream. Replace it with
your own async text source; no second provider or key is needed to run the demo.

## Optional protocol examples

The existing [Python typed turn](../python/examples/realtime/typed_turn.py) and
[Node.js typed turn](../typescript/examples/realtime/typed-turn.mjs) save raw PCM from typed
input. The [Python tools](../python/examples/realtime/tools.py) and [Node.js tools](../typescript/examples/realtime/tools.mjs)
show one tool round and discard audio. These are connection and protocol checks.
The original TTS `stream.py` / `stream.mjs` examples also save raw PCM.
Raw output is mono 24 kHz signed little-endian PCM16, without a WAV header.

## Manual checks

Run these on each supported OS before publishing the device examples:

1. Open both saved WAV files and confirm they contain intelligible speech.
2. Start TTS playback; stop it mid-sentence. Start again and let it finish.
3. Start a Prism conversation with headphones. Ask two questions in sequence.
4. Speak during a reply. Confirm old speech stops and the new question gets an answer.
5. Ask for order `demo-123`, then interrupt its answer and ask a different question.
6. Stop while capturing or playing. Confirm the microphone indicator clears and a new session works.

The server/model is required for these checks. Automated tests use controlled
peers and simulated output clocks; they do not prove physical device behavior.

## Go TTS

Run `go -C go run ./examples/save` with `RIME_API_KEY` set. See
[the Go guide](../go/README.md) for output options.

## Rust TTS

Run `cargo run --manifest-path rust/Cargo.toml --example save` from the repository
root with `RIME_API_KEY` set. It saves raw PCM16 mono 24 kHz audio to `speech.pcm`.

## Rust STT

Run `cargo run --manifest-path rust/Cargo.toml --example transcribe -- utterance.pcm en`
with `RIME_API_KEY` set. Input must be headerless PCM16 mono 16 kHz. The example
prints replacement partials, the final text, and the request ID. See
[the Rust STT guide](../rust/README.md#transcribe-speech) for other input formats.

## Speech recognition

Prepare a headerless signed PCM16 little-endian mono 16 kHz file and choose its
spoken language explicitly. After local setup, run:

```sh
uv run --project python python/examples/stt/stream.py utterance.pcm --language en
npm --prefix typescript/examples run stt -- /absolute/path/utterance.pcm --language es
```

Use `--mode verbatim` to request spoken formatting. File EOF ends the utterance;
errors fail the command without printing a successful final result. Partial
lines are complete replacement transcripts, not fragments to concatenate.

## Go STT

The Go module includes [`examples/transcribe`](../go/examples/transcribe/main.go).
From `go/`, run `go run ./examples/transcribe -language en -sample-rate 24000 audio.pcm`
to transcribe a headerless PCM16 little-endian mono file. It prints replacement
partials, the final transcript and request ID. See the [Go STT documentation](../go/README.md#transcribe-speech).

## Use examples outside the repository

Copy the complete Python or TypeScript `examples/` directory to a new location.
Its dependency manifest contains a normal SDK version requirement. Without the
repository workspace, `uv sync` or `npm install` installs a published SDK.
See the [Python setup](../python/examples/README.md) and
[TypeScript setup](../typescript/examples/README.md). A new example can require
an SDK version that has not been published yet. Use the checkout until that
release is available.

For recorded Prism examples outside this repository, supply `--input` with your
own 16 kHz mono PCM16 WAV file. You can also copy `fixtures/audio/france.wav`.
The default recording path points to the repository's shared fixture.

For Go, copy an example's `main.go` into a new directory. Run `go mod init
example.com/rime-example`, then `go get github.com/rimelabs/rime-sdk/go@latest`
and `go run .`. For Rust, create a binary project with `cargo new`, copy
`rust/examples/save.rs` to its `src/main.rs`, and add `rimelabs-sdk`,
`futures-util`, and `tokio` dependencies. Enable Tokio's `macros`, `rt-multi-thread`,
`fs`, and `io-util` features. Inside this repository, Go and Cargo use their
local module or crate automatically.

CI checks examples against local SDK code. After publication, a separate workflow
copies Python and TypeScript examples outside the workspace and checks the exact
published version. Go release checks also verify the published module. Rust
checks its crate package before publication.
