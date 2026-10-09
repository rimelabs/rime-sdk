# SDK examples

Start with a WAV file, then try a live conversation. Most examples use the
published SDK packages; the terminal and cascaded agents use this checkout.
They require a Rime API key; Prism also requires a
deployment URL and voice. Run the commands from the repository root.

| Use case | Python | Node.js / TypeScript | Result |
| --- | --- | --- | --- |
| STT: transcribe audio | [stream.py](python/stt/stream.py) | [stream.mjs](typescript/stt/stream.mjs) | Replacement transcripts and a final result from a PCM file. |
| TTS: save speech | [save.py](python/tts/save.py) | [save.ts](typescript/tts/save.ts) | A playable `speech.wav` file. |
| TTS: stream and play | [play.py](python/tts/play.py) | [play.ts](typescript/tts/play.ts) | Hear incremental text; stop generation and playback. |
| Prism: recorded speech | [recorded.py](python/realtime/recorded.py) | [recorded.ts](typescript/realtime/recorded.ts) | Send the included recording; save `reply.wav`. No audio hardware required. |
| Prism: voice conversation | [voice.py](python/realtime/voice.py) | [voice.ts](typescript/realtime/voice.ts) | Speak, hear replies, interrupt, and continue talking. |
| Prism: voice with tools | [voice_tools.py](python/realtime/voice_tools.py) | [voice-tools.ts](typescript/realtime/voice-tools.ts) | Ask about order `demo-123`; hear the demo result. |

The live audio paths have automated device simulations and protocol tests.
Physical microphone and speaker validation is still required before release.
See [manual checks](#manual-checks).

## Terminal STT voice loop

For a conversation with an LLM, use the [cascaded voice agent guide](agent/README.md).
It includes Python, TypeScript, and Go versions using Rime STT/TTS and OpenAI,
plus manual checks for Mist v3 timestamps, Coda custom lexicons, and useful errors.

Use the [terminal voice guide](TERMINAL_VOICE.md) to speak, inspect partial/final
transcripts, and hear the final text through TTS in Python, JavaScript or Go.
It uses Enter to start/end a turn and requires only a Rime API key and SoX.
The guide includes the setup for this unreleased STT development checkout.

## Coda WebSocket streaming from the terminal

With `RIME_API_KEY` set and SoX installed (`brew install sox` on macOS), run:

```sh
uv run --project python python examples/python/tts/coda_ws.py
```

This connects directly to `wss://api.rime.ai/coda/ws`.

Enter complete sentences or stable clauses, one per line. Each line becomes a
`text` message in the same synthesis context, and audio plays as it arrives.
Enter `/end` to finish that turn. The WebSocket stays open for the next turn.
An unfinished input can pause synthesis mid-sentence until more text or `/end`
arrives, even when the last chunk ends in punctuation. Send `/end` as soon as
you have supplied all text for that turn. With an LLM, send it when the LLM's
text stream finishes, rather than waiting for audio to finish.

For listening to complete typed turns, add `--auto-end`:

```sh
uv run --project python python examples/python/tts/coda_ws.py --auto-end
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
uv run --project python python examples/python/tts/coda_ws.py \
  --text "Hello from Coda." \
  --text "This sentence arrives two seconds later." --chunk-delay 2
```

The tester prints each sent chunk, the request ID, and elapsed time from `start`
to the first received audio byte. This timing excludes speaker buffering.

To isolate synthesis input mode, use exactly the same text and voice:

```sh
# Complete input in start.text; output audio still streams.
uv run --project python python examples/python/tts/coda_ws.py \
  --complete-text --text "Your appointment is confirmed for tomorrow at ten."
# Streaming input: empty start, one text message, then immediate end.
uv run --project python python examples/python/tts/coda_ws.py \
  --text "Your appointment is confirmed for tomorrow at ten."
# Same streaming input with additional initial text context.
uv run --project python python examples/python/tts/coda_ws.py \
  --lookahead-tokens 8 --text "Your appointment is confirmed for tomorrow at ten."
```

`--complete-text` requires exactly one `--text` and sends no `text` or `end`
messages. `--auto-end` still uses streaming-input mode. `--lookahead-tokens`
defaults to zero and applies only to streaming input. More lookahead can trade
first-audio latency for additional text context. Compare saved WAVs for speech
quality and live playback for delivery gaps, and repeat samples because model
generation can vary between requests.

It uses the [Coda JSON WebSocket protocol](https://rimelabs-docs-coda-websocket-reference.mintlify.site/api-reference/coda/websockets)
and needs no LLM key. The existing cascaded agent waits for a complete LLM reply;
use this tester to exercise incremental text input independently.

## Python

Python 3.11+ and `uv` are required. The examples have their own environment;
you do not need to build the SDK or install its development dependencies.

```sh
uv sync --project examples/python --locked
export RIME_API_KEY="your-api-key"
uv run --project examples/python -m examples.python.tts.save
```

For microphone capture or playback, install the audio extra:

```sh
uv sync --project examples/python --locked --extra audio
uv run --project examples/python --extra audio -m examples.python.tts.play
```

The audio extra uses PortAudio through `sounddevice`. If PortAudio is missing,
follow the [sounddevice installation instructions](https://python-sounddevice.readthedocs.io/en/latest/installation.html).
Device support depends on your OS. Grant microphone access to your terminal.

Configure Prism with values from your deployment operator. The TTS endpoint and
voice catalog do not supply these values:

```sh
export PRISM_URL="wss://your-prism-host/v1/realtime"
export PRISM_VOICE="your-deployment-voice"
uv run --project examples/python -m examples.python.realtime.recorded
uv run --project examples/python --extra audio -m examples.python.realtime.voice
# After stopping that conversation:
uv run --project examples/python --extra audio -m examples.python.realtime.voice_tools
```

Use headphones. The Python example does not remove speaker echo. Keep the
microphone active during replies so Prism can hear interruptions. Ctrl-C closes
capture, playback, and the SDK connection. Use `--list-devices`, `--input-device N`,
or `--output-device N` with either voice script to choose devices.

## Node.js

Node.js 22+ is required. Install the example dependencies, including the published
SDK and the TypeScript runner:

```sh
npm --prefix examples/typescript ci
export RIME_API_KEY="your-api-key"
npm --prefix examples/typescript run tts:save
npm --prefix examples/typescript run tts:play
```

For playback, open `http://127.0.0.1:3000` in a browser and press **Start**.
**Stop** cancels the request and clears playback. Ctrl-C stops the local server.

Set `PRISM_URL` and `PRISM_VOICE` as above, then run one example at a time:

```sh
npm --prefix examples/typescript run prism:recorded
npm --prefix examples/typescript run prism:voice
# Or start the conversation with a demo order tool:
npm --prefix examples/typescript run prism:voice-tools
```

The SDK runs in Node.js. The browser supplies microphone audio and playback;
it never receives your API key. Use headphones and grant microphone permission.
The browser needs AudioWorklet and
[`AudioContext.getOutputTimestamp`](https://developer.mozilla.org/en-US/docs/Web/API/AudioContext/getOutputTimestamp)
to report the output device's playback position. Select
audio devices in your browser or OS settings. Set `PORT` to change port 3000.
The server accepts one local connection and is a demo, not a deployment template.

## Inputs, outputs, and behavior

The included [speech fixture](audio/README.md) says "The capital of France is Paris."
Use `--input path/to/recording.wav --output answer.wav` to supply your own 16 kHz
mono PCM16 WAV file. With npm, put these arguments after `--`.
The recorded example sends audio at capture speed and continues sending silence
until the response ends. It has a 60-second deadline.

Python saves files in your working directory. npm scripts save files under
`examples/typescript/`. A failed run can leave a partial WAV file; only a
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

The existing [Python typed turn](python/realtime/typed_turn.py) and
[Node.js typed turn](typescript/realtime/typed-turn.mjs) save raw PCM from typed
input. The [Python tools](python/realtime/tools.py) and [Node.js tools](typescript/realtime/tools.mjs)
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

## Speech recognition

Prepare a headerless signed PCM16 little-endian mono 16 kHz file and choose its
spoken language explicitly. After local setup, run:

```sh
uv run --project python examples/python/stt/stream.py utterance.pcm --language en
npm --prefix examples/typescript run stt -- /absolute/path/utterance.pcm --language es
```

Use `--mode verbatim` to request spoken formatting. File EOF ends the utterance;
errors fail the command without printing a successful final result. Partial
lines are complete replacement transcripts, not fragments to concatenate.

## Go STT

The Go module includes [`examples/transcribe`](../go/examples/transcribe/main.go).
From `go/`, run `go run ./examples/transcribe -language en -sample-rate 24000 audio.pcm`
to transcribe a headerless PCM16 little-endian mono file. It prints replacement
partials, the final transcript and request ID. See the [Go STT documentation](../go/README.md#transcribe-speech).
