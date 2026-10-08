# SDK examples

Start with a WAV file, then try a live conversation. These examples use the
published SDK packages. They require a Rime API key; Prism also requires a
deployment URL and voice. Run the commands from the repository root.

| Use case | Python | Node.js / TypeScript | Result |
| --- | --- | --- | --- |
| TTS: save speech | [save.py](python/tts/save.py) | [save.ts](typescript/tts/save.ts) | A playable `speech.wav` file. |
| TTS: stream and play | [play.py](python/tts/play.py) | [play.ts](typescript/tts/play.ts) | Hear incremental text; stop generation and playback. |
| Prism: recorded speech | [recorded.py](python/realtime/recorded.py) | [recorded.ts](typescript/realtime/recorded.ts) | Send the included recording; save `reply.wav`. No audio hardware required. |
| Prism: voice conversation | [voice.py](python/realtime/voice.py) | [voice.ts](typescript/realtime/voice.ts) | Speak, hear replies, interrupt, and continue talking. |
| Prism: voice with tools | [voice_tools.py](python/realtime/voice_tools.py) | [voice-tools.ts](typescript/realtime/voice-tools.ts) | Ask about order `demo-123`; hear the demo result. |

The live audio paths have automated device simulations and protocol tests.
Physical microphone and speaker validation is still required before release.
See [manual checks](#manual-checks).

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
