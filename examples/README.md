# SDK examples

Start with a file you can run and change. These examples use public SDK imports
and local package builds. Run the commands below from the repository root.

| Example | What it does |
| --- | --- |
| [Python TTS](python/tts/stream.py) | Streams text fragments and saves `speech.pcm`. |
| [Node.js TTS](typescript/tts/stream.mjs) | Runs the same flow with an async text generator. |
| [Python Prism: typed turn](python/realtime/typed_turn.py) | Sends one user turn, prints text, and saves `reply.pcm`. |
| [Python Prism: tools](python/realtime/tools.py) | Handles a tool call with local fixture data, submits its result, and continues the reply. |

## Python

Use Python 3.11 or later and `uv`:

```sh
uv sync --project python --locked --dev
export RIME_API_KEY="your-api-key"
uv run --project python examples/python/tts/stream.py
```

Prism also needs a deployment endpoint. Supply a voice unless that deployment has
a valid default; TTS voices are not automatically available on Prism.

```sh
export PRISM_URL="wss://your-prism-host/v1/realtime"
export PRISM_VOICE="your-deployment-voice"
uv run --project python examples/python/realtime/typed_turn.py
uv run --project python examples/python/realtime/tools.py
```

Both Prism scripts run one conversation with a 60-second application deadline.
They keep one event consumer active while sending input and handling tools.
The tool result is example data, and tool use still depends on the model's reply.

## Node.js

With Node.js 22 or later, build the SDK and install the example's local package
link. Its `@rimelabs/sdk` import also works in an application using the published package.

```sh
npm --prefix typescript ci
npm --prefix typescript run build
npm --prefix examples/typescript ci
export RIME_API_KEY="your-api-key"
npm --prefix examples/typescript run tts
```

Realtime / Prism is currently Python-only.

The Node.js script writes `examples/typescript/speech.pcm`. Python scripts write
their output in the directory where you run them.

## Audio and cleanup

Output files contain raw mono 24 kHz signed little-endian PCM16, with no WAV header.
The scripts save or discard audio; they do not open a microphone or play sound.
Configure a player for that format before listening to the saved files.

Each script closes its client on success or failure. A failure can leave a partial
audio file, so check the exit status before treating the output as complete.
Prism playback receipts are omitted because these scripts have no player.

Read the API guides for [Python TTS](../python/docs/tts.md),
[Python Realtime](../python/docs/realtime.md), and
[Node.js TTS](../typescript/docs/tts.md) before adapting the examples for continuous use.
