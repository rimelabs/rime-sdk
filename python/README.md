# Rime SDK for Python

Use TTS to turn your text into speech, or Prism to run a conversation with text,
audio, and tool calls. Both use the same `Rime` client and credentials.

## Install

Requires Python 3.11 or later. This package is in alpha.

```sh
uv add --prerelease=allow rimelabs-sdk
export RIME_API_KEY="your-api-key"
```

An explicit `Rime(api_key="...")` overrides the environment. The SDK does not
load `.env` files.

## TTS quick start

Save playable speech with the
[WAV example](https://github.com/rimelabs/rime-sdk/blob/main/examples/python/tts/save.py).
After the [example setup](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md#python), run from the repository root:

```sh
uv run --project examples/python -m examples.python.tts.save
```

Open `speech.wav` in your audio player. To hear streamed text as it arrives:

```sh
uv run --project examples/python --extra audio -m examples.python.tts.play
```

Press Ctrl-C to stop generation and playback. Replace the example's async text
source with your LLM's text stream. The SDK handles sentence boundaries.
Coda is the default; select Mist v3 with `Rime(model="mistv3")`.
The [TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/tts.md)
covers voices, formats, and cancellation.

## Prism quick start

Start a voice conversation: speak into your microphone, hear the reply, and speak
again to interrupt it. Use headphones. Your application owns capture and playback.

Set `PRISM_URL` to your deployment's full WebSocket URL ending in `/v1/realtime`.
Set `PRISM_VOICE` to a voice from that deployment unless it has a default.
Obtain these values from your deployment operator; the TTS endpoint does not serve Prism.

After the [example setup](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md#python), run from the repository root:

```sh
uv run --project examples/python --extra audio -m examples.python.realtime.voice
```

Without a microphone, send the included speech recording and save `reply.wav`:

```sh
uv run --project examples/python -m examples.python.realtime.recorded
```

The [voice tool example](https://github.com/rimelabs/rime-sdk/blob/main/examples/python/realtime/voice_tools.py)
adds a demo order lookup to the same conversation. The
[Realtime guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/realtime.md)
covers audio, tools, playback reports, errors, and optional typed input.
TTS client settings do not configure Prism sessions.

## Examples and license

Run the [examples](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md)
from a source checkout. They cover WAV output, streamed playback, recorded speech, and voice conversations
with interruption and tools.

The SDK uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/python/LICENSE).
The Prism protocol module uses [Apache 2.0](https://github.com/rimelabs/rime-sdk/blob/main/python/LICENSE-PRISM).
