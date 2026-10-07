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

Save this as `speech.py` and run `uv run speech.py`:

```python
import asyncio
from rimelabs_sdk import Rime


async def main():
    async with Rime() as client:
        async with client.tts.stream("Hello. This is Rime.") as audio:
            with open("speech.pcm", "wb") as output:
                async for chunk in audio:
                    output.write(chunk)
            print(audio.request_id)


asyncio.run(main())
```

Output is raw mono 24 kHz signed little-endian PCM16, with no WAV header.
The default model is Coda. Select Mist v3 with `Rime(model="mistv3")`.

For streamed text, voices, audio formats, and cancellation, read the
[TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/tts.md).

## Prism quick start

Prism needs a full WebSocket endpoint that accepts the Rime API key as a Bearer
token. Set `PRISM_URL` to that endpoint. Set `PRISM_VOICE` if the deployment has
no default voice.

```python
import asyncio
import os
from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import FaultEvent, ResponseEnded, RimeRealtimeError, TextDelta


async def read_reply(session):
    async for event in session.events:
        payload = event.payload
        if isinstance(payload, TextDelta):
            print(payload.delta, end="", flush=True)
        elif isinstance(payload, FaultEvent):
            raise RimeRealtimeError(payload.error)
        elif isinstance(payload, ResponseEnded):
            if payload.status != "completed":
                raise RuntimeError(f"Response {payload.status}: {payload.reason}")
            print()
            return
    raise RuntimeError("Session closed before the response ended")


async def main():
    async with asyncio.timeout(60), Rime() as client:
        async with client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.getenv("PRISM_VOICE"),
        ) as session:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(read_reply(session))
                await session.send_text("Hello!")


asyncio.run(main())
```

This prints the text from one reply and discards its audio. Keep event consumption
running alongside input tasks; each session supports one event consumer.

Connection options, audio input, tools, playback, and errors are in the
[Realtime guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/realtime.md).
The client-level `model`, `endpoint`, and `timeout` options apply to TTS and
discovery. Configure Prism through `realtime.connect(...)`.

## Examples and license

Run the [examples](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md)
from a source checkout. They cover streamed TTS text, saved Prism audio, and a
tool round.

The SDK uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/python/LICENSE).
The Prism protocol module uses [Apache 2.0](https://github.com/rimelabs/rime-sdk/blob/main/python/LICENSE-PRISM).
