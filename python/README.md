# Rime SDK for Python

Generate speech with TTS, transcribe audio with STT, or send speech to Prism
and receive a spoken reply. All use the `Rime` client. Your application handles
audio capture and playback.

## Install

Requires Python 3.11 or later and [uv](https://docs.astral.sh/uv/). This package is in alpha.
Start in a new directory, or skip `uv init` if you already have a Python project:

```sh
uv init rime-quickstart
cd rime-quickstart
uv add --prerelease=allow rimelabs-sdk
export RIME_API_KEY="your-api-key"
```

Get a TTS API key from the [Rime dashboard](https://app.rime.ai/).
An explicit `Rime(api_key="...")` overrides `RIME_API_KEY`. The SDK does not load `.env` files.
The commands below use a macOS or Linux shell.

## TTS: save speech

Save this as `tts.py`. It writes a playable `speech.wav` file with no audio-device dependencies.

```python
import asyncio
import wave

from rimelabs_sdk import Rime


async def main():
    async with (
        Rime(timeout=60) as client,
        client.tts.synthesize("Your appointment is confirmed for tomorrow.") as audio,
    ):
        with wave.open("speech.wav", "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(24000)
            async for chunk in audio:
                wav.writeframesraw(chunk)
    print("Saved speech.wav")


asyncio.run(main())
```

Run `uv run tts.py`, then open `speech.wav` in your audio player.
A failed run can leave a partial file. Only a successful exit confirms completion.

Coda is the default model. Use `Rime(model="mistv3")` for Mist v3, or pass
`voice="your-voice"` to `client.tts.synthesize(...)` to select a voice for that model.
The default output is mono 24 kHz PCM16. The example adds its WAV header.
Use `client.tts.synthesize(text)` for a complete string. To supply text from an LLM,
pass an async iterable of strings to `client.tts.stream(chunks)`. Both methods
return streaming audio. The SDK handles sentence boundaries.

## STT quick start

Stream one utterance of headerless signed PCM16 little-endian audio. Specify the
spoken language; exhaust the audio source when the utterance ends. Each update
replaces the complete current transcript.

```python
from rimelabs_sdk import Rime


async def recognize(audio_chunks):
    async with Rime() as client:
        async with client.stt.stream(audio_chunks, language="en") as transcript:
            async for update in transcript:
                print(update.kind, update.text)
```

Input defaults to 16 kHz mono. The [STT guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/stt.md)
covers other PCM formats, written/verbatim output, recognition hints and cancellation.

## Prism: send recorded speech

Prism needs a deployment that serves its realtime API. Obtain the endpoint,
credentials, and voice from your deployment operator. The TTS endpoint does not serve Prism.
Set `RIME_API_KEY` to the credential for that deployment if it differs from your TTS key.

```sh
export PRISM_URL="wss://your-prism-host/v1/realtime"
export PRISM_VOICE="your-deployment-voice"
```

You can omit `PRISM_VOICE` if the deployment has a default voice.
Record a short question and export it as `question.wav`, using **16 kHz, mono, signed 16-bit PCM WAV**.
Save the following as `prism.py` beside that file. It sends the recording at capture speed,
then sends silence so Prism can detect the end of speech. The spoken reply goes to `reply.wav`.

```python
import asyncio
import os
import wave

from rimelabs_sdk import Rime
from rimelabs_sdk import realtime as r


async def main():
    with wave.open("question.wav", "rb") as wav:
        if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, 16000):
            raise ValueError("question.wav must be 16 kHz mono PCM16")
        data = wav.readframes(wav.getnframes())
    if not data or len(data) % 2:
        raise ValueError("question.wav must contain complete PCM16 samples")

    async with (
        asyncio.timeout(60),
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.getenv("PRISM_VOICE"),
            instructions="Give short, clear answers.",
        ) as session,
    ):

        async def send():
            offset = 0
            while True:
                frame = data[offset : offset + 1280].ljust(1280, b"\0")
                await session.send_audio(r.AudioChunk(data=frame))
                offset += 1280
                await asyncio.sleep(0.04)

        async with asyncio.TaskGroup() as tasks:
            sending = tasks.create_task(send())
            with wave.open("reply.wav", "wb") as wav:
                wav.setnchannels(1)
                wav.setsampwidth(2)
                wav.setframerate(24000)
                async for event in session.events:
                    payload = event.payload
                    if isinstance(payload, r.AudioDelta):
                        wav.writeframesraw(payload.audio.data)
                    elif isinstance(payload, r.FaultEvent):
                        raise r.RimeRealtimeError(payload.error)
                    elif isinstance(payload, r.ResponseEnded):
                        if payload.status != "completed":
                            raise RuntimeError(f"Response {payload.status}: {payload.reason}")
                        sending.cancel()
                        break
                else:
                    raise RuntimeError("Session closed before the reply ended")
    print("Saved reply.wav")


asyncio.run(main())
```

Run `uv run prism.py`, then open `reply.wav`. This example has a 60-second deadline.
It saves audio without playing it, so it sends no playback receipt.
For live conversations, your application must capture microphone audio, play replies,
stop playback on interruption, and report the actual played position to Prism.
TTS settings do not configure Prism sessions.

## Help and licenses

See [Rime documentation](https://docs.rime.ai/) for API keys, TTS models, and deployment information.
The examples above run with the installed package. No source checkout is required.

SDK code uses the MIT license. The Prism protocol module uses Apache 2.0.
License texts and notices are included in the distribution.
