# Rime SDK for Python

Generate speech with Coda or Mist v3, transcribe audio with STT, and build spoken
conversations with Prism through the async `Rime` client. Your application handles
audio capture and playback.

## Installation

Requires **Python 3.11 or later**. Install the alpha package from PyPI with
[uv](https://docs.astral.sh/uv/):

```sh
uv init rime-quickstart
cd rime-quickstart
uv add --prerelease=allow rimelabs-sdk
export RIME_API_KEY="your-api-key"
```

For an existing project, skip the first two commands. Get an API key from the
[Rime dashboard](https://app.rime.ai/). A key is required: `Rime()` reads
`RIME_API_KEY`, or you can pass `Rime(api_key="...")` to override it.
The SDK does not load `.env` files. Shell commands below assume macOS or Linux.

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

## TTS

Use `client.tts.synthesize(text, **options)` for a complete string, or
`client.tts.stream(text_chunks, **options)` for an async iterable of strings.
Both return an `AudioStream` immediately; consume it with `async for`, without
awaiting the method call. For complete audio in memory, await
`client.tts.synthesize_to_buffer(text, **options)` to receive `bytes`.
Text is required and must contain non-whitespace characters.

### Client options

These optional `Rime(...)` arguments configure TTS. They do not configure STT or Prism.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `model` | Select `"coda"` or `"mistv3"`. | No | `"coda"` |
| `endpoint` | Override the TTS TLS hostname and optional port, such as `"coda.api.rime.ai:443"`; no scheme or path. | No | `coda.api.rime.ai:443` for Coda; `mist.api.rime.ai:443` for Mist v3. |
| `timeout` | Overall TTS timeout as positive numeric seconds or `datetime.timedelta`; `None` disables it. | No | `None` |

### Synthesis options

All three TTS methods accept the same keyword arguments. Import `AudioFormat`
and `PronunciationEntry` from `rimelabs_sdk` when using those options.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `voice` | Voice name for the selected model, such as `"clementine"` or `"astra"`. | No | `"clementine"` for Coda; `"astra"` for Mist v3. |
| `language` | Spoken language code, such as `"en"` or `"es"`; the service validates support. | No | `"en"` |
| `audio_format` | Output encoding: `AudioFormat.PCM`, `.WAV`, `.MP3`, `.PCM_24000`, or `.MULAW_8000`. | No | Raw PCM16 at 24 kHz (`PCM_24000`). |
| `sample_rate` | Requested output rate in Hz: `8000`, `16000`, or `24000`. | No | `24000`; `8000` for `MULAW_8000`. |
| `time_scale_factor` | Duration multiplier from `0.4` to `2.5`; below `1` speeds speech up, above `1` slows it down. | No | Omitted; normal duration (`1`). |
| `timestamps` | `True` requests word timings; supported only with Mist v3. | No | `False` |
| `custom_lexicon` | Sequence of `PronunciationEntry(spelling, pronunciation)` values for Coda; pronunciations use space-separated X-SAMPA. | No | Empty sequence. |
| `timeout` | Override the overall timeout with positive numeric seconds or `datetime.timedelta`, or `None` to disable it. | No | Inherits the client timeout. |

Discover valid values with `await client.voices.list(language="en")` and
`await client.languages.list()`. Omit the voice language filter to list all voices
for the selected model. Both calls accept a `timeout` as positive numeric seconds
or `datetime.timedelta`; discovery has an additional ten-second maximum.

### Example: save an IVR greeting

Save as `tts.py`. This writes a complete 16 kHz WAV file, ready for playback or upload.

```python
import asyncio
from pathlib import Path

from rimelabs_sdk import AudioFormat, Rime


async def main():
    async with Rime(timeout=60) as client:
        audio = await client.tts.synthesize_to_buffer(
            "Thank you for calling. Your appointment is confirmed for tomorrow.",
            audio_format=AudioFormat.WAV,
            sample_rate=16000,
            time_scale_factor=0.9,
        )
    await asyncio.to_thread(Path("greeting.wav").write_bytes, audio)
    print("Saved greeting.wav")


asyncio.run(main())
```

Run `uv run tts.py`, then open `greeting.wav` in an audio player.

### Audio and stream behavior

All output is mono. PCM is signed 16-bit little-endian audio without a header;
WAV contains PCM16; MP3 is encoded by the service. PCM/WAV rates are requested
from the server. MP3 headers determine the actual encoded rate. The fixed
`PCM_24000` and `MULAW_8000` presets reject conflicting sample rates; mu-law is
converted locally from 24 kHz PCM. Read `audio.sample_rate` on an `AudioStream`
for the requested rate.

The buffer helper waits for successful completion and finalizes WAV lengths.
Its fixed limit is **64 MiB per call**, including headers; exceeding it raises
`RimeResourceLimitError` without returning partial audio. WAV/MP3 stream chunks
are fragments of one file. Use the buffer helper when you need a complete WAV.

For incremental text, send fragments as they arrive; the service combines them
into sentences. Consume streaming audio to the end to confirm success, since an
error can follow earlier audio. Use `async with` for clients and streams, or
`await audio.cancel()` when stopping early. A client belongs to one process and
one event loop. Synthesis is not retried automatically, and overall timeouts
include pauses by the caller; connection and progress limits also apply.

For timestamps, enable `timestamps=True`, consume the audio stream fully, then
call `await audio.timestamps()`. The result has `status` and `spans` with word
text and start/end seconds. Nonzero alignment status does not fail the audio.
The buffer helper returns only bytes, so use an audio stream to access timings.
Custom lexicons apply to the whole request; the service validates pronunciations
and model/language support.

## STT

### Transcribe a complete WAV recording

For a complete recording, use the native unary `Transcribe` RPC through the SDK.
Pass **complete WAV bytes, including the header**, up to **16 MiB (16,777,216
bytes)**. PCM16 mono 16 kHz WAV is the portable format. The SDK sends the file
unchanged; it does not decode, resample, or add a header. The service validates
its encoding and the supplied language and recognition hints.

The result is a `Transcription` containing final text, resolved language, and a
request ID when available. Silence may return empty text. There are no partial
updates or automatic retries. TTS client settings do not apply. An optional
overall timeout includes connection and upload; the SDK also bounds connection
time and the unary request (120 seconds). Timeout, cancellation, and service
errors use the existing SDK error categories. A request ID is included when the
transport has made it available.

```python
from pathlib import Path
from rimelabs_sdk import Rime, TranscriptionMode

async with Rime() as client:
    result = await client.stt.transcribe(
        Path("utterance.wav").read_bytes(),
        language="en",
        mode=TranscriptionMode.WRITTEN,
        context_terms=["Super-G", "USOC"],
        timeout=120,
    )
    print(result.text, result.language, result.request_id)
```

Run this inside an async function. For large files, use
`await asyncio.to_thread(Path("utterance.wav").read_bytes)` to keep file reads off
the event loop. Cancelling the calling task preserves `asyncio.CancelledError`;
closing the client cancels pending requests with `RimeCancelledError`.

### Stream an utterance

Use `client.stt.stream(audio_chunks, **options)` to transcribe one utterance.
The required source is an async iterable of headerless signed PCM16 little-endian
`bytes`. Ending the source ends the utterance; your application decides when to stop.

STT uses gRPC over TLS to `stt.api.rime.ai:443`, with the client's API
key. There is no STT model, endpoint, or protocol selector. TTS client settings,
including its timeout, do not apply to STT.

### Transcription options

Import `PCMFormat` and `TranscriptionMode` from `rimelabs_sdk` when using those options.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `language` | Spoken BCP-47 language tag, such as `"en"` or `"es"`; passed to the service. | Yes | None; no automatic detection. |
| `mode` | `TranscriptionMode.WRITTEN` normalizes written text; `.VERBATIM` prefers spoken wording. | No | `WRITTEN` |
| `context_terms` | Sequence of recognition hints, such as `["Rime"]`; hints do not force a match. | No | Empty sequence. |
| `input_format` | `PCMFormat(sample_rate=16000, channels=1)`: rates `8000`, `16000`, `24000`, `48000`; channels `1` or `2`; encoding only `"pcm_s16le"`. | No | 16 kHz mono PCM16. |
| `timeout` | Overall timeout as positive numeric seconds or `datetime.timedelta`, including source/consumer pauses; `None` disables it. | No | `None`; independent of the client timeout. |

### Example: transcribe a recording

Save a short recording as `utterance.pcm`: **16 kHz, mono, signed PCM16
little-endian, with no WAV header**. Save this as `stt.py`; it reads the recording
into memory and sends it in chunks.

```python
import asyncio
from pathlib import Path

from rimelabs_sdk import Rime


async def audio_chunks():
    data = await asyncio.to_thread(Path("utterance.pcm").read_bytes)
    for offset in range(0, len(data), 3200):
        yield data[offset : offset + 3200]


async def main():
    async with (
        Rime() as client,
        client.stt.stream(
            audio_chunks(), language="en", timeout=120
        ) as transcript,
    ):
        async for update in transcript:
            print(update.kind, update.text)


asyncio.run(main())
```

Run `uv run stt.py`. Each partial update replaces the **complete current
transcript**; do not append snapshots. A final update confirms successful
completion and may be empty for silence. Errors do not turn partials into finals.

The SDK converts supported input formats to 16 kHz mono. Stereo must be
interleaved. WAV headers, MP3, and mu-law input are not accepted. Chunks may split
sample frames, but an incomplete final frame fails the request. The SDK does not
provide microphone capture, automatic end-of-speech detection, word timings, or
diarization. Recognition is not retried automatically.

Use `async with` for cleanup, or `await transcript.cancel()` to stop one utterance.
Closing the client cancels all its operations. Audio sources must yield without
blocking the event loop. Connection, acceptance, and completion limits still
apply when the overall timeout is disabled.

## Help and licenses

See the [TTS guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/tts.md),
[STT guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/stt.md), and
[Prism guide](https://github.com/rimelabs/rime-sdk/blob/main/python/docs/realtime.md)
for more examples. Visit [Rime documentation](https://docs.rime.ai/) for service
information, or report SDK issues on [GitHub](https://github.com/rimelabs/rime-sdk/issues).
Include the SDK version and the exception's `request_id` when available; omit API keys.

SDK code uses the [MIT license](LICENSE). The Prism protocol module uses
[Apache 2.0](LICENSE-PRISM); see its [notices](NOTICE-PRISM).
License texts and notices are included in the distribution.
