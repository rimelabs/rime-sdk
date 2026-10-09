# Streaming speech recognition

`client.stt.stream` transcribes one utterance from an async source of audio bytes.
The SDK handles the gRPC protocol, authentication, audio conversion, transcript
revisions, backpressure, and cancellation. Your application decides when speech
ends by exhausting the source.

```python
import asyncio
from pathlib import Path
from rimelabs_sdk import PCMFormat, Rime, TranscriptionMode


async def audio_chunks():
    with Path("utterance.pcm").open("rb") as audio:
        while data := await asyncio.to_thread(audio.read, 3200):
            yield data


async def transcribe():
    async with Rime() as client:
        async with client.stt.stream(
            audio_chunks(),
            language="en",
            input_format=PCMFormat(sample_rate=16000, channels=1),
            mode=TranscriptionMode.WRITTEN,
            context_terms=["Super-G", "USOC"],
            timeout=120,
        ) as transcript:
            async for update in transcript:
                if update.kind == "partial":
                    print("Current transcript:", update.text)
                else:
                    print("Final:", update.text, "Language:", update.language)
            print("Request:", transcript.request_id)
```

Run `asyncio.run(transcribe())` from a script. File reads run off the event loop
using `asyncio.to_thread`, as in the runnable
[example](../../examples/python/stt/stream.py).

## Options

| Option | Meaning |
| --- | --- |
| `language` | Required spoken BCP-47 tag. Passed unchanged to the service; no detection or SDK language allowlist. The deployed catalog currently supports English and Spanish. |
| `mode` | `TranscriptionMode.WRITTEN` (default) or `.VERBATIM`. Formatting intent, not a guarantee of exact tokens. |
| `context_terms` | Sequence of recognition hints, copied unchanged and in order. The service validates terms and prompt budgets. |
| `input_format` | `PCMFormat`: signed little-endian PCM16 at 8, 16, 24 or 48 kHz, mono or interleaved stereo. Defaults to 16 kHz mono. |
| `timeout` | Optional positive overall timeout in seconds. Includes connection time, source pauses and consumer pauses. `None` disables this SDK deadline, but not internal or server deadlines. |

Input contains raw samples, with no WAV header or compressed codec. Chunks may
split sample frames; the SDK carries incomplete bytes into the next chunk. A
truncated final frame raises `RimeAudioFormatError`, without committing the
utterance. Each gRPC audio payload is at most 64 KiB of 16 kHz mono PCM16.

The default recognition endpoint is `stt.api.rime.ai:50051`, using TLS and the same
API key as the other features. `Rime(stt_endpoint="host:443")` overrides only STT.
The existing `model`, `endpoint` and client `timeout` settings remain TTS settings.
There is no STT voice/model selector; the service selects its recognizer.

## Results and completion

`TranscriptionPartial` and `TranscriptionFinal` are frozen values, also available
from `rimelabs_sdk.stt`. Every update contains the complete current `text`.
**Replace the previous transcript. Do not concatenate partials.** Later partials
can change earlier words. `TranscriptionFinal.language` is the canonical language
selected by the service.

The source is not consumed until the server accepts the configuration. Audio
upload and transcript reading run concurrently. Decoding currently uses cumulative
two-second audio windows, so a partial is not promised for each audio packet.
Silence can finish successfully with empty final text and no partials.

Exhausting the source half-closes gRPC input. The SDK continues reading and emits
one final result only after a matching `done` and successful gRPC completion.
EOF, an error status, or cancellation cannot turn a partial into a final result.
No audio is automatically replayed after a connection failure.

## Lifetime and limits

Construction is lazy: entering the async context or starting iteration begins
work. Consume each stream with one reader. Context exit cancels unfinished work;
when iterating without a context, explicitly call `await transcript.cancel()` on
early exit. Cancelling a task waiting for an update also cancels its stream.
Client close cancels all its STT, TTS and realtime operations. Cancelling one
stream leaves its siblings running.

The SDK bounds the transcript queue to 16 updates, each with at most 64 KiB of
UTF-8 text, and bounds gRPC responses to 256 KiB. Slow consumers apply backpressure.
Internal connection and acceptance waits are 10 seconds; completion after source
exhaustion is bounded to 120 seconds. The completion timer stops after successful
gRPC completion. The optional overall timeout remains active until the final
result is consumed. Source iterators should honor task cancellation; source
cleanup is attempted for at most two seconds.

Server input-idle, utterance-size, request-duration and model-context limits still
apply. The current wire limit is 16 MiB per utterance; model context can impose a
smaller limit depending on audio and recognition terms. There is no SDK VAD,
server endpointing, word timing, diarization, or file-format detection.

Failures use the shared SDK error hierarchy: caller/input errors, cancellation,
authentication, permission, resource limits, timeouts, transient availability and
protocol failures stay distinct. `request_id` is retained on streams and failures
when the server supplies it. Keep it when reporting a problem.
