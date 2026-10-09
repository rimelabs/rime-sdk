# TTS for Python

You supply text; the SDK handles sentence boundaries and streams audio back.
Your application owns playback. Start with the complete WAV example in the
[package quick start](../README.md#tts-save-speech).

## Stream incoming text

An async text source can supply text as it arrives:

```python
async def text():
    yield "Hello. "
    yield "This text arrives in separate chunks."
```

Use `client.tts.stream(text())` in the same audio loop. The SDK handles sentence
boundaries. Do not await `tts.stream()`; entering its async context or reading
the first chunk starts work.

## Configuration

Select Mist v3 with `Rime(model="mistv3")`. Mist v1 and v2 are not supported.

| Model | Standard endpoint | Default voice |
| --- | --- | --- |
| `coda` | `coda.api.rime.ai:443` | `clementine` |
| `mistv3` | `mist.api.rime.ai:443` | `astra` |

### Client options

| `Rime(...)` option | Default | Meaning |
| --- | --- | --- |
| `api_key` | `RIME_API_KEY` | API key; an explicit value overrides the environment |
| `model` | `"coda"` | Model for speech and discovery |
| `endpoint` | Model's standard endpoint | Custom hostname and optional port for speech and discovery |
| `timeout` | `None` | Positive seconds for an overall deadline; `None` disables it |

Authentication uses bearer tokens over TLS. The SDK does not load `.env` files.

Custom deployments use `Rime(model="coda", endpoint="host:8443")`. Omit the scheme
and path. TLS is required; the default port is `443`. Model defaults still apply.

### Synthesis options

The `text` argument is required and accepts a nonblank string or an async text
source. All options in `client.tts.stream(text, ...)` are optional:

| Option | Default | Meaning |
| --- | --- | --- |
| `voice` | Model's default voice | Voice name from `client.voices.list()` |
| `language` | `"en"` | Language code |
| `audio_format` | `AudioFormat.PCM_24000` | Output profile, imported from `rimelabs_sdk` |
| `timestamps` | `False` | Request final word timestamps; requires `model="mistv3"` |
| `custom_lexicon` | `()` | Sequence of `PronunciationEntry` overrides for this request; requires Coda v2 |
| `timeout` | Client setting | Seconds; pass `None` to disable the overall deadline |

### Audio formats

Both profiles return raw audio without a file header.

| `AudioFormat` profile | Encoding | Sample rate | Channels |
| --- | --- | --- | --- |
| `PCM_24000` | Signed 16-bit little-endian PCM | 24 kHz | Mono |
| `MULAW_8000` | G.711 mu-law | 8 kHz | Mono |

Chunks contain complete sample frames, but their sizes vary. Inspect the read-only
`audio.format.encoding`, `audio.format.sample_rate`, and `audio.format.channels`.

### Timeouts

An overall deadline includes pauses between reads. Disabling it leaves internal
connection and progress limits active. Discovery allows at most 10 seconds;
a shorter supplied timeout takes precedence.

## Custom pronunciations (Coda v2)

Pass words or phrases and their space-separated X-SAMPA pronunciations:

```python
import asyncio
from pathlib import Path

from rimelabs_sdk import PronunciationEntry, Rime, RimeInputError


async def main():
    lexicon = [PronunciationEntry(spelling="hello", pronunciation='h @ . " l oU')]
    async with Rime(model="coda", timeout=60) as client:
        try:
            async with client.tts.stream(
                "Hello world.", language="en", custom_lexicon=lexicon
            ) as audio:
                with Path("speech.pcm").open("wb") as output:
                    async for chunk in audio:
                        output.write(chunk)
        except RimeInputError as error:
            print(f"Invalid synthesis request: {error}; request_id={error.request_id}")
            raise


asyncio.run(main())
```

`PronunciationEntry` is immutable and exported from `rimelabs_sdk`. The SDK
snapshots the sequence when `tts.stream()` is called. The same lexicon applies
to the entire request, including an async text source; subsequent requests have
their own options. Omit the option or pass an empty sequence for default pronunciation.

The service matches whole words or phrases case-insensitively and applies the
pronunciation after text normalization. Longest matches take precedence; the
last entry for a repeated spelling wins. Inline `pronounce(...)` directives are
not supported.

Coda v2 accepts custom pronunciations in German, English, Spanish, French,
Italian, and Portuguese, with up to 500 entries per request. The selected
deployment validates these capabilities and the pronunciation rules. Older
Coda deployments and Mist reject nonempty lexicons. `model="coda"` selects the
Coda endpoint; it does not pin a model version.

Malformed option types fail locally. The service rejects unsupported models or
languages, oversized lexicons, and ill-formed entries with `RimeInputError`
before producing audio. Catch it around entering and consuming the stream.
Its message preserves the offending spelling and checks such as
`no-primary-stress` or `unknown-phone`, and `request_id` identifies the request.
The whole request fails; entries are never silently skipped or repaired.

## Word timestamps (Mist v3)

Enable timestamps on a synthesis, consume all audio, then read its result:

```python
import asyncio
from pathlib import Path

from rimelabs_sdk import Rime


async def main():
    async with Rime(model="mistv3", timeout=60) as client:
        async with client.tts.stream("Hello world.", timestamps=True) as audio:
            with Path("speech.pcm").open("wb") as output:
                async for chunk in audio:
                    output.write(chunk)
            result = await audio.timestamps()
            if result.status.code == 0:
                for word in result.spans:
                    print(word.text, word.start, word.end)
            else:
                print("Timestamps unavailable:", result.status.code, result.status.message)


asyncio.run(main())
```

`TimestampResult`, `TimestampStatus`, and `WordTimestamp` are exported from
`rimelabs_sdk`. The result is immutable. `spans` is a tuple of words with `text`,
`start`, and `end`; times are seconds from the beginning of the entire synthesis,
including when text arrives incrementally. Resampling to `MULAW_8000` does not
change these times. Words reflect normalized spoken text, so a number may become
several words. The model supplies the timings; the SDK does not estimate them.

Timestamps arrive once, after generation finishes. They are not incremental
events for live interruption handling. `timestamps()` never consumes audio or
waits for you to consume it: calling before successful iteration finishes, or
without enabling timestamps, raises `RimeInputError`. Results remain accessible
after a successfully consumed stream or its client is closed.

`status.code` is a numeric `google.rpc.Code`: `0` is success. Nonzero codes such as
`12` (unimplemented) or `4` (deadline exceeded) carry an explanation and no spans;
they do not fail successful audio. Missing or malformed timestamp responses raise
`RimeStreamError` only when reading timestamps. A synthesis error or cancellation
also prevents reading a successful timestamp result. Enabling timestamps with
Coda raises `RimeInputError` before any request is sent.

## Discover voices and languages

List voices and languages inside `main()`:

```python
async with Rime(model="mistv3") as client:
    voices = await client.voices.list(language="en")
    languages = await client.languages.list()
    print(voices, languages)
```

Both return string lists and accept `timeout`. Omit `language` to disable filtering.

## Errors and cancellation

Catch `RimeError` around your application entry point to report the request ID:

```python
from rimelabs_sdk import RimeError

try:
    asyncio.run(main())
except RimeError as error:
    print(f"Speech failed: {error}; request_id={error.request_id}")
    raise
```

Each SDK error inherits from `RimeError`:

| Error | Meaning |
| --- | --- |
| `RimeAuthenticationError` | Missing or rejected credentials |
| `RimePermissionError` | Access denied |
| `RimeInputError` | Invalid input or client use |
| `RimeResourceLimitError` | Service resource limit reached |
| `RimeUnavailableError` | Service unavailable |
| `RimeTimeoutError` | Overall or internal deadline reached |
| `RimeAudioFormatError` | Unsupported or unexpected audio format |
| `RimeCancelledError` | Operation cancelled |
| `RimeStreamError` | Other stream or transport failure |

Service errors retain their diagnostic message and request ID when provided.
Use the exception type for programmatic handling; message wording may change.

Partial audio can arrive before an error. Output is complete only after successful
iteration. Synthesis is not retried automatically.

Keep the stream's `async with` block to cancel unfinished work on exit.
`await audio.cancel()` stops one operation; `await client.close()` stops all client
work. Both are safe to repeat. A client belongs to one process and event loop.
Python task cancellation remains `asyncio.CancelledError`.

## Complete text input

`complete_text=True` selects the `Synthesize` RPC, which sends the full input in one request
and streams audio back. The default still uses `SynthesizeStreaming` with
incremental sentence input, including when given a string. Full-text mode accepts
only a string of at most 65,536 UTF-8 bytes; collect an incremental source in the
application first. Server-side text splitting
applies. Lexicon overrides, error details and request IDs, cancellation, audio
profiles, and Mist v3 timestamps use the same SDK behavior in both modes.

```python
async with client.tts.stream(
    "Please read this.", complete_text=True,
    custom_lexicon=[PronunciationEntry("read", '" r\\ E d')],
) as audio:
    async for chunk in audio:
        ...  # Play or save each PCM chunk.
```
