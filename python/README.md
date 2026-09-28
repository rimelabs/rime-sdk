# Rime SDK for Python

Stream speech from a string or async text source with Rime.

## Install

Requires Python 3.11 or later. This package is in alpha.

```sh
uv add --prerelease=allow rimelabs-sdk
export RIME_API_KEY="your-api-key"
```

## Quick start

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

Output is raw mono 24 kHz signed 16-bit little-endian PCM. Configure your player
for this format; there is no WAV header.

## Stream incoming text

Add this async text source before `main()` in the quick start:

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

Pass these options to `client.tts.stream(text, ...)`:

| Option | Default | Meaning |
| --- | --- | --- |
| `voice` | Model's default voice | Voice name from `client.voices.list()` |
| `language` | `"en"` | Language code |
| `audio_format` | `AudioFormat.PCM_24000` | Output profile, imported from `rimelabs_sdk` |
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

Replace the quick start's final `asyncio.run(main())` to report SDK errors and request IDs:

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

Partial audio can arrive before an error. Output is complete only after successful
iteration. Synthesis is not retried automatically.

Keep the stream's `async with` block to cancel unfinished work on exit.
`await audio.cancel()` stops one operation; `await client.close()` stops all client
work. Both are safe to repeat. A client belongs to one process and event loop.
Python task cancellation remains `asyncio.CancelledError`.

## License

The SDK uses the [MIT License](LICENSE).
