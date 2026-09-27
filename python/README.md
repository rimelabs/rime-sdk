# Rime SDK for Python

Requires Python 3.11 or later. This is an alpha release.
Install it from PyPI with `uv`:

```sh
uv add "rimelabs-sdk==0.1.0a1"
```

```python
import asyncio
from rimelabs_sdk import Rime, AudioFormat

async def main():
    async with Rime(api_key="your-api-key") as client:
        async with client.tts.stream(
            "Hello. This is Rime.",
            voice="clementine",
            audio_format=AudioFormat.PCM_24000,
        ) as audio:
            with open("speech.pcm", "wb") as output:
                async for chunk in audio:
                    output.write(chunk)
            print(audio.request_id)

asyncio.run(main())
```

Omit `api_key` to read `RIME_API_KEY`. The default model is `coda`.
The SDK selects its standard endpoint. Only `coda` is currently supported.
For a custom deployment of that model, set `endpoint`:

```python
client = Rime(model="coda", endpoint="coda.api.customer-name.rime.ai")
```

Use a hostname with an optional port, such as `host:8443`. Omit the scheme and path.
Connections always use TLS; the default port is `443`.
The endpoint applies to speech, voices, and languages for this client.

The default voice is `clementine`; the default language is `en`.
`tts.stream()` also accepts `AsyncIterable[str]`. Do not await the factory.
It starts work when you enter its async context or request the first chunk.
Use the stream context to release resources when you exit a loop early.

`await client.voices.list(language="en")` returns voice names.
`await client.languages.list()` returns language codes.
Both calls accept a keyword `timeout` in seconds.

`Rime(timeout=30)` sets an overall timeout for operations. An operation can
replace it with `timeout=10` or disable it with `timeout=None`.
Omitting the operation option inherits the client setting. Internal connection
and progress limits still apply. An overall timeout continues during caller pauses.

`audio.format` is a read-only `AudioFormat` with `encoding`, `sample_rate`, and
`channels`. `PCM_24000` is raw signed 16-bit little-endian mono PCM at 24 kHz.
`MULAW_8000` is raw mono G.711 mu-law at 8 kHz. Neither includes a file header.
Chunks preserve complete sample frames. Chunk sizes are not fixed.

`await audio.cancel()` cancels one operation. `await client.close()` cancels all
client work. Both are idempotent. A client belongs to one process and event loop.
Normal iterator completion means that the final service status was successful.
Partial audio can precede a typed error. Synthesis is never replayed.

Catch `RimeError` or one of `RimeAuthenticationError`, `RimePermissionError`,
`RimeInputError`, `RimeResourceLimitError`, `RimeUnavailableError`,
`RimeTimeoutError`, `RimeAudioFormatError`, `RimeCancelledError`, or
`RimeStreamError`. Errors expose a message and optional `request_id`.
Cancellation of the caller's Python task keeps `asyncio.CancelledError`.

The SDK sends the API key as a bearer token over TLS.

## Development for contributors

These instructions require access to the source repository.
Run the commands from its `python/` directory:

```sh
uv sync --locked --dev
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src
uv run pytest
uv build
```

## License

The SDK is licensed under the [MIT License](LICENSE).
