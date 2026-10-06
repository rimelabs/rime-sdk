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

## Prism speech-to-speech

Prism uses `client.realtime.connect`. It does not use the TTS model, endpoint,
voice defaults, or token-exchange policy. Supply the complete WebSocket URL for
an endpoint that accepts `Authorization: Bearer <RIME_API_KEY>`. Use `wss://` in
production. Plain `ws://` is limited to localhost.

```python
import asyncio
import os

from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import AudioDelta, ResponseEnded, TextDelta


async def main():
    async with Rime() as client:
        async with client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.environ["PRISM_VOICE"],
            instructions="Give short, clear answers.",
        ) as session:
            response = await session.send_text("Hello!")
            with open("reply.pcm", "wb") as output:
                async for event in session.events:
                    if isinstance(event.payload, TextDelta):
                        print(event.payload.delta, end="", flush=True)
                    elif isinstance(event.payload, AudioDelta):
                        output.write(event.payload.audio.data)
                    elif isinstance(event.payload, ResponseEnded):
                        if event.payload.response == response:
                            break


asyncio.run(main())
```

`reply.pcm` contains raw 24 kHz mono signed little-endian PCM16. This example
saves the response; it does not play it. The TypeScript SDK does not yet expose
Prism sessions.

For a continuous conversation, run one consumer of `session.events` alongside
the tasks that send input. `AudioChunk(data=..., format=PCMFormat(...))` accepts
PCM16 at 8, 16, 24, or 48 kHz with one or two channels. `send_audio` downmixes and
resamples to 16 kHz mono with state across chunks. A format change or
`clear_audio` resets that state. Send chunks of at most 192000 bytes. Each call
waits for transport capacity; callers should also bound their input queues.

The session methods are:

| Method | Result and behavior |
| --- | --- |
| `send_text(text)` | Wait for readiness; start a typed turn; return `ResponseRef` at response creation. |
| `request_reply(instruction=None, tool_call=None)` | Wait for readiness; request a reply without new user input, or report a recorded tool result. Arguments are keyword-only. |
| `add_message(role, text)` | Append user or assistant text as history; return `ItemRef`. No reply is requested. |
| `submit_tool_result(call, output)` | Record a result for a `ToolCallRef`. Repeating the same result is safe; changing it is rejected. |
| `continue_reply(parent)` | Wait for the parent's completion and all result acknowledgments, then continue its tool round. A new turn supersedes the round. |
| `cancel(response)` | Request a stop if needed; return when that response is terminal. Its `ResponseEnded` event gives the actual status, which can be completed if generation wins the race. A refusal received first raises. |
| `clear_audio()` | Clear buffered input and reset input conversion state. |
| `report_playback(report)` | Report a caller interruption or completed playback. |
| `close()` | Close once, cancel pending work, and release the connection. |

Tool definitions use `ToolDefinition(name=..., parameters=..., description=...)`.
`ToolCall` events contain parsed JSON arguments and a session-scoped reference.
Your application executes tools. The SDK never executes or replays them. A late
result can still be recorded; use `request_reply(tool_call=call)` when your
application wants the caller to hear it.

`PlaybackInterrupted(output=..., audio_end_ms=...)` means that caller speech
stopped an assistant message. Use `PlaybackFinished(response=..., played_ms=...)`
when all queued audio has finished, or the application has permanently stopped
it. Account for output latency before reporting completion. The SDK waits for
generation completion and supplies the private drain token. Saving or receiving
audio does not prove playback. If you cannot measure playback, omit the receipt;
the server uses its playback estimate.

Settings apply once. `None` for voice or instructions preserves the server
default; the endpoint must have a usable default voice if you omit it. There is
no reconnect, history replay, manual audio commit, or live configuration update.
References belong to one session. The SDK retains the last 128 responses without
retaining their audio; older references expire. The event queue holds 256 events,
and incoming WebSocket messages are limited to 1 MiB. A slow consumer or lost
connection ends the session and fails pending operations.
Future response causes, terminal statuses, and error scopes are exposed as
`"unknown"`. An absent response cause still means a reply to speech, as specified
by the protocol. Unknown server event kinds and additional fields are ignored.

`RealtimeTimeouts` separates connection setup, readiness admission, and request
acknowledgment. Defaults are 10, 30, and 10 seconds. `RealtimeAdmissionTimeout`
extends `RimeTimeoutError` and means readiness expired with no request outstanding;
the session remains usable. Once a request starts, its acknowledgment has the
separate `request_s` deadline. Only named refusals that made
no change are retried after a new ready event. An unknown request outcome closes
the connection and is never replayed. Cancelling a pending reply request prevents
an unsent request or cancels its late accepted response. The latter emits a local
`ResponseAbandoned(response: ResponseRef)` notice, with kind `response.abandoned`.
Its envelope has an empty `event_id` and no `request_id`. It can arrive after
`ResponseEnded` if completion races cancellation. Applications still own playback
reports, including zero playback for responses with no player. Cancelling a control
request can wait for its acknowledgment so the next request cannot consume it.
Cancelling `send_audio` after submission starts closes the session because part
of the chunk may already be buffered by the server. Do not retry that chunk.
Cancellation before the chunk's first socket send leaves the session usable and
restores the audio format and conversion state, including while waiting for the
write lock. Cancellation during a socket send has an unknown outcome.

The protocol target is Rime #1894 at `32d6bd1`, with compatible playback receipts
for #2499. Tests validate client messages against the pinned schema and exercise
a local peer. Production gateway authentication and endpoint conformance still
need validation against the deployment you will use.
