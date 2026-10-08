# Realtime / Prism for Python

A conversation keeps producing events while your application sends input, runs
tools, and plays audio. Keep one event consumer running for the session.
The SDK handles protocol state; your application handles those external actions.

Start with the [recorded-speech quick start](../README.md#prism-send-recorded-speech)
to save a spoken reply without audio hardware.

For live conversations, keep the session open after normal cancellation and
report playback from the output clock. Saving a file is not playback.
Typed turns also support mixed text and voice applications.

## Connect

Use `async with client.realtime.connect(...) as session`. The context opens the
WebSocket, initializes the session, and closes it on exit. Settings apply once;
open a new session to change them.

| Argument | Required? | Default and behavior |
| --- | --- | --- |
| `endpoint: str` | Yes | Full `wss://host/v1/realtime` URL. Plain `ws://` is allowed only for localhost. |
| `voice: str \| None` | If the server has no default voice | `None` uses the server default. A supplied string must be nonblank. |
| `instructions: str \| None` | No | `None` uses server defaults. A supplied string must be nonblank. |
| `tools: Sequence[ToolDefinition]` | No | `()`. Sends an empty tool list when omitted. |
| `interrupt_on_speech: bool` | No | `True`. Requests interruption when the server confirms user speech. |
| `timeouts: RealtimeTimeouts` | No | Connection 10 s, readiness 30 s, request 10 s. |
| `model` | No | `"prism"`, the only accepted value. |

All arguments are keyword-only. Credentials come from `Rime(api_key=...)` or
`RIME_API_KEY`, sent as `Authorization: Bearer <key>`. The endpoint must accept
that handshake. TTS model, endpoint, voice, and timeout settings do not apply.

Once connected, `session.info` gives the session ID, voice, model, effective
interruption setting, and any published tool wait durations. Check
`session.info.interrupt_on_speech` for the setting the server actually applied.
Prism sessions always request both text and audio output.

## Choose an operation

Each method is async. Arguments without a default are required.

| Method | Use it to | Result |
| --- | --- | --- |
| `send_text(text)` | Start a new user turn after readiness. | `ResponseRef` at response creation |
| `send_audio(chunk)` | Send microphone audio. | `None` |
| `request_reply(*, instruction=None, tool_call=None)` | Ask for a reply without new user input, optionally about a recorded tool result. | `ResponseRef` at response creation |
| `add_message(role, text)` | Add user or assistant history without requesting a reply. | `ItemRef` after acknowledgment |
| `submit_tool_result(call, output)` | Record a tool result without requesting a reply. | `None` after acknowledgment |
| `continue_reply(parent)` | Continue a tool round after parent completion and all result acknowledgments. | New `ResponseRef` |
| `cancel(response)` | Request that generation stop. | `None` when the response is terminal |
| `clear_audio()` | Clear buffered input and reset audio conversion. | `None` after acknowledgment |
| `report_playback(report)` | Report interrupted or finished playback. | `None` |
| `close()` | Close the session and release its resources. | `None`; safe to repeat |

Text passed to `send_text`, `add_message`, or `request_reply(instruction=...)`
must be nonblank and at most 4,000 characters. History roles are `"user"` and
`"assistant"`. A one-reply instruction does not replace session instructions.

Returning a `ResponseRef` means generation started. Wait for `ResponseEnded` to
know how it ended. Cancellation can race with completion, so inspect that event's
status instead of assuming `"cancelled"`.

## Send audio

Microphone capture and echo cancellation belong to your application. Feed audio
continuously, including silence and while the assistant speaks or tools run.
The server detects and commits speech; there is no manual commit method.

```python
from rimelabs_sdk.realtime import AudioChunk, PCMFormat

# Inside your capture loop; pcm_bytes contains complete PCM16 frames.
await session.send_audio(
    AudioChunk(data=pcm_bytes, format=PCMFormat(sample_rate=48000, channels=2))
)
```

Only `AudioChunk.data` is required. Its default format is 16 kHz mono signed
little-endian PCM16. `PCMFormat` accepts 8, 16, 24, or 48 kHz and one or two
channels; `encoding` must be `"pcm_s16le"`.

Input chunks can contain at most 192,000 bytes per `send_audio` call. The SDK
downmixes and resamples to 16 kHz mono, retaining conversion state across calls.
A format change or acknowledged `clear_audio()` resets that state. Await each
send and bound your capture queue so a slow connection cannot grow it forever.

Received `AudioDelta.audio` contains 24 kHz mono PCM16. The outgoing chunk limit
does not apply to received audio.

## Handle tools

Define each tool with `ToolDefinition(name=..., parameters=..., description=...)`.
The name and JSON Schema object are required. Description defaults to `""`.
Names must be nonblank and unique within the session; schemas must contain finite
JSON values.

Execution follows this order:

1. Receive `ToolCall`. Its `arguments` are a parsed JSON object.
2. Validate the arguments and execute the named tool once.
3. Submit a string result with `submit_tool_result(call, output)`. Use
   `json.dumps(...)` for a structured result.
4. Call `continue_reply(parent)` if the assistant should answer. The SDK waits
   for the parent to end and for every tool result acknowledgment.

Keep the event consumer running while submitting tool results and requesting
continuation. Run slow tools asynchronously or in a worker so they do not block
the event loop.

A new user turn or proactive reply supersedes the old tool round. Late results
can still be recorded; `request_reply(tool_call=call)` asks the assistant to report
one. Repeating the same recorded result is safe within the live session. Changing
it is rejected. Never rerun a tool just because delivery failed.

Server tool deadlines appear in `session.info.tool_result_timeout_s` and
`tool_continuation_timeout_s`. These are total durations, not remaining time.
The SDK exposes both a missing field and a server `null` as `None`, so `None`
does not prove that the server has no deadline.

## Read events and references

Use `async for event in session.events`. Each event has `session_id`, `event_id`,
nullable `request_id`, and a typed `payload` with a fixed `kind` string.

| Payload | Fields to use |
| --- | --- |
| `InputReady` | No data fields; `send_text` and `request_reply` handle this wait for you. |
| `SpeechStarted`, `SpeechStopped` | `item_id` and `audio_start_ms` or `audio_end_ms` |
| `InputCommitted` | `item_id` |
| `TranscriptDelta`, `TranscriptFinal` | `item_id` and `delta` or `text` |
| `TranscriptFailed` | `item_id`, `error` |
| `ResponseStarted` | `response`, `cause`; optional `parent` and `input_item_id` |
| `MessageStarted` | `output` |
| `TextDelta`, `TextDone` | `output` and `delta` or `text` |
| `AudioDelta`, `AudioDone` | `output`; `AudioDelta` also has `audio` |
| `ToolCall` | `call`, `item_id`, `name`, `arguments` |
| `ResponseEnded` | `response`, `status`; optional `reason` |
| `ResponseAbandoned` | `response`; the SDK is cancelling a response accepted after its caller was cancelled. |
| `FaultEvent` | `error`, a `RealtimeFault` |

References come from the SDK. Pass them back to identify the object you mean:

| Reference | Required fields |
| --- | --- |
| `ResponseRef` | `session_id`, `response_id` |
| `ItemRef` | `session_id`, `item_id` |
| `ToolCallRef` | `session_id`, `response_id`, `call_id` |
| `OutputRef` | `response`, `item_id`, `output_index`, `content_index` |

Every reference belongs to one session. The SDK retains the last 128 responses,
so older references expire. Current output uses `content_index=0`.

Unknown response causes, terminal statuses, and error scopes become `"unknown"`.
An absent response cause means speech. Unknown event kinds and additional fields
are ignored. The local `ResponseAbandoned` notice has an empty `event_id` and
`request_id=None`; it can arrive after `ResponseEnded` when completion races cancellation.

## Report playback

Generation completion and playback completion are separate. Report the player's
actual state, including output latency and queued audio.

| Report | Required fields | Optional fields |
| --- | --- | --- |
| `PlaybackInterrupted` | `output`, `audio_end_ms` | None |
| `PlaybackFinished` | `response` | `played_ms=None`, `audible_tail_ms=None` |

Pass either value to `await session.report_playback(report)`. An interruption
reports the point where caller speech stopped an assistant message.
`audio_end_ms` must be an integer from 0 through 4,294,967,295.

Finished playback means all queued audio has played or has permanently stopped.
Optional durations must be finite and nonnegative. The SDK waits for generation
completion and supplies the server's drain token. A response with no player can
report `played_ms=0` once its audio has been discarded.

Saving audio does not establish playback. Reports are optional; when you cannot
measure playback, omit them and let the server use its estimate. The recorded and typed examples omit these receipts because they do not play audio.
The voice examples report actual playback.

## Timeouts and failures

Configure connection deadlines with `RealtimeTimeouts`. All fields are
optional and must be finite positive seconds.

| Field | Default | Applies to |
| --- | --- | --- |
| `connect_s` | 10 | Opening the connection and waiting for the initial session event |
| `ready_s` | 30 | Admission, tool readiness, and waiting for generation to end before a playback report |
| `request_s` | 10 | Request submission and acknowledgment |

These deadlines do not bound the whole conversation or generation after its
creation acknowledgment. The recorded and typed examples use `asyncio.timeout(60)` around the whole
run. Voice conversations run until stopped or the connection fails. Set an application deadline that fits your use case.

`RealtimeAdmissionTimeout` means readiness expired with no request outstanding;
the session remains usable. An unknown request outcome closes the connection and
is never replayed. Named refusals that made no change can allow a retry; the SDK
handles typed-turn readiness retries within `ready_s`.

Protocol faults appear as `FaultEvent` and can also raise `RimeRealtimeError` on
the affected request. Its `.fault` contains `code`, `message`, `scope`, and any
request, response, item, or tool-call identifiers. A fault does not by itself
end a response. Session-scoped faults and transport failures end the session.

Malformed fields in known events and mismatched request acknowledgments close
the session with `RimeStreamError`. Unknown event kinds and extra fields remain
accepted and ignored. The SDK does not replay a request after a protocol failure.

Cancelling a reply request prevents an unsent request or cancels its late accepted
response. A control operation may wait for acknowledgment during cancellation.
Cancelling `send_audio` after its first socket send starts closes the session
because some audio may already be buffered. Do not retry that chunk.
Before the first socket send, cancellation leaves the session usable and restores
the audio conversion state.

Keep up with the event stream: its queue holds 256 events, and incoming WebSocket
messages are limited to 1 MiB. A slow consumer or lost connection ends the session.
There is no automatic reconnect, history replay, or live configuration update.

Local peer tests cover the SDK protocol handling. Validate the gateway handshake
and endpoint behavior for the deployment you use.
