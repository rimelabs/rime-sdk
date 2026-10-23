# Realtime / Prism for Python

The endpoint must use Rime's `prism.` events and `prism_` fields. Endpoints
that use the former `prsm` names are not supported.

A conversation keeps producing events while your application sends input, runs
tools, and plays audio. Keep one event consumer running for the session.
The SDK handles protocol state; your application handles those external actions.

Start with the [recorded-speech quick start](../README.md#prism-send-recorded-speech)
to save a spoken reply without audio hardware.

For live conversations, keep the session open after a response is cancelled.
Report playback from the output clock. Saving a file is not playback.
Typed turns also support mixed text and voice applications.

## Optional conversation runner

Use `run_conversation` to connect your audio adapters once. It sends input,
queues output, handles interruption, reports measured playback, and stops its
operations when the conversation ends. The direct session methods remain available.

```python
from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import TranscriptFinal, run_conversation

def show_transcript(event):
    if isinstance(event.payload, TranscriptFinal):
        print(event.payload.text)

# microphone and speaker are adapters owned by your application.
async with Rime() as client:
    async with client.realtime.connect() as session:
        await run_conversation(
            session,
            source=microphone,
            playback=speaker,
            on_event=show_transcript,
        )
```

The input adapter implements `ConversationSource`. Its async `read()` returns
one `AudioChunk`, with an optional format, or `None` at EOF. An omitted format
uses 16 kHz mono PCM16. The runner awaits each
`session.send_audio` call before reading again. It uses the session's existing
PCM conversion and transport backpressure. Input continues during output playback.

The playback adapter implements `ConversationPlayback`:

| Method | Contract |
| --- | --- |
| `open(output: OutputRef) -> ConversationOutput` | Queue this output after previously opened outputs. It can be opened before the first audio chunk. |
| `interrupt(outputs: Sequence[OutputRef]) -> None` | Stop the selected outputs as one batch, discard their unplayed audio, then resume remaining output in queue order. |
| `await stop() -> None` | Confirm all playback has stopped and queued audio is discarded. Keep caller-owned resource contexts open. |

Each `ConversationOutput` implements:

| Method | Contract |
| --- | --- |
| `write(chunk: AudioChunk) -> None` | Queue 24 kHz mono PCM16 without blocking. Bound the queue and raise on overflow. |
| `end() -> None` | Accept no more audio. Let queued and device audio drain. |
| `await completion() -> float` | Return actual played milliseconds after drain or interruption. Raise if the device cannot measure playback. |

All synchronous adapter methods must return promptly. Completion can be awaited
before `end()`. Keep receipts separate for each `OutputRef`, even when a new
response starts before an old receipt completes. Play outputs in the order they
were opened, using one FIFO audio queue. Do not mix outputs, interleave their
audio, or play later outputs before earlier outputs finish or are interrupted.
Prism assumes that interrupting a partially played reply leaves later replies
in the same turn unheard. Independent concurrent playback violates that assumption.
The runner passes all affected output references in one `interrupt` call. Stop
them and discard their queued audio before starting any remaining output. Then
resume valid output in queue order, without waiting for another chunk or event.
Interruption must work after `end()` and tolerate repeated calls. A remote
adapter must preserve the batch, for example with one message containing all
selected output IDs. Shutdown stops all output and must not resume playback.
Receipt delivery can be delayed or arrive out of order; audible playback cannot.
A receipt includes the device buffer where device timing is available. Bytes
received or queued do not measure playback. Zero is valid for an output that
played no audio.

Pending `read()` and `completion()` calls must release their waits on normal
Python task cancellation. Cancellation of a completion wait must not cancel the
device receipt. For an adapter backed by a shared future, use
`await asyncio.shield(receipt)`. If an adapter cancels a receipt independently,
the runner raises `RimeStreamError` and shuts down; this is not caller cancellation.
The runner stops input, event processing, and
report submission on exit. It then awaits `stop()` for up to five seconds before
cancelling remaining receipt waits. `stop()` must return only after local or
remote playback has stopped, including device buffers. Sending a stop command
is not confirmation. It must release its wait on cancellation. The runner awaits
this cancellation cleanup before it returns, so cleanup can extend beyond the deadline. A missing
confirmation raises `RimeTimeoutError`; disconnects and other shutdown failures
must raise an error. The runner does not enter or close adapter resource contexts.

`on_event` is optional. It receives every event read from `session.events`, in
order, after the runner applies audio coordination. It must be synchronous and
nonblocking. An async callback is rejected. Callback exceptions reach the caller.
The callback can schedule tool work in an application-owned task group. Continue
to use `submit_tool_result` and `continue_reply`; the runner neither registers
nor executes tools. The application must cancel and await its own tool tasks.
Response failures and nonterminal faults are forwarded for application handling.
Session failures raise from the runner.

The runner is the only event reader. Do not consume `session.events` at the same
time, call `report_playback` independently, or send a second audio source. Other
session methods remain available for manual tools and application actions.

On confirmed `SpeechStarted`, the runner uses the effective
`session.info.interrupt_on_speech` setting. It stops affected output, waits for
the interrupted receipt, and reports the measured position. It ignores late
audio for that response. A `SpeechHint` alone does not stop playback. A server
`truncate_not_current` refusal is forwarded as an event; it does not stop a newer
response. Finished reports wait for generation completion and every output
receipt, including outstanding playback from earlier responses. Prism treats
the latest drain token as confirmation that all earlier playback has stopped.
An abandoned response also stops its dependent tool continuations, identified
by response parent references. Unrelated later turns remain eligible to play.
Late audio from an interrupted response or its continuations is discarded.
The runner retains at most 128 response parent references.
A report retains the measured duration of its own response. Waiting for earlier
playback does not block events or input, and does not wait for later responses.

Once called, the runner owns the session's active lifetime. Input EOF, explicit
session closure, cancellation, or failure stops all runner operations and closes
the session. EOF stops remaining playback and waits for shutdown confirmation.
Cancellation, including repeated cancellation during cleanup, waits for playback
shutdown confirmation or its five-second deadline before raising
`asyncio.CancelledError`. Explicit session closure ends normally even with playback
reports pending; remote connection failures raise the session error.
Cleanup errors reach the caller if there is no original failure. They
do not replace an existing failure or cancellation. Use
`asyncio.timeout(...)` to limit the run; playback cleanup can add five seconds
plus the time needed for adapter cancellation cleanup.

The runner adds no input or callback queue. It retains at most 64 responses,
128 output streams, and 256 pending operations. Exceeding a limit raises
`RimeResourceLimitError`. The adapters must also bound their audio queues.

The [live voice example](https://github.com/rimelabs/rime-sdk/blob/main/python/examples/realtime/voice.py) supplies the
[PortAudio adapters](https://github.com/rimelabs/rime-sdk/blob/main/python/examples/audio_devices.py). They keep microphone capture
active during playback and use the DAC clock for receipts. Device libraries stay
in the examples. The [manual tool example](https://github.com/rimelabs/rime-sdk/blob/main/python/examples/realtime/manual_tools.py)
keeps tool execution outside the runner.

## Connect

Use `async with client.realtime.connect() as session` with `RIME_API_KEY` set.
The context opens the WebSocket, initializes the session, and closes it on exit.
Settings apply once, except the reply labels, which `set_response_metadata`
replaces; open a new session to change the others.

| Argument | Required? | Default and behavior |
| --- | --- | --- |
| `endpoint: str \| None` | No | Omitted or `None` uses `wss://prism.api.rime.ai/v1/realtime`. Overrides must be full URLs; plain `ws://` is allowed only for localhost. |
| `voice: str \| None` | If the server has no default voice | `None` uses the server default. A supplied string must be nonblank. |
| `instructions: str \| None` | No | `None` uses server defaults. A supplied string must be nonblank. |
| `tools: Sequence[ToolDefinition]` | No | `()`. Sends an empty tool list when omitted. |
| `interrupt_on_speech: bool` | No | `True`. Requests interruption when the server confirms user speech. |
| `turn_detection: TurnDetection \| None` | No | Omitted settings use the server's turn timing defaults. |
| `timeouts: RealtimeTimeouts` | No | Connection 10 s, readiness 30 s, request 10 s. |
| `model` | No | `"prism"`, the only accepted value. |
| `request_id: str \| None` | No | Your id for the session. Sent as `x-request-id`. It must be nonblank printable ASCII, or the SDK raises `RimeInputError`. Prism uses it if it is at most 128 characters, else makes its own. For now, hosted Prism always makes its own. |
| `response_metadata: Labels \| None` | No | Labels every reply carries in `ResponseStarted.metadata`, such as `{"step": "3"}`: up to 16 strings, keys up to 64 characters and values up to 512, no key starting with `prism_`. Prism refuses labels that break these limits, or keys starting with `prism_`, with `RimeRealtimeError` code `invalid_session`. |

All arguments are keyword-only. Credentials come from `Rime(api_key=...)` or
`RIME_API_KEY`, sent as `Authorization: Bearer <key>`. The endpoint must accept
that handshake. TTS model, endpoint, voice, and timeout settings do not apply.

Once connected, `session.info` gives the session ID, voice, model, effective
interruption setting, turn timing, reply labels (`response_metadata`), and any
published tool wait durations.
`session.info.request_id` is the id Prism's logs use; include it when you report
a problem. It is `None` on servers that do not send it. An event's or error's
`request_id` names the SDK request that caused it instead. Check
`session.info.interrupt_on_speech` for the setting the server actually applied.
Prism sessions always request both text and audio output.

## Optional turn timing

Use `TurnDetection` to override how readily Prism ends a spoken turn.
Each field is optional; `None` leaves that field to the server.

| Field | Values | Current server default |
| --- | --- | --- |
| `eagerness` | `"low"`, `"medium"`, `"high"`, `"auto"` | `"auto"`, equivalent to `"medium"` |
| `end_of_turn_timeout_ms` | Integer from 1,000 through 10,000 | 5,000 ms |

`"low"` waits for more certainty that the user has finished. `"high"` answers
sooner, with more risk of cutting in. The timeout is a maximum silence limit,
not a fixed delay before each reply. If the server falls back to its local
voice activity detector, it ends turns after 500 ms of silence instead.

```python
from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import TurnDetection

async with Rime() as client:
    async with client.realtime.connect(
        turn_detection=TurnDetection(eagerness="low", end_of_turn_timeout_ms=5000),
    ) as session:
        print(session.info.turn_detection)
        # Send input and consume session.events here.
```

`session.info.turn_detection` is a `TurnDetectionInfo` with the settings the
server reported in its initialization acknowledgment. A missing field is
`None`; the SDK does not substitute a requested value or assume a server
default. An unrecognized server eagerness becomes `"unknown"`.
`interrupt_on_speech` remains a separate connection option.

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
| `set_response_metadata(metadata)` | Replace the labels for later replies. | `None` once Prism confirms |
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

Set `prism_filler` to control what Prism says while the tool runs:

```python
from rimelabs_sdk.realtime import PrismFiller, ToolDefinition

press_digits = ToolDefinition(
    name="press_digits",
    parameters={"type": "object", "properties": {"digits": {"type": "string"}}},
    prism_filler=PrismFiller(type="none"),
)
```

| Setting | Behavior |
| --- | --- |
| Omitted or `None` | Use the server default, currently `auto`. |
| `PrismFiller(type="none")` | Suppress filler for this tool. |
| `PrismFiller(type="text", text="One moment.")` | Speak the supplied text. |
| `PrismFiller(type="auto")` | Let Prism write a line from the tool description. |
| `PrismFiller(type="auto", instructions="Say you are checking the order.")` | Guide the generated line. |

Text and instructions must be valid UTF-8, contain at most 500 Unicode scalar
values, and contain a character outside the Unicode `White_Space` set. This
matches the engine's validation. The SDK preserves the supplied text.
Invalid settings raise `RimeInputError` before opening a connection.

Filler applies when the reply has said nothing before calling a tool. If a reply
calls several tools, the first call's tool controls the filler. `none` suppresses
filler only; it does not silence other speech. Settings are fixed for the session.

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
| `SpeechHint` | `audio_start_ms`; the user may have started speaking over a playing reply. It can be a false alarm: lower playback, do not stop it. |
| `SpeechStarted` | `item_id`, `audio_start_ms` |
| `SpeechStopped` | `item_id`, `audio_end_ms`, `end_reason`: what ended the turn (`semantic`, `timeout`, `silence`, `max_duration` or `detector_changed`), `unknown` for a newer value, or None from a server that does not send it |
| `InputCommitted` | `item_id` |
| `TranscriptDelta`, `TranscriptFinal` | `item_id` and `delta` or `text` |
| `TranscriptFailed` | `item_id`, `error` |
| `ResponseStarted` | `response`, `cause`; optional `parent` and `input_item_id`; `input_item_ids` (the user items it answers); `metadata` (your own keys) |
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

`RealtimeFault.retryable` reports whether the same request, unchanged, may
succeed on another attempt. Read `event.payload.error.retryable` on a
`FaultEvent` or `error.fault.retryable` on `RimeRealtimeError`. The SDK derives
this value from the code; Prism does not send a retry flag.

| Error code | `retryable` |
| --- | --- |
| `capacity_exceeded` | `True` |
| `unavailable` | `True` |
| `response_timeout` | `True` |
| `context_length_exceeded` | `False` |
| All other codes, including unknown codes | `False` |

The application chooses the delay, attempt limit, and recovery operation. This
value does not start a retry. It does not describe a local `RimeTimeoutError` or
prove that no output or side effect occurred. The SDK's existing readiness
retries have separate rules: an explicit refusal such as `typed_turn_busy`
permits another attempt after a new readiness event, although its `retryable`
value is `False` under this contract.

For example, Prism can accept a user's question and then report
`capacity_exceeded`. `send_text()` has already returned a `ResponseRef`; the
failure arrives through `FaultEvent`. Wait for `ResponseEnded`, then apply
the application's retry policy. If the question is already in history, use
`request_reply()` to ask for another reply when appropriate. Repeating
`send_text()` can add the question twice. A `response_timeout` can follow partial
text or audio, which the application must handle before another attempt.

For `context_length_exceeded`, the server message contains token counts.
Unchanged attempts will fail until the conversation is shorter. Start a new
session with shorter context. The protocol also supports history deletion, but
this SDK does not yet expose `conversation.item.delete`.

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
