# Realtime / Prism for Node.js

The endpoint must use Rime's `prism.` events and `prism_` fields. Endpoints
that use the former `prsm` names are not supported.

Keep one event consumer running while your application sends input, runs tools,
and plays audio. The SDK manages the protocol state. Your application owns
microphone capture, tool execution, and playback.

Start with the [recorded-speech quick start](../README.md#prism-send-recorded-speech)
to save a spoken reply without audio hardware.

For live conversations, keep the session open after a response is cancelled.
Report playback from the output clock. Saving a file is not playback.
Typed turns also support mixed text and voice applications.

## Optional conversation runner

Use `runConversation` to connect your audio adapters once. It sends input,
queues output, handles interruption, reports measured playback, and stops its
operations when the conversation ends. The direct session methods remain available.

```ts
import { Rime, runConversation } from "@rimelabs/sdk";

const client = new Rime();
const controller = new AbortController();
try {
  const session = await client.realtime.connect();
  // microphone and speaker are adapters owned by your application.
  await runConversation(session, {
    source: microphone,
    playback: speaker,
    onEvent(event) {
      if (event.payload.kind === "transcript.final")
        console.log(event.payload.text);
    },
    signal: controller.signal,
  });
} finally {
  await client.close();
}
```

The input adapter implements `ConversationSource`.
`read(signal: AbortSignal): Promise<AudioChunk | null>` returns one chunk,
with an optional format, or `null` at EOF. An omitted format uses 16 kHz mono
PCM16. Abort must reject and release a pending
read. The runner awaits each `session.sendAudio` call before reading again. It
uses the session's existing PCM conversion and transport backpressure. Input
continues during output playback.

The playback adapter implements `ConversationPlayback`:

| Method | Contract |
| --- | --- |
| `open(output: OutputRef): ConversationOutput` | Queue this output after previously opened outputs. It can be opened before the first audio chunk. |
| `interrupt(outputs: readonly OutputRef[]): void` | Stop the selected outputs as one batch, discard their unplayed audio, then resume remaining output in queue order. |
| `stop(signal: AbortSignal): Promise<void>` | Confirm all playback has stopped and queued audio is discarded. Keep caller-owned resources open. |

Each `ConversationOutput` implements:

| Method | Contract |
| --- | --- |
| `write(chunk: AudioChunk): void` | Queue 24 kHz mono PCM16 without blocking. Bound the queue and throw on overflow. |
| `end(): void` | Accept no more audio. Let queued and device audio drain. |
| `completion(signal: AbortSignal): Promise<number>` | Return actual played milliseconds after drain or interruption. Reject if measurement fails or the signal aborts. |

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

Abort must release each pending adapter operation and remove its listeners.
Cancellation of a completion wait must not cancel the device receipt. The runner
stops input, event processing, and report submission on exit. It then awaits
`stop(signal)` before aborting remaining receipt waits. The stop signal is
independent of caller cancellation and has a five-second deadline. `stop` must
resolve only after local or remote playback has stopped, including device
buffers. Sending a stop command is not confirmation. It must reject and release
its wait when the stop signal aborts. The runner awaits this cancellation cleanup
before it returns, so cleanup can extend beyond the deadline. A missing confirmation raises
`RimeTimeoutError`; disconnects and other shutdown failures must reject too.
The caller opens and closes the adapter resources. The runner does not open
audio devices or host a browser.

`onEvent` is optional. It receives every event read from `session.events`, in
order, after the runner applies audio coordination. It must be synchronous and
nonblocking. Returning a promise is rejected. Callback exceptions reach the
caller. The callback can schedule application work. Continue to use
`submitToolResult` and `continueReply`; the runner neither registers nor executes
tools. The application must cancel and await its own tool tasks. Response
failures and nonterminal faults are forwarded for application handling. Session
failures reject the runner promise.

The runner is the only event reader. Do not consume `session.events` at the same
time, call `reportPlayback` independently, or send a second audio source. Other
session methods remain available for manual tools and application actions.

On confirmed `speech.started`, the runner uses the effective
`session.info.interruptOnSpeech` setting. It stops affected output, waits for the
interrupted receipt, and reports the measured position. It ignores late audio
for that response. A `speech.hint` alone does not stop playback. A server
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
session closure, abort, or failure stops all runner operations and closes the
session. EOF stops remaining playback and waits for shutdown confirmation.
Abort, including an abort received during cleanup, waits for playback shutdown
confirmation or its five-second deadline, then awaits adapter cancellation cleanup
before rejecting with `signal.reason`.
Explicit session closure ends normally even with playback reports pending;
remote connection failures reject with the session error.
Expected cancellation errors during cleanup are ignored. Other cleanup errors
reach the caller if there is no original failure. They do not replace an
existing failure or caller abort. The signal is optional. Use
an application deadline if an adapter can wait indefinitely.

The runner adds no input or callback queue. It retains at most 64 responses,
128 output streams, and 256 pending operations. Exceeding a limit throws
`RimeResourceLimitError`. The adapters must also bound their audio queues.

The [live voice example](https://github.com/rimelabs/rime-sdk/blob/main/typescript/examples/realtime/conversation.ts) supplies the
[browser audio adapter](https://github.com/rimelabs/rime-sdk/blob/main/typescript/examples/common/browser-audio.ts). Browser hosting
stays in the example. Its player uses `AudioContext.getOutputTimestamp()` for
receipts and shutdown confirmation. A disconnect cannot confirm a remote stop.
The [manual tool example](https://github.com/rimelabs/rime-sdk/blob/main/typescript/examples/realtime/manual-tools.ts) keeps
tool execution outside the runner.

## Connect

Use `await client.realtime.connect()` with `RIME_API_KEY` set. It opens a WebSocket,
initializes one conversation, and returns a `RealtimeSession`. Settings apply
once. Open a new session to change them.

| Option | Required? | Default and behavior |
| --- | --- | --- |
| `endpoint` | No | Defaults to `wss://prism.api.rime.ai/v1/realtime`. Overrides must be full URLs; plain `ws://` is allowed only for localhost. |
| `voice` | If the server has no default voice | Omitted or `null` uses the server default. Supplied strings must be nonblank. |
| `instructions` | No | Omitted or `null` uses server defaults. Supplied strings must be nonblank. |
| `tools` | No | `[]`. Each tool requires `name` and a JSON Schema `parameters` object. `description` defaults to `""`. |
| `interruptOnSpeech` | No | `true`. Requests interruption when the server confirms user speech. |
| `turnDetection` | No | Omitted settings use the server's turn timing defaults. |
| `timeouts` | No | `{ connectS: 10, readyS: 30, requestS: 10 }`. Each field is optional. Units are seconds. |
| `model` | No | `"prism"`, the only accepted value. |
| `requestId` | No | Your id for the session, sent as `x-request-id`. Prism uses it if it is 1 to 128 printable ASCII characters, else makes its own. |
| `responseMetadata` | No | Labels every reply carries in `response.started.metadata`, such as `{ step: "3" }`: up to 16 strings, keys up to 64 characters and values up to 512, no key starting with `prism_`. |
| `signal` | No | An `AbortSignal` that cancels connection setup. It does not cancel an established session. |

Credentials come from `new Rime({ apiKey: "..." })` or `RIME_API_KEY`. Prism uses
`Authorization: Bearer <key>`, without a TTS token exchange. TTS endpoint, model,
voice, and timeout settings do not apply to Realtime.

After connection, `session.info` contains `sessionId`, `requestId`, `model`,
`voice`, `interruptOnSpeech`, `turnDetection`, `responseMetadata`,
`toolResultTimeoutS`, and `toolContinuationTimeoutS`. `requestId` is the id
Prism's logs use; include it when you report a problem. It is `null` on servers
that do not send it.
Check the effective interruption setting there. Tool wait durations are total
server deadlines, not remaining time. A `null` duration can mean an omitted
field; it does not prove that no deadline exists.

Close the session in `finally` with `await session.close()`. Closing the parent
client closes all its TTS and Realtime operations. Both objects also support
`Symbol.asyncDispose`. Repeated close calls are safe.

## Optional turn timing

Pass a `TurnDetection` object to override how readily Prism ends a spoken turn.
Each field is optional; omitted fields use server defaults.

| Field | Values | Current server default |
| --- | --- | --- |
| `eagerness` | `"low"`, `"medium"`, `"high"`, `"auto"` | `"auto"`, equivalent to `"medium"` |
| `endOfTurnTimeoutMs` | Integer from 1,000 through 10,000 | 5,000 ms |

`"low"` waits for more certainty that the user has finished. `"high"` answers
sooner, with more risk of cutting in. The timeout is a maximum silence limit,
not a fixed delay before each reply. If the server falls back to its local
voice activity detector, it ends turns after 500 ms of silence instead.

```typescript
const session = await client.realtime.connect({
  turnDetection: { eagerness: "low", endOfTurnTimeoutMs: 5000 },
});
try {
  console.log(session.info.turnDetection);
  // Send input and consume session.events here.
} finally {
  await session.close();
}
```

`session.info.turnDetection` is a `TurnDetectionInfo` with the settings the
server reported in its initialization acknowledgment. A missing field is
`null`; the SDK does not substitute a requested value or assume a server
default. An unrecognized server eagerness becomes `"unknown"`.
`interruptOnSpeech` remains a separate connection option.

## Choose an operation

Every method returns a promise. Arguments without `?` are required.

| Method | Result and behavior |
| --- | --- |
| `sendText(text, options?)` | Starts a typed user turn after readiness; returns `ResponseRef` at response creation. |
| `sendAudio(chunk, options?)` | Sends microphone audio with transport backpressure. |
| `requestReply(options?)` | Requests a reply without new user input; returns `ResponseRef`. Options can include `instruction` and `toolCall`. |
| `addMessage(role, text, options?)` | Adds user or assistant history without a reply; returns `ItemRef` after acknowledgment. |
| `submitToolResult(call, output, options?)` | Records a string result and waits for acknowledgment. |
| `continueReply(parent, options?)` | Waits for parent completion and all tool result acknowledgments, then returns a new `ResponseRef`. |
| `cancel(response, options?)` | Requests generation cancellation and waits for a terminal response or refusal. |
| `clearAudio(options?)` | Clears buffered input and resets audio conversion after acknowledgment. |
| `setResponseMetadata(metadata, options?)` | Replaces the labels for later replies once Prism confirms. |
| `reportPlayback(report, options?)` | Reports interrupted or finished playback. |
| `close()` | Closes the session and releases its resources. |

Operation options accept `signal?: AbortSignal`. `requestReply` accepts its
instruction, tool call, and signal in one object. Methods other than `sendText`,
`requestReply`, `addMessage`, and `continueReply` return `Promise<void>`.

Text must be nonblank and contain at most 4,000 Unicode characters. History
roles are `"user"` and `"assistant"`. A reply instruction applies to one response;
it does not replace session instructions.

A returned `ResponseRef` means generation started. Read `response.ended` for the
terminal status. Generation can finish before cancellation takes effect, so a
successful `cancel()` does not guarantee a `"cancelled"` status.

## Send audio

Pass a plain object with `data: Uint8Array`. Node.js `Buffer` is also accepted.
The optional `format` object defaults to 16 kHz mono signed little-endian PCM16.

```typescript
await session.sendAudio({
  data: pcmBytes,
  format: { sampleRate: 48000, channels: 2 },
});
```

Supported `sampleRate` values are `8000`, `16000`, `24000`, and `48000`.
Channels can be `1` or `2`; `encoding` can only be `"pcm_s16le"`. Each field in
`format` is optional. Chunks must contain complete sample frames and at most
192,000 bytes. The SDK downmixes and resamples to 16 kHz mono, retaining state
across calls. A format change or acknowledged `clearAudio()` resets that state.

Await each send and bound your capture queue. Keep audio flowing, including
silence and while the assistant speaks or tools run. Prism detects and commits
speech; there is no manual commit method. Echo cancellation belongs to your app.

Received `audio.delta` events carry 24 kHz mono PCM16. The outgoing chunk limit
does not apply to received audio.

## Handle tools

Declare tools with plain `ToolDefinition` objects. Names must be nonblank and
unique; schemas must contain finite JSON values. The SDK copies schemas before
connection setup, so later caller changes cannot alter initialization.

Set `prismFiller` to control what Prism says while the tool runs:

```ts
import type { ToolDefinition } from "@rimelabs/sdk";

const pressDigits: ToolDefinition = {
  name: "press_digits",
  parameters: { type: "object", properties: { digits: { type: "string" } } },
  prismFiller: { type: "none" },
};
```

| Setting | Behavior |
| --- | --- |
| Omitted | Use the server default, currently `auto`. |
| `{ type: "none" }` | Suppress filler for this tool. |
| `{ type: "text", text: "One moment." }` | Speak the supplied text. |
| `{ type: "auto" }` | Let Prism write a line from the tool description. |
| `{ type: "auto", instructions: "Say you are checking the order." }` | Guide the generated line. |

Text and instructions must be valid UTF-8, contain at most 500 Unicode scalar
values, and contain a character outside the Unicode `White_Space` set. This
matches the engine's validation. The SDK preserves the supplied text and copies
filler settings before connection setup. Invalid settings raise `RimeInputError` before opening a
connection, including for JavaScript callers. The exported `PrismFiller` union
also checks TypeScript callers.

Filler applies when the reply has said nothing before calling a tool. If a reply
calls several tools, the first call's tool controls the filler. `none` suppresses
filler only; it does not silence other speech. Settings are fixed for the session.

Execute a tool once for each `tool.call` event. Validate its parsed `arguments`,
then submit a string result with `submitToolResult(payload.call, output)`.
Use `JSON.stringify(...)` for a structured result. Call
`continueReply(payload.call)` when the assistant should answer; a tool call
reference also carries its parent response identity.

Keep the event consumer running while submitting tool results and requesting
continuation. Run slow tools asynchronously or in a worker so they do not block
the event loop.

A new user turn or proactive reply supersedes the old tool round. Late results
can still be recorded. Use `requestReply({ toolCall: call })` to ask the assistant
to report a recorded result. Repeating the same result is safe within the live
session; changing it is rejected. Do not rerun a tool because delivery failed.

## Read events and references

Use `for await (const event of session.events)`. Each `SessionEvent` has
`sessionId`, `eventId`, nullable `requestId`, and a `payload`. Switch on
`payload.kind` to narrow the TypeScript union. All public types and error classes
are exported from `@rimelabs/sdk`.

| Kind | Payload fields |
| --- | --- |
| `input.ready` | No data fields. Turn methods handle the readiness wait. |
| `speech.hint` | `audioStartMs`; the user may have started speaking over a playing reply. It can be a false alarm: lower playback, do not stop it. |
| `speech.started` | `itemId`, `audioStartMs` |
| `speech.stopped` | `itemId`, `audioEndMs`, `endReason`: what ended the turn (`semantic`, `timeout`, `silence`, `max_duration` or `detector_changed`), `unknown` for a newer value, or null from a server that does not send it |
| `input.committed` | `itemId` |
| `transcript.delta`, `transcript.final` | `itemId` and `delta` or `text` |
| `transcript.failed` | `itemId`, `error` |
| `response.started` | `response`, `cause`, nullable `parent` and `inputItemId`, `inputItemIds` (the user items it answers), `metadata` (your own keys) |
| `message.started` | `output` |
| `text.delta`, `text.done` | `output` and `delta` or `text` |
| `audio.delta`, `audio.done` | `output`; a delta also has `audio.data` and a complete `audio.format` |
| `tool.call` | `call`, `itemId`, `name`, parsed `arguments` |
| `response.ended` | `response`, `status`, nullable `reason` |
| `response.abandoned` | `response`; the SDK requests cancellation after the requesting caller aborted. |
| `error` | `error`, a `RealtimeFault` |

References come from the SDK. Pass them back to identify the object you mean.

| Reference | Fields |
| --- | --- |
| `ResponseRef` | `sessionId`, `responseId` |
| `ItemRef` | `sessionId`, `itemId` |
| `ToolCallRef` | `sessionId`, `responseId`, `callId` |
| `OutputRef` | `response`, `itemId`, `outputIndex`, `contentIndex` |

Every reference belongs to one session. Only the last 128 responses are retained;
older references expire. Current output uses `contentIndex: 0`.

Unknown event kinds and extra fields are ignored. Unknown response causes,
terminal statuses, and error scopes become `"unknown"`. An absent response cause
means speech. The local `response.abandoned` event has an empty `eventId` and
`requestId: null`; it can follow `response.ended` when completion races cancellation.

## Report playback

Generation completion and playback completion are separate. Report the player's
actual state, including output latency and queued audio.

| Report object | Required fields | Optional fields |
| --- | --- | --- |
| Interrupted | `kind: "interrupted"`, `output`, `audioEndMs` | None |
| Finished | `kind: "finished"`, `response` | `playedMs`, `audibleTailMs`; both default to `null` |

Pass the object to `await session.reportPlayback(report)`. Interruption records
the point where caller speech stopped an assistant message. `audioEndMs` must
be an integer from 0 through 4,294,967,295. Finished playback means all queued
audio has played or has permanently stopped. Optional durations must be finite,
nonnegative milliseconds. The SDK waits for generation completion and supplies
the server's drain token.

Saving audio does not establish playback. Reports are optional. If you cannot
measure playback, omit them and let the server use its estimate. The recorded and typed examples have no player and omit these reports.
The voice examples report actual playback.

## Timeouts and failures

Configure positive, finite seconds through `timeouts`. `connectS` bounds the
WebSocket handshake and initial session event. `readyS` bounds turn admission,
tool readiness, and waiting for generation to end before a playback report.
Once a request is submitted, `requestS` bounds its write and acknowledgment.

These limits do not bound the entire conversation or generation after creation.
Set an application deadline and close the session when it expires. The recorded and typed examples use a 60-second deadline. Voice conversations
run until stopped or the connection fails.

`RealtimeAdmissionTimeout` means readiness expired with no request outstanding;
the session remains usable. An unknown request outcome closes the connection and
is never replayed. The SDK retries typed-turn admission only after named refusals
that confirm the request made no change.

Protocol faults appear as `error` events and can also reject an affected method
with `RimeRealtimeError`. Its `.fault` has `code`, `message`, `scope`, nullable
`requestId`, `responseId`, `itemId`, `correlationId`, `parameter`, and a `callIds`
array. A response fault does not itself end generation. Session faults and lost
connections end the session.

`NormalizedRealtimeFault.retryable` reports whether the same request, unchanged,
may succeed on another attempt. Read `event.payload.error.retryable` on an `error`
event or `error.fault.retryable` on `RimeRealtimeError`. The SDK derives this
value from the code; Prism does not send a retry flag.

`RealtimeFault` accepts the previous fault shape, with an optional `retryable`
field, for caller-created faults. `RimeRealtimeError` accepts this shape and
ignores any supplied retry flag. Its `.fault` and SDK-emitted event faults are
`NormalizedRealtimeFault` values with a required boolean `retryable` field.
The SDK freezes these faults and their `callIds` arrays. Later changes to a
constructor input do not change the error's fault.

`TranscriptFailed`, `FaultEvent`, `InputEvent`, and `SessionEvent` default to
`RealtimeFault`, so existing event literals can omit `retryable`. These types
also accept a fault type parameter. `session.events` and the `runConversation`
event callback use `SessionEvent<NormalizedRealtimeFault>`, which gives their
faults a required boolean `retryable` field. An explicit `SessionEvent`
annotation uses the default type and makes `retryable` optional.

| Error code | `retryable` |
| --- | --- |
| `capacity_exceeded` | `true` |
| `unavailable` | `true` |
| `response_timeout` | `true` |
| `context_length_exceeded` | `false` |
| All other codes, including unknown codes | `false` |

The application chooses the delay, attempt limit, and recovery operation. This
value does not start a retry. It does not describe a local `RimeTimeoutError` or
prove that no output or side effect occurred. The SDK's existing readiness
retries have separate rules: an explicit refusal such as `typed_turn_busy`
permits another attempt after a new readiness event, although its `retryable`
value is `false` under this contract.

For example, Prism can accept a user's question and then report
`capacity_exceeded`. `sendText()` has already returned a `ResponseRef`; the
failure arrives through an `error` event. Wait for `response.ended`, then apply
the application's retry policy. If the question is already in history, use
`requestReply()` to ask for another reply when appropriate. Repeating
`sendText()` can add the question twice. A `response_timeout` can follow partial
text or audio, which the application must handle before another attempt.

For `context_length_exceeded`, the server message contains token counts.
Unchanged attempts will fail until the conversation is shorter. Start a new
session with shorter context. The protocol also supports history deletion, but
this SDK does not yet expose `conversation.item.delete`.

Malformed fields in known events and mismatched request acknowledgments close
the session with `RimeStreamError`. Unknown event kinds and extra fields remain
accepted and ignored. The SDK does not replay a request after a protocol failure.

Aborting an operation rejects it with `RimeCancelledError`. For reply requests,
the SDK prevents an unsent request or cancels a late accepted response. Control
operations can wait for acknowledgment before rejecting, to keep acknowledgment
matching correct. A submitted tool result keeps matching its acknowledgment even
if its caller aborts. Aborting audio after submission starts closes the session;
before submission, it restores conversion state. Do not resend a chunk with an
unknown outcome.

Keep up with the event stream. The queue holds 256 events; incoming messages
are limited to 1 MiB. A slow consumer ends the session. There is no automatic
reconnect, history replay, or live configuration update.

Local peer tests cover the SDK protocol handling. Validate the gateway handshake
and endpoint behavior for the deployment you use.
