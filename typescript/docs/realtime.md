# Realtime / Prism for Node.js

Keep one event consumer running while your application sends input, runs tools,
and plays audio. The SDK manages the protocol state. Your application owns
microphone capture, tool execution, and playback.

Start with the [voice conversation](https://github.com/rimelabs/rime-sdk/blob/main/examples/typescript/realtime/voice.ts)
for microphone input, playback, and caller interruption. The
[recorded-speech example](https://github.com/rimelabs/rime-sdk/blob/main/examples/typescript/realtime/recorded.ts)
saves a spoken reply without audio hardware. See the
[example setup](https://github.com/rimelabs/rime-sdk/blob/main/examples/README.md)
for commands, device requirements, and the voice tool example.

Typed turns remain useful for connection checks and mixed text/voice applications.
The live examples keep the session open after normal cancellation and report
playback from the output clock. Saving a file is not playback.

## Connect

Use `await client.realtime.connect(options)`. It opens a WebSocket, initializes
one conversation, and returns a `RealtimeSession`. Settings apply once. Open a
new session to change them.

| Option | Required? | Default and behavior |
| --- | --- | --- |
| `endpoint` | Yes | Full `wss://host/v1/realtime` URL. Plain `ws://` is allowed only for localhost. |
| `voice` | If the server has no default voice | Omitted or `null` uses the server default. Supplied strings must be nonblank. |
| `instructions` | No | Omitted or `null` uses server defaults. Supplied strings must be nonblank. |
| `tools` | No | `[]`. Each tool requires `name` and a JSON Schema `parameters` object. `description` defaults to `""`. |
| `interruptOnSpeech` | No | `true`. Requests interruption when the server confirms user speech. |
| `timeouts` | No | `{ connectS: 10, readyS: 30, requestS: 10 }`. Each field is optional. Units are seconds. |
| `model` | No | `"prism"`, the only accepted value. |
| `signal` | No | An `AbortSignal` that cancels connection setup. It does not cancel an established session. |

Credentials come from `new Rime({ apiKey: "..." })` or `RIME_API_KEY`. Prism uses
`Authorization: Bearer <key>`, without a TTS token exchange. TTS endpoint, model,
voice, and timeout settings do not apply to Realtime.

After connection, `session.info` contains `sessionId`, `model`, `voice`,
`interruptOnSpeech`, `toolResultTimeoutS`, and `toolContinuationTimeoutS`.
Check the effective interruption setting there. Tool wait durations are total
server deadlines, not remaining time. A `null` duration can mean an omitted
field; it does not prove that no deadline exists.

Close the session in `finally` with `await session.close()`. Closing the parent
client closes all its TTS and Realtime operations. Both objects also support
`Symbol.asyncDispose`. Repeated close calls are safe.

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

Execute a tool once for each `tool.call` event. Validate its parsed `arguments`,
then submit a string result with `submitToolResult(payload.call, output)`.
Use `JSON.stringify(...)` for a structured result. Call
`continueReply(payload.call)` when the assistant should answer; a tool call
reference also carries its parent response identity.

The [tool example](https://github.com/rimelabs/rime-sdk/blob/main/examples/typescript/realtime/tools.mjs)
keeps the event consumer running while result submissions and continuation wait.
It uses local order data. Slow tools need asynchronous work or a worker thread
so they do not block the Node.js event loop.

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
| `speech.started`, `speech.stopped` | `itemId` and `audioStartMs` or `audioEndMs` |
| `input.committed` | `itemId` |
| `transcript.delta`, `transcript.final` | `itemId` and `delta` or `text` |
| `transcript.failed` | `itemId`, `error` |
| `response.started` | `response`, `cause`, nullable `parent` and `inputItemId` |
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

The [pinned contract](https://github.com/rimelabs/rime-sdk/blob/main/conformance/prism/README.md)
is the local test target. The deployed gateway handshake and endpoint behavior
still need validation for the deployment you use.
