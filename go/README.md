# Rime SDK for Go

Generate speech with Coda or Mist v3, transcribe audio with STT, and run Prism conversations
through the `rime.Client`. Your application handles audio capture and playback.

## Installation

Requires **Go 1.24 or later**; cgo is not required. Install the module from the
public `rimelabs/rime-sdk` repository:

```sh
mkdir rime-quickstart
cd rime-quickstart
go mod init example.com/rime-quickstart
go get github.com/rimelabs/rime-sdk/go@latest
export RIME_API_KEY="your-api-key"
```

For an existing module, skip the first three commands. Get an API key from the
[Rime dashboard](https://app.rime.ai/). A key is required:
`rime.NewClient(rime.Config{})` reads `RIME_API_KEY`, or you can set `Config.APIKey`
to override it. The SDK does not load `.env` files. Shell commands below assume
macOS or Linux.

## TTS

Use `client.TTS.Synthesize(ctx, text, options)` for a complete string, or
`client.TTS.Stream(ctx, source, options)` for incremental text. Both return
`(*AudioStream, error)`; read audio with `Recv()` until `io.EOF`. For complete
audio in memory, `client.TTS.SynthesizeToBuffer(ctx, text, options)` returns
`([]byte, error)`. Text is required and must contain non-whitespace characters.
Pass a non-nil `context.Context` to every operation.

### Client options

These optional `rime.Config` fields configure TTS. They do not configure STT.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Model` | Select `"coda"` or `"mistv3"`. | No | `"coda"` when empty. |
| `Endpoint` | Override the TTS TLS hostname and optional port, such as `"coda.api.rime.ai:443"`; no scheme or path. | No | `coda.api.rime.ai:443` for Coda; `mist.api.rime.ai:443` for Mist v3. |
| `Timeout` | Overall TTS timeout as a `time.Duration`, such as `60 * time.Second`; zero disables it. | No | `0` |

### Synthesis options

All three TTS methods accept `rime.SynthesisOptions{}` with these optional fields.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Voice` | Voice name for the selected model, such as `"clementine"` or `"astra"`. | No | `"clementine"` for Coda; `"astra"` for Mist v3. |
| `Language` | Spoken language code, such as `"en"` or `"es"`; the service validates support. | No | `"en"` when empty. |
| `AudioFormat` | Output encoding: `rime.PCM`, `rime.WAV`, `rime.MP3`, `rime.PCM24000`, or `rime.MULAW8000`. | No | Raw PCM16 at 24 kHz. |
| `SampleRate` | Requested output rate in Hz: `8000`, `16000`, or `24000`; zero selects the format's default. | No | `24000`; `8000` for `MULAW8000`. |
| `TimeScaleFactor` | `*float64` duration multiplier from `0.4` to `2.5`; below `1` speeds speech up, above `1` slows it down. | No | `nil`; normal duration (`1`). |
| `Timestamps` | `true` requests word timings; supported only with Mist v3. | No | `false` |
| `CustomLexicon` | `[]rime.PronunciationEntry` with `Spelling` and space-separated X-SAMPA `Pronunciation` strings for Coda. | No | Empty slice. |
| `Timeout` | `*time.Duration` override; a pointer to zero disables the overall timeout. Negative values are invalid. | No | `nil`; inherits the client timeout. |

Discover valid values with `client.Voices.List(ctx, rime.VoiceListOptions{Language: "en"})`
and `client.Languages.List(ctx, rime.DiscoveryOptions{})`. Both return
`([]string, error)`. Omit the voice language filter to list all voices for the
selected model. Both options structs accept `Timeout`; discovery has an
additional ten-second maximum.

### Example: save an IVR greeting

Save as `tts.go`. This writes a complete 16 kHz WAV file, ready for playback or upload.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := rime.NewClient(rime.Config{Timeout: 60 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()

	factor := 0.9
	audio, err := client.TTS.SynthesizeToBuffer(context.Background(),
		"Thank you for calling. Your appointment is confirmed for tomorrow.",
		rime.SynthesisOptions{
			AudioFormat:     rime.WAV,
			SampleRate:      16000,
			TimeScaleFactor: &factor,
		},
	)
	if err != nil {
		return err
	}
	if err := os.WriteFile("greeting.wav", audio, 0600); err != nil {
		return err
	}
	fmt.Println("Saved greeting.wav")
	return nil
}
```

Run `go run tts.go`, then open `greeting.wav` in an audio player.

### Audio and stream behavior

All output is mono. PCM is signed 16-bit little-endian audio without a header;
WAV contains PCM16; MP3 is encoded by the service. PCM/WAV rates are requested
from the server. MP3 headers determine the actual encoded rate. The fixed
`PCM24000` and `MULAW8000` presets reject conflicting sample rates; mu-law is
converted locally from 24 kHz PCM. Read `stream.SampleRate()` for the requested rate.

The buffer helper waits for successful completion and finalizes WAV lengths.
Its fixed limit is **64 MiB per call**, including headers; exceeding it returns
`ErrResourceLimit` and nil audio. WAV/MP3 stream chunks are fragments of one file.
Use the buffer helper when you need a complete WAV.

For incremental text, supply a `rime.TextSource`, a
`func(context.Context) (string, error)` that returns `io.EOF` when finished.
Send fragments as they arrive; the service combines them into sentences.
Consume streaming audio until `io.EOF` to confirm success, since an error can
follow earlier audio. Defer `stream.Close()` and `client.Close()` for cleanup.
A client supports concurrent operations; each stream supports one reader.
Sources must honor cancellation. Synthesis is not retried automatically, and
overall timeouts include pauses by the caller; connection and progress limits
also apply. A caller context can impose an earlier deadline.

For timestamps, enable `Timestamps: true`, consume audio through `io.EOF`, then
call `stream.Timestamps()`. The result has `Status` and `Spans` with word text and
start/end seconds. Nonzero alignment status does not fail the audio. The buffer
helper returns only bytes, so use an audio stream to access timings. Custom
lexicons apply to the whole request; the service validates pronunciations and
model/language support.

## STT

### Append-fragment streaming

Both contracts use gRPC. The default returns replacement partials. Append mode
returns permanent deltas from independent five-second audio windows, useful for
long recordings or consumers that cannot replace text. Append each delta exactly,
including its newline separators. The final transcript equals their concatenation
and is emitted only after `done` and successful gRPC completion. Silence can
produce an empty final with no deltas. Cancellation and failures never produce a
successful final. `Transcribe` is unchanged and takes no output contract.

```go
stream, err := client.STT.Stream(ctx, source, rime.TranscriptionOptions{
    Language: "en", OutputContract: rime.TranscriptionAppendFragments,
})
// Handle rime.TranscriptionDelta by appending Text exactly.
// Handle rime.TranscriptionPartial by replacing text (default contract).
// Handle rime.TranscriptionFinal as the confirmed complete transcript.
```

The SDK caps cumulative transcript text at 1 MiB. Send recordings faster than
real time and split them to respect the service's 16 MiB audio and 120-second
call limits. Append mode does not remove those limits. Live voice agents usually
prefer revised hypotheses for provisional text and cross-window corrections.



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

```go
audio, err := os.ReadFile("utterance.wav")
if err != nil {
    return err
}
result, err := client.STT.Transcribe(ctx, audio, rime.TranscribeOptions{
    Language: "en",
    Mode: rime.TranscriptionWritten,
    ContextTerms: []string{"Super-G", "USOC"},
})
if err != nil {
    return err
}
fmt.Println(result.Text, result.Language, result.RequestID)
```

This fragment assumes an existing client and context, inside a function returning
`error`. Cancel the context or close the client to cancel the request. The
optional `TranscribeOptions.Timeout` is an overall `*time.Duration`; nil or zero
disables that budget. Keep the audio slice unchanged until the call returns.

### Stream an utterance

Use `client.STT.Stream(ctx, source, options)` to transcribe one utterance.
The required `rime.AudioSource` is a `func(context.Context) ([]byte, error)`
supplying headerless signed PCM16 little-endian audio. Return `io.EOF` to end
the utterance; bytes returned with `io.EOF` are sent first. Your application
decides when to stop.

STT uses gRPC over TLS to `stt.api.rime.ai:443`, with the client's API
key. There is no STT model, endpoint, or protocol selector. TTS client settings,
including its timeout, do not apply to STT.

### Transcription options

Pass these fields in `rime.TranscriptionOptions`.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Language` | Spoken BCP-47 language tag, such as `"en"` or `"es"`; passed to the service. | Yes | None; no automatic detection. |
| `Mode` | `rime.TranscriptionWritten` normalizes written text; `rime.TranscriptionVerbatim` prefers spoken wording. | No | `TranscriptionWritten` when empty. |
| `ContextTerms` | `[]string` recognition hints, such as `[]string{"Rime"}`; hints do not force a match. | No | Empty slice. |
| `InputFormat` | `rime.PCMFormat{SampleRate: 16000, Channels: 1}`: rates `8000`, `16000`, `24000`, `48000`; channels `1` or `2`. | No | 16 kHz mono PCM16; zero fields select these defaults. |
| `Timeout` | `*time.Duration` overall budget, including source/consumer pauses; `nil` or a pointer to zero disables it. Negative values are invalid. | No | `nil`; independent of the client timeout. |

### Example: transcribe a recording

Save a short recording as `utterance.pcm`: **16 kHz, mono, signed PCM16
little-endian, with no WAV header**. Save this as `stt.go`.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	file, err := os.Open("utterance.pcm")
	if err != nil {
		return err
	}
	defer file.Close()
	client, err := rime.NewClient(rime.Config{})
	if err != nil {
		return err
	}
	defer client.Close()

	buffer := make([]byte, 3200)
	source := func(ctx context.Context) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := file.Read(buffer)
		return buffer[:n], err
	}
	budget := 120 * time.Second
	transcript, err := client.STT.Stream(context.Background(), source,
		rime.TranscriptionOptions{Language: "en", Timeout: &budget})
	if err != nil {
		return err
	}
	defer transcript.Close()
	for {
		update, err := transcript.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch value := update.(type) {
		case rime.TranscriptionPartial:
			fmt.Println("partial:", value.Text)
		case rime.TranscriptionFinal:
			fmt.Println("final:", value.Text)
		}
	}
}
```

Run `go run stt.go`. The TTS and STT examples are separate programs; run each
by filename. Each partial update replaces the **complete current transcript**;
do not append snapshots. A final update confirms successful completion and may
be empty for silence. Errors do not turn partials into finals.

The SDK converts supported input formats to 16 kHz mono. Stereo must be
interleaved. WAV headers, MP3, and mu-law input are not accepted. Chunks may split
sample frames, but an incomplete final frame fails the request. The SDK does not
provide microphone capture, automatic end-of-speech detection, word timings, or
diarization. Recognition is not retried automatically.

The first `Recv()` starts network work and the overall timeout. Source calls are
serial; returned bytes must remain unchanged until the next call. Cancel the
context or close the stream to stop one utterance. Closing the client cancels
all its operations. Sources must honor their context; the SDK cannot stop a
blocked source function. Connection, acceptance, and completion limits still
apply when the overall timeout is disabled.

## Help and licenses

See the [examples](https://github.com/rimelabs/rime-sdk/tree/main/go/examples)
for streaming text, transcription, and a microphone conversation loop.
Visit [Rime documentation](https://docs.rime.ai/) for service information, or
report SDK issues on [GitHub](https://github.com/rimelabs/rime-sdk/issues).
Use `errors.Is` to check categories such as `rime.ErrTimeout`, and `errors.As`
with `*rime.Error` to read `Message` and `RequestID`. Include the SDK version and
request ID when reporting a failure; omit API keys.

SDK code uses the [MIT license](LICENSE).
Prism adaptations retain Apache-2.0 attribution in `NOTICE-PRISM` and `LICENSE-PRISM`.

## Prism realtime

If Prism refuses the connection with HTTP 429, `Realtime.Connect` returns
an error matching `ErrResourceLimit`. HTTP 503 returns `ErrUnavailable`.
Handshake errors include the HTTP status, the server request ID when supplied,
and a short server explanation when available. Missing or invalid error bodies
use a default message. The SDK does not retry the connection automatically.

Prism is available from `v0.1.0-alpha.11`. Request IDs, reply labels, session updates,
`RealtimeFault.Retryable()`, and `ErrRealtime` are pending the next release.
Use this checkout for those APIs.

When you upgrade from alpha.11, check `errors.Is(err, rime.ErrRealtime)` for
service faults. Connection and protocol failures still use `ErrStream`.
Read the fault's `Scope` to see whether the session ended.

This excerpt belongs inside a function that returns `error`. It assumes an
existing `ctx` and the `rime` package import. See the
[typed example](https://github.com/rimelabs/rime-sdk/blob/main/go/examples/realtime/typed/main.go)
for a complete program with an event consumer and a timeout.

```go
client, err := rime.NewClient(rime.Config{})
if err != nil { return err }
defer client.Close()
session, err := client.Realtime.Connect(ctx, rime.RealtimeConnectOptions{})
if err != nil { return err }
defer session.Close()
// Read session.Recv(ctx) concurrently with requests.
if _, err := session.SendText(ctx, "Hello"); err != nil { return err }
```

`RealtimeSession` owns authentication, initialization, request matching, events,
audio conversion, and cleanup. Prism defaults to
`wss://prism.api.rime.ai/v1/realtime`. TTS model, voice, endpoint, and timeout
settings do not apply. `RealtimeConnectOptions` selects the endpoint, voice,
instructions, tools, interruption, turn detection, and timeouts. Plain `ws://`
is permitted only for localhost. Zero timeouts select 10 seconds for connection,
30 seconds for admission, and 10 seconds for writes and acknowledgments.

Go uses the default HTTP transport for Prism. It honors `HTTPS_PROXY` and
`NO_PROXY` for `wss://` connections. Set these variables before the first
connection; Go caches the proxy environment. The transport bypasses proxies
for localhost and loopback addresses.

`TurnDetection` accepts `low`, `medium`, `high`, or `auto` eagerness and a silence
limit of 1000 to 10000 milliseconds. `PrismFiller` selects `none`, `text`, or
`auto` speech while a tool runs. Filler text and filler instructions must be
valid, nonblank UTF-8 with at most 500 Unicode scalar values. Omit these settings
to use server defaults. `session.Info()` returns a copy of the effective published
settings.

### Update session settings

```go
instructions := "Handle billing. Use only the supplied tools."
info, err := session.Update(ctx, rime.RealtimeUpdateOptions{
    Instructions: &instructions,
    Tools: []rime.ToolDefinition{refundTool},
})
if err != nil {
    return err
}
fmt.Println(info.SettingsRevision)
```

`Instructions`, `Voice`, and `InterruptOnSpeech` are pointers; `nil` keeps the
setting. A `nil` `Tools` slice keeps the tools. A non-nil empty slice clears them.

Omitted fields keep their values. A supplied tool list replaces the complete
list. Empty instructions clear the prompt; an empty tool list removes all tools.
An empty voice selects the deployment default. Instructions and tools supplied
together change in one request.

The method waits for the matching `session.updated` acknowledgment and returns
session information. The stored session information changes only after that
acknowledgment. A rejected update leaves it unchanged and keeps the connection
open. Cancellation after submission does not undo an accepted update.

Updates do not wait for turn readiness or tool results. Each reply keeps the
settings it began with. A reply prepared on an early turn-end guess can be
announced after an update but still use the previous settings revision.
Modalities, eagerness, and the end-of-turn timeout cannot change mid-session.

`SessionInfo`, `ResponseStarted`, and `ResponseEnded` expose `SettingsRevision`
as `*uint64`. It is `nil` when the server does not publish it. Users do not need
to manage it. Use each reply's revision to identify its settings.

The conversation runner captures each reply's interruption policy from its own
settings revision. On confirmed speech, the newest reply still generating or
awaiting a measured playback receipt controls interruption of the playback batch.
A later update does not change that policy, even if the server has finished its
estimated playback before the device finishes playing.

The SDK retains the latest 128 interruption policy changes. Prompt-only and
tool-only updates do not consume this history. A newly announced reply that
references an older, evicted policy fails with `ErrResourceLimit`. Replies whose
policy was already captured keep it while queued or playing. Servers that omit
revisions use the acknowledged setting in effect when the reply arrives.

If an interruption setting changes without a published revision, later reply
revisions can be ambiguous. The SDK preserves earlier known settings and fails
with a resource limit error for an ambiguous reply. A later acknowledgment with
a revision establishes a new known setting. Replies without a revision use the
current acknowledged setting.

`RequestID` is sent as the upgrade's `x-request-id`. It must be nonblank
printable ASCII, or `Connect` returns `ErrInput`. Prism replaces IDs longer than
128 characters. A gateway can replace shorter IDs too. `Info().RequestID`
reports the ID Prism actually used. Include it when
you report a problem; an event's or fault's `RequestID` names the SDK request that
caused it instead. `ResponseMetadata` labels every reply, such as `{"step": "3"}`: up to 16
strings, keys up to 64 characters and values up to 512, no key starting with
`prism_`. Prism refuses labels that break these rules with a `*RealtimeFault`
whose code is `invalid_session`. `SetResponseMetadata` replaces these labels. `ResponseStarted.Metadata` carries them, and
`ResponseStarted.InputItemIDs` names the user items a reply answers.
`ResponseStarted.InstructionItemID` names a saved nudge instruction. Pass it to
`DeleteItem` to remove the instruction from later history.
Set `ReplyOptions.Metadata` to label one proactive reply. For a typed reply or
tool continuation, use `SendTextWithOptions` or `ContinueReplyWithOptions` and
pass `ReplyRequestOptions{Metadata: map[string]string{"step": "3"}}`.
Prism applies request labels over matching session labels. The SDK adds its
`prism_` fields; caller keys cannot use that prefix. Existing methods keep
their request shape, and replies to speech use session labels.

| Session method | Behavior |
| --- | --- |
| `Update` | Apply supplied settings and return acknowledged `SessionInfo`. |
| `SendText` | Wait for admission, add a typed turn, and return its response reference. |
| `SendTextWithOptions` | Send a typed turn with labels for this reply. |
| `AddMessage` | Add user or assistant history without a reply. |
| `RequestReply` | Request a proactive reply, with optional instructions or a recorded tool call. |
| `SubmitToolResult` | Record an application result. The same result can be submitted again; a changed result fails. |
| `ContinueReply` | Wait for the latest tool response and all its results, then request continuation. |
| `ContinueReplyWithOptions` | Continue a tool round with labels for this reply. |
| `Cancel` | Cancel a retained response. |
| `SetResponseMetadata` | Replace the labels for later replies once Prism confirms. |
| `SendAudio` / `ClearAudio` | Convert input PCM or clear the input buffer. |
| `ReportPlayback` | Report measured device playback completion or interruption. |
| `Recv` | Read one typed event; normal closure returns `io.EOF`. |
| `Close` | Stop the session and wait for cleanup. |

Use a type switch on `SessionEvent.Payload`, such as `TextDelta`, `ToolCall`,
`SpeechStarted`, or `ResponseEnded`. There is one event consumer. Read events
while requests wait. Every operation has its own context; the context passed to
`Connect` controls the session lifetime. Closing the parent client stops its
sessions. `Close` is safe to repeat and waits for cleanup.
If connection cancellation is already observable when initialization succeeds,
`Connect` closes the session and returns the context error. The context continues
to own the session after a successful return.

Cancellation before submission sends nothing. After submission, the worker keeps
acknowledgment matching even if the caller stops waiting. It cancels a reply
accepted after caller cancellation. A write or acknowledgment with an unknown
outcome closes the session. There is no automatic reconnect or replay. The SDK
retries turn admission only after an explicit busy refusal and a new readiness
event. Service faults use `ErrRealtime`. Use `errors.As` with `*RealtimeFault`
for service code, scope, owners, and correlation details. A refusal can leave
the session usable; a fault with session scope ends it. Connection and protocol
failures use `ErrStream` and have no realtime fault.

`RealtimeFault.Retryable()` reports whether the same request, unchanged, may
succeed on another attempt. Call it on `FaultEvent.Fault` or the fault obtained
through `errors.As`. The SDK derives this value from the code; Prism does not
send a retry flag.

| Error code | `Retryable()` |
| --- | --- |
| `capacity_exceeded` | `true` |
| `unavailable` | `true` |
| `response_timeout` | `true` |
| `context_length_exceeded` | `false` |
| All other codes, including unknown codes | `false` |

The application chooses the delay, attempt limit, and recovery operation.
`Retryable()` does not start a retry or describe a local `ErrTimeout`. It does
not prove that no output or side effect occurred. The SDK's existing readiness
retries have separate rules: `typed_turn_busy`, for example, returns `false`
here but permits another attempt after a new readiness event.

For example, Prism can accept a user's question and then report
`capacity_exceeded`. `SendText` has already returned a `ResponseRef`; the failure
arrives through `FaultEvent`. Wait for `ResponseEnded`, then apply the
application's retry policy. If the question is already in history, use
`RequestReply` to ask for another reply when appropriate. Repeating `SendText`
can add the question twice. A `response_timeout` can follow partial text or
audio, which the application must handle before another attempt.

For `context_length_exceeded`, the server message contains token counts.
Unchanged attempts will fail until the conversation is shorter. Start a new
session with shorter context. Use the history deletion method to remove items before retrying.

Numbers in `ToolCall.Arguments`, including nested values, use `json.Number` to
preserve precision. Use `String`, `Int64`, or `Float64` to read the required form.

Voice and session instructions must be nonblank at connection setup. Updates accept empty strings. The SDK imposes
no length cap on these settings, consistent with the shared Prism schema and
the Python and TypeScript SDKs. This does not guarantee that the service accepts
every value.

Canceling an active audio submission closes the session. Canceling queued audio
sends nothing and permits other operations to continue. Terminal failures take
precedence over queued events and remain available from later session operations.
Normal closure returns `io.EOF` after the remaining queued events.

Text turns and history contain at most 4000 Unicode scalar values. Tool filler
text and filler instructions contain at most 500 Unicode scalar values. Input audio
contains complete PCM16 frames and at most 192000 bytes per chunk. Input supports
mono or stereo at 8, 16, 24, or 48 kHz. The session sends mono 16 kHz PCM and
receives mono 24 kHz PCM. A clear or input-format change resets conversion.
There are at most 128 pending requests, 128 retained responses, 128 tools or
outputs per response, and 256 queued events. Incoming messages are limited to
1 MiB. Expired references fail with `ErrInput`; a full event queue fails with
`ErrResourceLimit`. Known malformed events fail the session. Unknown events and
fields are ignored; unknown enum values become `unknown`.

### Conversation runner and examples

`ConversationRunner` accepts application-owned `ConversationSource` and
`ConversationPlayback` adapters. It consumes events, sends capture audio,
preserves playback order, interrupts selected outputs, rejects late audio, and
reports device measurements. It cancels dependent responses that arrive after
interruption. Applications execute tools in their own goroutines. `OnEvent`
runs inline and must not block. Do not call `Recv` or send playback reports
elsewhere while the runner runs.

Each output has an independent completion receipt. Report actual played
milliseconds, including partial playback after interruption. Receiving or
queueing PCM does not confirm playback. `Interrupt` discards selected outputs
as a batch, then resumes the remaining FIFO queue. `Stop` confirms that all
playback has stopped and keeps caller resources open. `Read`, `Completion`, and
`Stop` must honor their context. `Open`, `Write`, `End`, and `Interrupt` have no
context and must return without blocking. The runner passes a five-second context
to `Stop` and waits for cancellation cleanup. This is a cooperative deadline,
not a hard limit on `Run`: an adapter that ignores cancellation can prevent it
from returning.
The SDK cannot forcibly stop an adapter goroutine or claim that its device has
stopped. It stops at source EOF, session closure,
failure, or cancellation, then closes the session.

`Completion` starts when an output opens. It can remain pending during `Write`,
`End`, `Interrupt`, and `Stop`. Adapters must permit these calls to overlap;
do not hold a shared lock while waiting for playback completion. The runner
serializes `Open`, `Interrupt`, and `Stop`, while source reads run independently.

```sh
go run ./examples/realtime/typed
go run ./examples/realtime/recorded utterance.pcm
go run ./examples/realtime/tools
go run ./examples/realtime/live 127.0.0.1:8766
```

Set `RIME_API_KEY` before running these examples. Set `PRISM_URL` to override
the default Prism endpoint, for example when using a local test server.

The live example uses an application-owned audio bridge with no device library
dependency. The local TCP connection carries newline-delimited JSON, at most
1 MiB per line. Send `{"type":"audio","data":"BASE64"}` for complete 16 kHz
mono PCM16 capture chunks. The example sends `open`, `audio`, and `end` with an
opaque output `id`; output audio is 24 kHz mono PCM16. Play outputs in `open`
order. For `interrupt`, discard outputs in `ids` as one batch. Send
`{"type":"played","id":"ID","played_ms":12.5}` when an output finishes or is
interrupted, using device measurements. Reply to `stop` with
`{"type":"stopped"}` only after all playback stops. Send `{"type":"eof"}` to
end capture, and keep reading until the stop handshake completes. This bridge
protocol belongs to the example, not the SDK.

## Conversation history operations

Available in the next SDK release. The Prism server must support history operations.
Read history to obtain the items the next reply would see, in server order.
Create messages with user, assistant, or system roles, past function calls, and
function results. Creating an item does not start a reply or execute a tool.
Message text is limited to 4000 Unicode characters. Optional client item IDs have
1 to 32 characters. Tool arguments and results preserve the supplied strings.

Pass an empty string as the insertion anchor to append, use `root` to insert
first, or give an item ID to insert after its whole turn. Results go after their
calls and require an empty anchor. Add a past call before adding its result.

Deletion hides items from subsequent replies and keeps the conversation record.
Deleting a call or its result hides both. An active reply keeps its original
context. A deleted active call still needs a result and continuation through the
existing tool APIs. Deleting an already deleted item succeeds; an unknown ID fails.
History excludes replies still running and their inputs until those replies end.
Requests work without typed-turn readiness. A canceled submitted request can still
take effect; the SDK tracks its acknowledgment and does not replay it.

```text
item, err := session.CreateItem(ctx, rime.HistoryItem{Type: rime.HistoryMessage, Role: "system", Text: "Transferred to billing."}, "root")
items, err := session.GetHistory(ctx)
err = session.DeleteItem(ctx, item.ItemID())
```

## History truncation

Available in the next SDK release. Set `truncation` at connection setup or during
an existing session. Omit it to preserve compatibility with older Prism servers.
The server must support truncation before you send this setting.

`auto` drops the oldest turns and retains 0.8 of the available history capacity.
`disabled` keeps all history and reports `context_length_exceeded` when it no
longer fits. A retention ratio above zero and at most one selects how much history
capacity to retain. Instructions and tools remain. This setting does not delete
items from the conversation record. Session information reports the acknowledged
policy, or an absent value when an older server does not publish it.

```text
session.Update(ctx, rime.RealtimeUpdateOptions{Truncation: &rime.Truncation{Mode: rime.TruncationDisabled}})
```

## Retained text after interruption

Available in the next SDK release. `ItemTruncated` appears in `SessionEvent.Payload`
through `Recv` and the conversation runner's `OnEvent` callback. Playback reporting
does not wait for event consumption. It carries `ItemID`, `ContentIndex`,
`AudioEndMS`, and `KeptText`.

`KeptText` points to the text retained in server history. An empty string means
no words remain. A nil pointer means unavailable, including older servers and
replies still being generated. The event does not promise a later text update.
Replies with tool calls retain all words, so this text can differ from the words
heard during playback. The event can arrive after `ResponseEnded` and does not
require a retained response reference. `TextDone` still reports generated text.

In your event handler:

```go
if cut, ok := event.Payload.(rime.ItemTruncated); ok && cut.KeptText != nil {
    fmt.Printf("Retained text for %s: %q\n", cut.ItemID, *cut.KeptText)
}
```

## Response usage and idle sessions

`ResponseEnded.Usage` holds input, output, and total token counts and the
context window when Prism counted the reply. It is nil when no count is
available. An idle session ends with `ErrTimeout`; use `errors.As` with
`*RealtimeIdleTimeout` to read `IdleTimeoutS`.

## History excluded from a reply

Available in the next SDK release. `ResponseEnded.Truncation` is an optional
`*ResponseTruncation` with `ItemsLeftOut` and optional `*string` `FirstKeptItemID`.
It exposes `response.prism_truncation` through `Recv` and `OnEvent`.
The count refers to items excluded from this reply's prompt, not turns or items
deleted from history. Already hidden items do not count. A nil first ID means
no item from before this turn was retained.

A nil summary means the server reports no exclusions or the information is
unavailable, including older servers or a reply that ended before usage was counted.
The summary does not change the session's truncation policy.

```go
if ended, ok := event.Payload.(rime.ResponseEnded); ok && ended.Truncation != nil {
    fmt.Printf("Items excluded from prompt: %d\n", ended.Truncation.ItemsLeftOut)
}
```

Migration: use keyed `ResponseEnded` struct literals. Existing unkeyed literals
must include the new `Truncation` field, with nil when no summary is available.
