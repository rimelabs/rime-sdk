# Rime SDK for Rust

Generate speech with Coda or Mist v3, transcribe audio with STT, and run Prism conversations
through the async `Client`. Your application handles audio capture and playback.

## Installation

Requires **Rust 1.88 or later** and a Tokio runtime with I/O and time enabled.
Install the alpha crate from crates.io. For a new project:

```sh
cargo new rime-quickstart
cd rime-quickstart
export RIME_API_KEY="your-api-key"
```

Add these dependencies to `Cargo.toml`. Tokio's `fs` and `io-util` features,
`bytes`, and `futures-util` support the examples below.

```toml
[dependencies]
rimelabs-sdk = "0.1.0-alpha.5"
bytes = "1"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "time", "fs", "io-util"] }
```

The STT streaming examples below use the unreleased `TranscriptionOutputContract`
and `TranscriptionUpdate::Delta` APIs. Published crates through `0.1.0-alpha.7`
do not contain them. Until append support is released, run the example from this
source checkout's `rust/` directory:

```sh
cargo run --example transcribe -- utterance.pcm en append_fragments
```

The installation requirement above must be raised to the first release containing
these APIs when that release is prepared; it does not currently cover these STT
streaming examples.

Get an API key from the [Rime dashboard](https://app.rime.ai/). A key is required:
`Client::builder().build()` reads `RIME_API_KEY`, or you can use
`Client::builder().api_key("...").build()` to override it. The SDK does not load
`.env` files. Shell commands below assume macOS or Linux.

## TTS

Use `client.tts().synthesize(text, options)` for a complete string, or
`client.tts().stream(source, options)` for incremental text. Both return
`Result<AudioStream, Error>` immediately; do not await these method calls.
Read audio with `StreamExt::next().await`. For complete audio in memory, await
`client.tts().synthesize_to_buffer(text, options)` for `Result<Vec<u8>, Error>`.
Text is required and must contain non-whitespace characters.

### Client options

These optional `Client::builder()` methods configure TTS. They do not configure STT.

| Builder method | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `.model(model)` | Select `Model::Coda` or `Model::MistV3`. | No | `Model::Coda` |
| `.endpoint(host)` | Override the TTS TLS hostname and optional port, such as `"coda.api.rime.ai:443"`; no scheme or path. | No | `coda.api.rime.ai:443` for Coda; `mist.api.rime.ai:443` for Mist v3. |
| `.timeout(duration)` | Overall TTS timeout as a positive `Duration`, such as `Duration::from_secs(60)`; zero is invalid. | No | No overall timeout. |

### Synthesis options

All three TTS methods accept `SynthesisOptions::default()` with these optional
builders. Import `AudioFormat`, `Model`, and `PronunciationEntry` from
`rimelabs_sdk` as needed, and `Duration` from `std::time`.

| Builder method | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `.voice(name)` | Voice name for the selected model, such as `"clementine"` or `"astra"`. | No | `"clementine"` for Coda; `"astra"` for Mist v3. |
| `.language(code)` | Spoken language code, such as `"en"` or `"es"`; the service validates support. | No | `"en"` |
| `.audio_format(format)` | Output encoding: `AudioFormat::Pcm`, `Wav`, `Mp3`, `Pcm24000`, or `Mulaw8000`. | No | Raw PCM16 at 24 kHz. |
| `.sample_rate(rate)` | Requested output rate in Hz: `8000`, `16000`, or `24000`. | No | `24000`; `8000` for `Mulaw8000`. |
| `.time_scale_factor(factor)` | `f32` duration multiplier from `0.4` to `2.5`; below `1` speeds speech up, above `1` slows it down. | No | Omitted; normal duration (`1`). |
| `.timestamps(enabled)` | `true` requests word timings; supported only with Mist v3. | No | `false` |
| `.custom_lexicon(entries)` | `Vec<PronunciationEntry>` for Coda, created with `PronunciationEntry::new(spelling, pronunciation)`; pronunciations use space-separated X-SAMPA. | No | Empty vector. |
| `.timeout(budget)` | `Some(Duration)` overrides the overall timeout; `None` disables it. Zero is invalid. | No | Inherits the client timeout. |

Discover valid values with `client.voices(Some("en")).await` and
`client.languages().await`. Both return `Result<Vec<String>, Error>`. Use
`client.voices(None)` to list all voices for the selected model. Discovery uses
the client timeout with an additional ten-second maximum.

### Example: save an IVR greeting

Replace `src/main.rs` with this example. It writes a complete 16 kHz WAV file,
ready for playback or upload.

```rust,no_run
use rimelabs_sdk::{AudioFormat, Client, SynthesisOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;
    let result = client
        .tts()
        .synthesize_to_buffer(
            "Thank you for calling. Your appointment is confirmed for tomorrow.",
            SynthesisOptions::default()
                .audio_format(AudioFormat::Wav)
                .sample_rate(16000)
                .time_scale_factor(0.9),
        )
        .await;
    client.close().await;
    tokio::fs::write("greeting.wav", result?).await?;
    println!("Saved greeting.wav");
    Ok(())
}
```

Run `cargo run`, then open `greeting.wav` in an audio player.

### Audio and stream behavior

All output is mono. PCM is signed 16-bit little-endian audio without a header;
WAV contains PCM16; MP3 is encoded by the service. PCM/WAV rates are requested
from the server. MP3 headers determine the actual encoded rate. The fixed
`Pcm24000` and `Mulaw8000` presets reject conflicting sample rates; mu-law is
converted locally from 24 kHz PCM. Read `audio.sample_rate()` for the requested rate.

The buffer helper waits for successful completion and finalizes WAV lengths.
Its fixed limit is **64 MiB per call**, including headers; exceeding it returns
`ErrorKind::ResourceLimit` without partial audio. WAV/MP3 stream chunks are
fragments of one file. Use the buffer helper when you need a complete WAV.

For incremental text, supply a `Stream<Item = Result<String, Error>> + Send + 'static`.
Send fragments as they arrive; the service combines them into sentences.
Consume streaming audio through the final `None` to confirm success, since an
error can follow earlier audio. Dropping a stream requests cancellation;
`audio.close().await` also waits for cleanup. `client.close().await` cancels
operations on all client clones. Keep the Tokio runtime alive during cleanup.
Synthesis is not retried automatically, and overall timeouts include pauses by
the caller; connection and progress limits also apply.

For timestamps, enable `.timestamps(true)`, consume audio through the final
`None`, then call the synchronous `audio.timestamps()`. The result has `status`
and `spans` with word text and start/end seconds. Nonzero alignment status does
not fail the audio. The buffer helper returns only bytes, so use an audio stream
to access timings. Custom lexicons apply to the whole request; the options own
their entries, and the service validates pronunciations and model/language support.

## STT

### Append-fragment streaming

Both contracts use gRPC. The default returns replacement partials. Append mode
returns permanent deltas from independent five-second audio windows, useful for
long recordings or consumers that cannot replace text. Append each delta exactly,
including its newline separators. The final transcript equals their concatenation
and is emitted only after `done` and successful gRPC completion. Silence can
produce an empty final with no deltas. Cancellation and failures never produce a
successful final. `Transcribe` is unchanged and takes no output contract.

```rust
use rimelabs_sdk::{TranscriptionOptions, TranscriptionOutputContract};
let options = TranscriptionOptions::new("en")
    .output_contract(TranscriptionOutputContract::AppendFragments);
// Pass options to client.stt().stream(source, options).
// Append TranscriptionUpdate::Delta { text } exactly.
```

When upgrading, exhaustive matches on `TranscriptionUpdate` must include the
`Delta` variant, even if the application keeps the default revised contract.

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

```no_run
use rimelabs_sdk::{Client, TranscribeOptions, TranscriptionMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder().build()?;
let audio = tokio::fs::read("utterance.wav").await?;
let result = client.stt().transcribe(
    audio,
    TranscribeOptions::new("en")
        .mode(TranscriptionMode::Written)
        .context_terms(vec!["Super-G".into(), "USOC".into()])
        .timeout(Some(std::time::Duration::from_secs(120))),
).await?;
println!("{} ({}) {:?}", result.text, result.language, result.request_id);
client.close().await;
Ok(())
}
```

Dropping the future or closing the client cancels the request. The optional
`TranscribeOptions::timeout` is an overall budget; `None` disables it.

### Stream an utterance

Use `client.stt().stream(source, options)` to transcribe one utterance.
The required source is a `Stream<Item = Result<Bytes, Error>> + Send + 'static`
of headerless signed PCM16 little-endian audio. Ending the source ends the
utterance; your application decides when to stop.

STT uses gRPC over TLS to `stt.api.rime.ai:443`, with the client's API
key. There is no STT model, endpoint, or protocol selector. TTS client settings,
including its timeout, do not apply to STT.

### Transcription options

Start with `TranscriptionOptions::new(language)`, then use the optional builders.
Import `PcmFormat`, `TranscriptionMode`, and `TranscriptionOptions` from `rimelabs_sdk`.

| Constructor or builder | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `TranscriptionOptions::new(language)` | Spoken BCP-47 language tag, such as `"en"` or `"es"`; passed to the service. | Yes | None; no automatic detection. |
| `.mode(mode)` | `TranscriptionMode::Written` normalizes written text; `Verbatim` prefers spoken wording. | No | `Written` |
| `.context_terms(terms)` | `Vec<String>` recognition hints, such as `vec!["Rime".into()]`; hints do not force a match. | No | Empty vector. |
| `.input_format(format)` | `PcmFormat { sample_rate: 16000, channels: 1 }`: rates `8000`, `16000`, `24000`, `48000`; channels `1` or `2`. | No | `PcmFormat::default()`: 16 kHz mono PCM16. |
| `.timeout(budget)` | `Some(Duration)` overall budget, including source/consumer pauses; `None` disables it. Zero is invalid. | No | `None`; independent of the client timeout. |

### Example: transcribe a recording

Save a short recording as `utterance.pcm`: **16 kHz, mono, signed PCM16
little-endian, with no WAV header**. Replace `src/main.rs` with this example.

```rust,no_run
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use rimelabs_sdk::{Client, Error, TranscriptionOptions, TranscriptionUpdate};
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = tokio::fs::File::open("utterance.pcm").await?;
    let source = stream::try_unfold(file, |mut file| async move {
        let mut data = vec![0; 3200];
        let count = file.read(&mut data).await.map_err(Error::input)?;
        if count == 0 {
            return Ok(None);
        }
        data.truncate(count);
        Ok(Some((Bytes::from(data), file)))
    });
    let client = Client::builder().build()?;
    let mut transcript = client.stt().stream(
        source,
        TranscriptionOptions::new("en").timeout(Some(Duration::from_secs(120))),
    )?;
    let result = async {
        while let Some(update) = transcript.next().await {
            match update? {
                TranscriptionUpdate::Delta { text } => println!("delta: {text}"),
                TranscriptionUpdate::Partial { text } => println!("partial: {text}"),
                TranscriptionUpdate::Final { text, language } => {
                    println!("final ({language}): {text}")
                }
            }
        }
        Ok::<(), Error>(())
    }
    .await;
    client.close().await;
    result?;
    Ok(())
}
```

Run `cargo run`. Each partial update replaces the **complete current
transcript**; do not append snapshots. A final update confirms successful
completion and may be empty for silence. Errors do not turn partials into finals.

The SDK converts supported input formats to 16 kHz mono. Stereo must be
interleaved. WAV headers, MP3, and mu-law input are not accepted. Chunks may split
sample frames, but an incomplete final frame fails the request. The SDK does not
provide microphone capture, automatic end-of-speech detection, word timings, or
diarization. Recognition is not retried automatically.

The first poll starts network work and the overall timeout. Audio sources must
yield without blocking Tokio; use `Error::input(error)` for source failures.
Dropping the transcript or calling `transcript.cancel()` requests cancellation;
`transcript.close().await` also waits for cleanup. Closing the client cancels all
its operations. Connection, acceptance, and completion limits still apply when
the overall timeout is disabled.

## Help and licenses

See the [examples](https://github.com/rimelabs/rime-sdk/tree/main/rust/examples)
for streaming text and transcription. Visit [Rime documentation](https://docs.rime.ai/)
for service information, or report SDK issues on
[GitHub](https://github.com/rimelabs/rime-sdk/issues). Use `error.kind()` to check
categories such as `ErrorKind::Timeout`, and `error.request_id()` to identify a
request. Include the SDK version and request ID when reporting a failure; omit API keys.

SDK code uses the [MIT license](https://github.com/rimelabs/rime-sdk/blob/main/rust/LICENSE).
Prism adaptations retain Apache-2.0 attribution in `NOTICE-PRISM` and `LICENSE-PRISM`.

## Prism realtime

Realtime support is unreleased. Run these examples from this checkout's `rust/`
directory until the first release that contains `realtime`.

```rust,no_run
use futures_util::StreamExt;
use rimelabs_sdk::{Client, realtime::{RealtimeConnectOptions, RealtimeEvent}};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder().build()?;
let session = client.realtime().connect(RealtimeConnectOptions::default()).await?;
let mut events = session.events()?;
let reader = tokio::spawn(async move {
    while let Some(event) = events.next().await {
        match event?.payload {
            RealtimeEvent::TextDelta { delta, .. } => print!("{delta}"),
            RealtimeEvent::ResponseEnded { .. } => break,
            _ => {},
        }
    }
    Ok::<_, rimelabs_sdk::Error>(())
});
let reply = session.send_text("Hello").await;
if reply.is_err() { session.close().await; }
let received = reader.await?;
client.close().await;
reply?;
received?;
# Ok(()) }
```

`RealtimeSession` owns authentication, initialization, request matching, events,
audio conversion, and cleanup. Prism defaults to
`wss://prism.api.rime.ai/v1/realtime`. TTS model, voice, endpoint, and timeout
settings do not apply. Use `RealtimeConnectOptions` builders to select a voice,
instructions, tools, interruption, turn detection, and timeouts. Plain `ws://`
is permitted only for localhost. Connection, admission, and request timeouts
default to 10, 30, and 10 seconds.

`TurnDetection` accepts `low`, `medium`, `high`, or `auto` eagerness and a silence
limit of 1000 to 10000 milliseconds. `PrismFiller` selects no speech, fixed text,
or generated speech while a tool runs. Filler text and instructions must be
nonblank and have at most 500 Unicode scalar values. Omit these settings to use
server defaults. `session.info()` returns the effective settings that Prism
published.

`request_id` is sent as the upgrade's `x-request-id`. It must be printable
ASCII, or `connect` returns an input error. Prism uses it if it is at most 128
characters, else makes its own; `info().request_id` reports the one it used.
Include it when you report a problem; an event's or fault's `request_id` is the
SDK's event ID, not this one. `response_metadata` labels every reply, such as
`{"step": "3"}`: up to 16 strings, keys up to 64 characters and values up to 512,
no key starting with `prism_`. Prism refuses labels that break these rules with a
realtime fault whose code is `invalid_session`. Settings apply once, except these
labels, which `set_response_metadata` replaces. `ResponseStarted`'s `metadata`
carries them, and its `input_item_ids` names the user items a reply answers.

| Session method | Behavior |
| --- | --- |
| `send_text` | Wait for admission, add a typed turn, and return its response reference. |
| `add_message` | Add user or assistant history without a reply. |
| `request_reply` | Request a proactive reply, with optional instructions or a recorded tool call. |
| `submit_tool_result` | Record an application result. The same result can be submitted again; a changed result fails. |
| `continue_reply` | Wait for the latest tool response and all its results, then request continuation. |
| `cancel` | Cancel a retained response. |
| `set_response_metadata` | Replace the labels for later replies once Prism confirms. |
| `send_audio` / `clear_audio` | Convert input PCM or clear the input buffer. |
| `report_playback` | Report measured device playback completion or interruption. |
| `events` | Take the single owned `Stream<Item = Result<SessionEvent, Error>>`. |
| `close` | Stop the session and wait for cleanup. |

Text turns and history contain at most 4000 Unicode scalar values. Audio chunks
contain complete PCM16 frames and at most 192000 bytes. Input supports mono or
stereo at 8, 16, 24, or 48 kHz. The session sends mono 16 kHz PCM and receives
mono 24 kHz PCM. Clear audio to reset the streaming converter. A change of input
format also resets it.

Session handles can be cloned. Dropping the last handle requests cleanup.
Explicit `close().await` confirms cleanup. Closing the parent client stops its
sessions. Dropping an operation future prevents submission if possible. After
submission, the worker keeps acknowledgment matching. It cancels a reply that
is accepted after its caller stops waiting. A write or acknowledgment with an
unknown outcome closes the session. The SDK does not reconnect or replay it.
The SDK retries turn admission only after an explicit busy refusal and a new
readiness event.

Voice and session instructions must be nonblank when supplied. The SDK imposes
no length cap on these settings, consistent with the shared Prism schema and
the Python and TypeScript SDKs. This does not guarantee that the service accepts
every value.

Dropping an active audio submission closes the session. Dropping queued audio
sends nothing and permits other operations to continue. Terminal failures take
precedence over queued events and remain available from later session operations.
The event stream yields the terminal error once, then ends. Normal closure drains
the remaining queued events and ends without an error.

There are at most 128 pending requests, 128 retained responses, 128 tools or
outputs per response, and 256 queued events. Incoming messages are limited to
1 MiB. An expired reference fails with `ErrorKind::Input`. A full event queue
fails with `ErrorKind::ResourceLimit`; consume events during requests. Known
malformed events fail the session. Unknown events and fields are ignored;
unknown server enum values are reported as `unknown`. Service faults retain
code, scope, owners, and correlation details through `Error::realtime_fault()`.

`RealtimeFault::retryable()` reports whether the same request, unchanged, may
succeed on another attempt. Call it on the fault in `RealtimeEvent::Fault` or
the fault returned by `Error::realtime_fault()`. The SDK derives this value from
the code; Prism does not send a retry flag.

| Error code | `retryable()` |
| --- | --- |
| `capacity_exceeded` | `true` |
| `unavailable` | `true` |
| `response_timeout` | `true` |
| `context_length_exceeded` | `false` |
| All other codes, including unknown codes | `false` |

The application chooses the delay, attempt limit, and recovery operation.
`retryable()` does not start a retry or describe a local `ErrorKind::Timeout`.
It does not prove that no output or side effect occurred. The SDK's existing
readiness retries have separate rules: `typed_turn_busy`, for example, returns
`false` here but permits another attempt after a new readiness event.

For example, Prism can accept a user's question and then report
`capacity_exceeded`. `send_text()` has already returned a `ResponseRef`; the
failure arrives through `RealtimeEvent::Fault`. Wait for
`RealtimeEvent::ResponseEnded`, then apply the application's retry policy. If
the question is already in history, use `request_reply()` to ask for another
reply when appropriate. Repeating `send_text()` can add the question twice.
A `response_timeout` can follow partial text or audio, which the application
must handle before another attempt.

For `context_length_exceeded`, the server message contains token counts.
Unchanged attempts will fail until the conversation is shorter. Start a new
session with shorter context. The protocol also supports history deletion, but
this SDK does not yet expose `conversation.item.delete`.

### Conversation runner and examples

`ConversationRunner::new(session, source, playback)` accepts application-owned
`ConversationSource` and `ConversationPlayback` adapters. The runner consumes
events, sends capture audio, preserves playback order, cuts interrupted audio,
rejects late audio, and reports device measurements. It cancels dependent
responses that arrive after interruption. Applications execute tools separately.
The event callback must return without blocking. Do not consume session events
or send playback reports elsewhere while the runner runs.

Each playback output has an independent receipt. Completion must report actual
played milliseconds, including partial playback after interruption. Receiving
or queueing PCM does not confirm playback. `interrupt` discards the selected
outputs as a batch, then resumes the remaining queue in order. `stop` confirms
that all playback has stopped and keeps application resources open. Adapter
futures must finish cancellation cleanup. The runner cancels the stop token
after five seconds and waits for adapter cleanup before returning. This is a
cooperative deadline, not a hard limit on cleanup. An adapter that ignores
cancellation can prevent cleanup from finishing. The runner
stops at source EOF, session closure, failure, or cancellation. Dropping its run
future requests shutdown through its owned task.

The owned task retains the adapter handles while it confirms playback stop and
closes the session. Keep the Tokio runtime alive until cleanup finishes. To wait
for confirmation, keep the `run` future and cancel its token, then await that
future. Dropping the future requests cleanup but does not itself wait for it.
The application retains ownership of device resources through its adapter handles.

The runner starts `completion(&self)` when an output opens. Its shared `Arc`
handle permits `write`, `end`, playback interruption, and stop while that receipt
is pending. Adapters must not hold a lock across the completion wait if those
operations need that lock. The runner serializes `open`, `interrupt`, and `stop`.

```sh
cargo run --example realtime_typed
cargo run --example realtime_recorded -- utterance.pcm
cargo run --example realtime_tools
cargo run --example realtime_live -- 127.0.0.1:8766
```

The live example connects to an application-owned audio bridge. It has no device
library dependency. The bridge uses newline-delimited JSON on local TCP, with
at most 1 MiB per line. Send `{"type":"audio","data":"BASE64"}` for complete
16 kHz mono PCM16 capture chunks. The example sends `open`, `audio`, and `end`
messages with an opaque output `id`; output audio is 24 kHz mono PCM16. Play
outputs in `open` order. For `interrupt`, discard the outputs in `ids` as one
batch. Send `{"type":"played","id":"ID","played_ms":12.5}` when each output
finishes or is interrupted, using device measurements. Reply to `stop` with
`{"type":"stopped"}` only after all playback stops. Send `{"type":"eof"}` to
end capture, and keep reading until the stop handshake completes. This bridge
protocol belongs to the example, not the SDK.
