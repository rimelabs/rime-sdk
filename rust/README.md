# Rime SDK for Rust

Asynchronous text-to-speech for Coda and Mist v3, plus streaming speech recognition.
Requires Rust 1.88 or later
and a Tokio runtime with I/O and time enabled. Realtime Prism support is not
included.

Add these dependencies to your `Cargo.toml`:

```toml
[dependencies]
rimelabs-sdk = "0.1.0-alpha.1"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "time"] }
```

Set `RIME_API_KEY`, then use this example in `src/main.rs`:

```rust,no_run
use futures_util::StreamExt;
use rimelabs_sdk::{Client, SynthesisOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?; // Reads RIME_API_KEY.
    let mut audio = client.tts().synthesize("Hello.", SynthesisOptions::default())?;
    while let Some(chunk) = audio.next().await {
        let pcm_bytes = chunk?;
        // Send raw PCM16 little-endian mono 24 kHz bytes to your audio sink.
    }
    client.close().await;
    Ok(())
}
```

The SDK does not read `.env` files. Explicit credentials use `.api_key(...)`.
Use `.model(Model::MistV3)` to select Mist v3. Coda is the default.

Use `client.tts().synthesize(text, options)` for complete text. Use
`client.tts().stream(source, options)` for incremental text. Both return an
`AudioStream` immediately; do not await these method calls. Read audio chunks
as they arrive. The TTS view shares the client's configuration and shutdown.

For example, replace the synthesis call above with:

```rust,no_run
# use rimelabs_sdk::{Client, SynthesisOptions};
# async fn example(client: &Client) -> Result<(), rimelabs_sdk::Error> {
let text = futures_util::stream::iter([
    Ok("Hello. ".to_owned()),
    Ok("This text arrives in chunks.".to_owned()),
]);
let mut audio = client.tts().stream(text, SynthesisOptions::default())?;
// Consume audio with the same loop as above.
# audio.close().await;
# Ok(())
# }
```

`client.tts().stream` accepts a `Send + 'static` stream of `Result<String, Error>`.
It preserves source text while detecting sentences. Input errors cancel the
request. Audio can arrive before input ends. Successful completion requires
reading the audio stream to its end; earlier chunks can precede a later error.
An error discards audio still in the SDK queue and takes priority on the next
read. Audio already returned to the application remains available to it.

Dropping an audio stream requests cancellation. `audio.close().await` also
waits for its worker to stop. `client.close().await` cancels all operations on
all clones. Dropping the last client handle requests cancellation. Applications
must keep their Tokio runtime alive while waiting for cleanup.

The audio queue holds at most 96,000 bytes in chunks of at most 9,600 bytes.
Backpressure pauses output reads. Progress deadlines exclude a paused text
source and buffered audio. An optional overall timeout still applies during
either pause. Synthesis requests are never replayed automatically.

`AudioFormat::Mulaw8000` selects G.711 mu-law mono 8 kHz output. Conversion
uses the same filter and test vectors as the other SDKs. Audio has no WAV header.

The sentence detector embeds the shared BlingFire WASM binary. No external
libraries, C toolchain, or runtime download is needed. See `vendor/README.md`.

## Synthesis options

Both TTS methods accept `SynthesisOptions::default()` with optional builders:

| Builder method | Default and behavior |
| --- | --- |
| `.voice("name")` | The model's default voice: `clementine` for Coda, `astra` for Mist v3. |
| `.language("en")` | `en`; passed to the service. Blank values fail locally. |
| `.audio_format(AudioFormat::Mulaw8000)` | `AudioFormat::Pcm24000`; both profiles stream raw mono audio. |
| `.timeout(Some(Duration::from_secs(60)))` | `Some(duration)` overrides the client timeout; this example sets it to 60 seconds. Default options inherit the client timeout. `None` disables it; zero is invalid. Applies until audio is fully consumed, including consumer pauses. |
| `.custom_lexicon(entries)` | Empty vector; request-wide pronunciation overrides for Coda. |
| `.timestamps(true)` | `false`; Mist v3 word timings after successful audio consumption. |
| `.complete_text(true)` | `false`; selects the full-text RPC. Requires `synthesize()` and at most 65,536 UTF-8 bytes. |

## Custom pronunciations (Coda)

Pass words or phrases and their space-separated X-SAMPA pronunciations:

```rust,no_run
use futures_util::StreamExt;
use rimelabs_sdk::{Client, ErrorKind, PronunciationEntry, SynthesisOptions};

# async fn example() -> Result<(), rimelabs_sdk::Error> {
let client = Client::builder().build()?; // Coda is the default.
let options = SynthesisOptions::default()
    .language("en")
    .custom_lexicon(vec![PronunciationEntry::new("hello", r#"h @ . " l oU"#)]);
let mut audio = client.tts().synthesize("Hello world.", options)?;
while let Some(chunk) = audio.next().await {
    match chunk {
        Ok(bytes) => { /* Send bytes to your audio sink. */ }
        Err(error) => {
            if error.kind() == ErrorKind::Input {
                eprintln!("{}; request: {:?}", error.message(), error.request_id());
            }
            return Err(error);
        }
    }
}
client.close().await;
# Ok(())
# }
```

Options own their lexicon entries. Calling `synthesize()` or `stream()` transfers
those options to the request; cloning options makes an independent copy. The
same lexicon applies to the entire request, including incremental text. Subsequent
requests have their own options. Omit the option or use an empty vector for
default pronunciation. Rust's typed entries and strings guarantee field types
and valid UTF-8.

The service matches whole words or phrases case-insensitively and applies the
pronunciation after text normalization. Longest matches take precedence; the
last entry for a repeated spelling wins. Inline `pronounce(...)` directives are
not supported.

Coda accepts custom pronunciations in German, English, Spanish, French, Italian,
and Portuguese, with up to 500 entries per request. The service validates
languages, pronunciation rules, and entry limits. Mist rejects nonempty lexicons.
The SDK passes entries unchanged, including their order, case, and whitespace.

Service-rejected entries fail the request with `ErrorKind::Input` before audio.
Check errors when consuming the stream as well as at construction.
`error.message()` preserves the service's explanation, including checks such as
`no-primary-stress` or `unknown-phone`, and `error.request_id()` identifies the
request when supplied by the service. Entries are never silently skipped or
repaired, and rejected requests are never replayed automatically.

## Word timestamps (Mist v3)

Enable timestamps, consume all audio, then read the result:

```rust,no_run
use futures_util::StreamExt;
use rimelabs_sdk::{Client, Model, SynthesisOptions};

# async fn example() -> Result<(), rimelabs_sdk::Error> {
let client = Client::builder().model(Model::MistV3).build()?;
let mut audio = client.tts().synthesize(
    "Twenty two.", SynthesisOptions::default().timestamps(true),
)?;
while let Some(chunk) = audio.next().await {
    let bytes = chunk?;
    // Send bytes to your audio sink.
}
let result = audio.timestamps()?;
if result.status.code == 0 {
    for word in result.spans {
        println!("{}: {:.3}–{:.3}s", word.text, word.start, word.end);
    }
} else {
    eprintln!("Alignment unavailable: {}", result.status.message);
}
client.close().await;
# Ok(())
# }
```

`audio.timestamps()` is synchronous: it never waits or consumes audio. Calls
without opt-in, or early calls on an otherwise healthy stream, return
`ErrorKind::Input`. Read audio through the stream's final `None` before requesting
the result. Enabling timestamps with Coda fails locally.

`TimestampResult` contains a `TimestampStatus` and a vector of `WordTimestamp`
values. Status code zero means success; nonzero codes are the service's
`google.rpc.Code` alignment failures, with an explanation and no spans. Alignment
failures do not fail audio. Missing or malformed timestamp trailers produce
`ErrorKind::Stream` from the accessor while audio remains usable. A failed RPC,
cancellation, or timeout is returned as an SDK error and cannot produce a
successful final result.

Timings describe normalized spoken words, with `start` and `end` in seconds from
the beginning of the entire synthesis. They are unchanged by PCM or mu-law output
conversion. They arrive after generation, not incrementally. Reading audio to
completion does not require waiting for speaker playback. Results own their
values and remain available after closing a successfully consumed stream or
client.

## Complete text input

Use `.complete_text(true)` on `synthesize()` to send the entire text in one
`Synthesize` RPC while receiving streaming audio:

```rust,no_run
# use rimelabs_sdk::{Client, SynthesisOptions};
# async fn example(client: &Client) -> Result<(), rimelabs_sdk::Error> {
let mut audio = client.tts().synthesize(
    "Please read this.", SynthesisOptions::default().complete_text(true),
)?;
// Consume audio with the same loop and cleanup as above.
# audio.close().await;
# Ok(())
# }
```

The default uses `SynthesizeStreaming` and local sentence detection, including
when `synthesize()` receives a string. Full-text mode accepts at most 65,536
UTF-8 bytes and uses server-side text splitting. Passing this option to `stream()`
fails locally; collect incremental input in your application and call
`synthesize()` instead. Lexicons, Mist v3 timestamps, audio profiles, error
details and request IDs, deadlines, and cancellation behave the same in both
modes.

## Transcribe speech

Use `client.stt().stream(source, options)` for one utterance. The source supplies
raw signed little-endian PCM16 bytes. Source exhaustion ends the utterance.
The application owns file access, microphone capture, and the decision to stop.

```rust,no_run
use bytes::Bytes;
use futures_util::StreamExt;
use rimelabs_sdk::{Client, TranscriptionOptions, TranscriptionUpdate};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder().build()?;
// Replace with an asynchronous source of your recorded PCM16 audio.
let source = futures_util::stream::iter([Ok(Bytes::from_static(&[0, 0]))]);
let mut transcript = client.stt().stream(source, TranscriptionOptions::new("en"))?;
while let Some(update) = transcript.next().await {
    match update? {
        TranscriptionUpdate::Partial { text } => println!("partial: {text}"),
        TranscriptionUpdate::Final { text, language } => println!("final ({language}): {text}"),
    }
}
println!("request: {:?}", transcript.request_id());
client.close().await;
# Ok(())
# }
```

Add `bytes = "1"` to your dependencies for audio chunks. The source must implement
`Stream<Item = Result<Bytes, Error>> + Send + 'static`. Use `Error::input(error)`
to report a source failure. See [the file example](examples/transcribe.rs) for
asynchronous file reads.

`TranscriptionOptions::new(language)` requires a spoken BCP-47 language tag.
The SDK passes it unchanged to the service. Options are:

| Builder method | Behavior |
| --- | --- |
| `.mode(TranscriptionMode::Written)` | Default formatting intent. Use `Verbatim` to preserve spoken wording. |
| `.context_terms(vec!["Rime".into()])` | Recognition hints, passed unchanged and in order. |
| `.input_format(PcmFormat { sample_rate: 24000, channels: 2 })` | PCM16 at 8, 16, 24, or 48 kHz; mono or interleaved stereo. Default: 16 kHz mono. |
| `.timeout(Some(Duration::from_secs(120)))` | Overall timeout from the first poll until final consumption, including source and consumer pauses. Default: `None`. Zero is invalid. |

The SDK converts input to 16 kHz mono. Chunks can split sample frames. An
incomplete frame at source exhaustion fails the request. Each gRPC audio payload
is at most 64 KiB. Input must have no WAV header or compressed encoding.

STT uses `stt.api.rime.ai:443` with TLS and the client's API key. Override it with
`Client::builder().stt_endpoint("host:443")`. The TTS `model`, `endpoint`, and
client `timeout` settings do not apply to STT. STT has no voice or model selector.

Stream construction validates options. The first poll starts network work.
The SDK reads no audio until the service accepts the configuration. Upload and
transcript reading then run concurrently. Each partial replaces the previous
transcript. Do not concatenate partials. Silence can return an empty final.

A final requires source exhaustion, a matching protocol result, and successful
gRPC completion. Errors never convert partial text into a final result. Requests
are not replayed automatically. The SDK has no voice activity detection, automatic
endpointing, word timing, or speaker diarization.

The transcript queue holds at most 16 updates, each with at most 64 KiB of UTF-8
text. The gRPC response limit is 256 KiB. Slow readers pause response processing.
Connection and acceptance each have a ten-second limit. Completion has a
120-second limit after source exhaustion, ending on successful gRPC completion.
The optional overall timeout stays active until the caller consumes the final.
Server limits also apply.

Dropping a transcript stream cancels its operation. `transcript.cancel()` requests
cancellation; `transcript.close().await` also waits for worker cleanup. Client
close cancels all TTS and STT operations, including unpolled streams. Cancelling
one stream leaves other streams active. Audio sources must yield without blocking
Tokio. Cancellation drops the source; it cannot stop external work started by
the application. Errors use the shared `ErrorKind` categories and retain the
request ID when the service supplies it.
