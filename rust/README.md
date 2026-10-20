# Rime SDK for Rust

Generate speech with Coda or Mist v3 and transcribe audio with STT
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

SDK code uses the [MIT license](LICENSE).
