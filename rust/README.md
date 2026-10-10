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
