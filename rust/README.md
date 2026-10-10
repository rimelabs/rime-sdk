# Rime SDK for Rust

Asynchronous text-to-speech for Coda and Mist v3. Requires Rust 1.88 or later
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
