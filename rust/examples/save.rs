//! Save headerless PCM16 mono 24 kHz audio. Set RIME_API_KEY before running.
use futures_util::StreamExt;
use rimelabs_sdk::{Client, SynthesisOptions};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?;
    let mut file = tokio::fs::File::create("speech.pcm").await?;
    let mut audio = client.synthesize("Hello from Rust.", SynthesisOptions::default())?;
    while let Some(chunk) = audio.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    client.close().await;
    Ok(())
}
