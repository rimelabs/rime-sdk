//! Stream a raw 16 kHz mono PCM16 recording to Prism, then send silence until the reply ends.
//! Usage: cargo run --example realtime_recorded -- utterance.pcm
use futures_util::StreamExt;
use rimelabs_sdk::{
    realtime::{AudioChunk, RealtimeConnectOptions, RealtimeEvent},
    Client, Error, PcmFormat,
};
use std::time::Duration;

/// 40 ms of 16 kHz mono PCM16.
const CHUNK_BYTES: usize = 1280;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("supply a raw 16 kHz mono PCM16 file")?;
    let recording = tokio::fs::read(path).await?;
    let client = Client::builder().build()?;
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default())
        .await?;
    let mut events = session.events()?;
    // Prism finds the end of a turn in the audio that it receives. Like a live
    // microphone, keep sending audio at capture speed until the reply ends.
    let send = async {
        let silence = [0; CHUNK_BYTES];
        let chunks = recording
            .chunks(CHUNK_BYTES)
            .chain(std::iter::repeat(&silence[..]));
        for chunk in chunks {
            session
                .send_audio(AudioChunk {
                    data: chunk.to_vec().into(),
                    format: PcmFormat::default(),
                })
                .await?;
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        Ok::<_, Error>(())
    };
    let receive = async {
        while let Some(event) = events.next().await {
            match event?.payload {
                RealtimeEvent::TranscriptFinal { text, .. } => println!("You: {text}"),
                RealtimeEvent::TextDelta { delta, .. } => print!("{delta}"),
                RealtimeEvent::ResponseEnded { .. } => break,
                _ => {}
            }
        }
        Ok::<_, Error>(())
    };
    // `send` returns only on an error, so this ends when the reply ends or either side fails.
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {
            sent = send => sent,
            received = receive => received,
        }
    })
    .await;
    client.close().await;
    result??;
    Ok(())
}
