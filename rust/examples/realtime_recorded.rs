//! Stream a raw 16 kHz mono PCM16 recording to Prism, then send one second of silence.
//! Usage: cargo run --example realtime_recorded -- utterance.pcm
use futures_util::StreamExt;
use rimelabs_sdk::{
    realtime::{AudioChunk, RealtimeConnectOptions, RealtimeEvent},
    Client, Error, PcmFormat,
};
use tokio::io::AsyncReadExt;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("supply a raw 16 kHz mono PCM16 file")?;
    let client = Client::builder().build()?;
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default())
        .await?;
    let mut events = session.events()?;
    let consumer = tokio::spawn(async move {
        while let Some(event) = events.next().await {
            match event?.payload {
                RealtimeEvent::TranscriptFinal { text, .. } => println!("You: {text}"),
                RealtimeEvent::TextDelta { delta, .. } => print!("{delta}"),
                RealtimeEvent::ResponseEnded { .. } => break,
                _ => {}
            }
        }
        Ok::<_, Error>(())
    });
    let sent = async {
        let mut file = tokio::fs::File::open(path).await.map_err(Error::input)?;
        loop {
            let mut data = vec![0; 1280];
            let mut count = 0;
            while count < data.len() {
                let n = file.read(&mut data[count..]).await.map_err(Error::input)?;
                if n == 0 {
                    break;
                }
                count += n
            }
            if count == 0 {
                break;
            }
            data.truncate(count);
            session
                .send_audio(AudioChunk {
                    data: data.into(),
                    format: PcmFormat::default(),
                })
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        }
        for _ in 0..25 {
            session
                .send_audio(AudioChunk {
                    data: vec![0; 1280].into(),
                    format: PcmFormat::default(),
                })
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(40)).await
        }
        Ok::<_, Error>(())
    }
    .await;
    if sent.is_err() {
        session.close().await;
    }
    let received = tokio::time::timeout(std::time::Duration::from_secs(60), consumer).await;
    client.close().await;
    sent?;
    received???;
    Ok(())
}
