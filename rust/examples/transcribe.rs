//! Transcribe a headerless PCM16 mono 16 kHz file. Set RIME_API_KEY before running.
//! Usage: cargo run --example transcribe -- utterance.pcm en
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use rimelabs_sdk::{Client, Error, TranscriptionOptions, TranscriptionUpdate};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: transcribe <file.pcm> <language>")?;
    let language = args
        .next()
        .ok_or("usage: transcribe <file.pcm> <language>")?;
    if args.next().is_some() {
        return Err("usage: transcribe <file.pcm> <language>".into());
    }
    let client = Client::builder().build()?;
    // Defer file access until service acceptance. Source errors cancel the RPC.
    let source = stream::try_unfold((Some(path), None), |(path, file)| async move {
        let mut file = match file {
            Some(file) => file,
            None => tokio::fs::File::open(path.expect("initial path"))
                .await
                .map_err(Error::input)?,
        };
        let mut data = vec![0; 3200];
        let count = file.read(&mut data).await.map_err(Error::input)?;
        if count == 0 {
            return Ok(None);
        }
        data.truncate(count);
        Ok(Some((Bytes::from(data), (None, Some(file)))))
    });
    let mut transcript = client
        .stt()
        .stream(source, TranscriptionOptions::new(language))?;
    let result = async {
        while let Some(update) = transcript.next().await {
            match update? {
                TranscriptionUpdate::Partial { text } => println!("partial: {text}"),
                TranscriptionUpdate::Final { text, language } => {
                    println!("final ({language}): {text}")
                }
            }
        }
        Ok::<_, Error>(())
    }
    .await;
    if let Some(id) = transcript.request_id() {
        println!("request: {id}");
    }
    client.close().await;
    result?;
    Ok(())
}
