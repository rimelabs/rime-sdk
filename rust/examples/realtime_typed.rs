//! Send one typed Prism turn. Run from this checkout with RIME_API_KEY set.
use futures_util::StreamExt;
use rimelabs_sdk::{
    realtime::{RealtimeConnectOptions, RealtimeEvent},
    Client,
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?;
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default())
        .await?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut events = session.events()?;
        let consumer = async {
            while let Some(event) = events.next().await {
                match event?.payload {
                    RealtimeEvent::TextDelta { delta, .. } => print!("{delta}"),
                    RealtimeEvent::ResponseEnded { .. } => break,
                    _ => {}
                }
            }
            Ok::<_, rimelabs_sdk::Error>(())
        };
        tokio::try_join!(session.send_text("Give me one short greeting."), consumer)?;
        println!();
        Ok::<_, rimelabs_sdk::Error>(())
    })
    .await;
    client.close().await;
    result??;
    Ok(())
}
