//! Execute one application-owned function and continue the tool response.
use futures_util::StreamExt;
use rimelabs_sdk::{
    realtime::{PrismFiller, RealtimeConnectOptions, RealtimeEvent, ToolDefinition},
    Client, Error,
};
use serde_json::json;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().build()?;
    let tool = ToolDefinition {
        name: "store_hours".into(),
        description: "Return today's store hours".into(),
        parameters: json!({"type":"object","properties":{},"additionalProperties":false})
            .as_object()
            .unwrap()
            .clone(),
        prism_filler: Some(PrismFiller::Text("I will check the hours.".into())),
    };
    let session = client
        .realtime()
        .connect(RealtimeConnectOptions::default().tools(vec![tool]))
        .await?;
    let mut events = session.events()?;
    let sender = session.clone();
    let request = tokio::spawn(async move {
        sender
            .send_text("Use store_hours to tell me when the store opens.")
            .await
    });
    let result = async {
        let mut parent = None;
        let mut tasks = tokio::task::JoinSet::new();
        while let Some(event) = events.next().await {
            match event?.payload {
                RealtimeEvent::ToolCall { call, .. } => {
                    parent = Some(call.response().clone());
                    let session = session.clone();
                    tasks.spawn(async move {
                        session
                            .submit_tool_result(
                                &call,
                                "The store opens at 09:00 and closes at 17:00.",
                            )
                            .await
                    });
                }
                RealtimeEvent::ResponseEnded { response, .. } => {
                    if parent.as_ref() == Some(&response) {
                        let session = session.clone();
                        tasks.spawn(
                            async move { session.continue_reply(&response).await.map(|_| ()) },
                        );
                        parent = None;
                    } else {
                        break;
                    }
                }
                RealtimeEvent::TextDelta { delta, .. } => print!("{delta}"),
                _ => {}
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(Error::input)??
        }
        Ok::<_, Error>(())
    }
    .await;
    client.close().await;
    request.await??;
    result?;
    println!();
    Ok(())
}
