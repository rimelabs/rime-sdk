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
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        sender
            .send_text("Use store_hours to tell me when the store opens.")
            .await
            .map(|_| ())
    });
    let result = async {
        let mut parent = None;
        loop {
            let event = tokio::select! {
                result = tasks.join_next(), if !tasks.is_empty() => {
                    result.expect("pending operation")??;
                    continue;
                }
                event = events.next() => {
                    let Some(event) = event else { break };
                    event
                }
            };
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
                RealtimeEvent::ResponseEnded {
                    response, status, ..
                } => {
                    if status != "completed" {
                        return Err(format!("response ended with status {status}").into());
                    }
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
                RealtimeEvent::Fault(fault)
                    if fault.scope == "event"
                        && matches!(
                            fault.code.as_str(),
                            "typed_turn_busy" | "instructions_busy" | "proactive_unavailable"
                        ) =>
                {
                    // The session retries these refusals after the next readiness event.
                }
                RealtimeEvent::Fault(fault) => return Err(fault.into()),
                _ => {}
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(Error::input)??
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    tasks.shutdown().await;
    client.close().await;
    result?;
    println!();
    Ok(())
}
