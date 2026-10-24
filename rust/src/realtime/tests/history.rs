use super::peer::{event, Peer};
use crate::realtime::{HistoryItem, HistoryRole, RealtimeEvent};
use serde_json::json;
#[tokio::test]
async fn history_create_read_delete_and_refusal() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let items = vec![
        HistoryItem::Message {
            id: Some("note".into()),
            role: HistoryRole::System,
            text: " Transferred. ".into(),
        },
        HistoryItem::FunctionCall {
            id: Some("past".into()),
            call_id: "old-call".into(),
            name: "lookup".into(),
            arguments: " {\"n\": 1} ".into(),
        },
        HistoryItem::FunctionResult {
            id: Some("result".into()),
            call_id: "old-call".into(),
            output: " raw output ".into(),
        },
    ];
    let mut wires = vec![];
    for item in &items {
        let wire = crate::realtime::history::settings(item).unwrap();
        wires.push(wire.clone());
        let clone = s.clone();
        let value = item.clone();
        let anchor = if matches!(item, HistoryItem::FunctionResult { .. }) {
            None
        } else {
            Some("root")
        };
        let task = tokio::spawn(async move { clone.create_item(value, anchor).await });
        let q = p.next("conversation.item.create").await;
        assert_eq!(q["item"], wire);
        p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":wire})).await;
        assert_eq!(task.await.unwrap().unwrap().item_id, item.id().unwrap());
    }
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.get_history().await });
    let q = p.next("prism.conversation.get").await;
    p.send(
        json!({"type":"prism.conversation","prism_request_event_id":q["event_id"],"items":wires}),
    )
    .await;
    assert_eq!(task.await.unwrap().unwrap(), items);
    for _ in 0..2 {
        let clone = s.clone();
        let task = tokio::spawn(async move { clone.delete_item("past").await });
        let q = p.next("conversation.item.delete").await;
        p.send(json!({"type":"conversation.item.deleted","prism_request_event_id":q["event_id"],"item_id":"past"})).await;
        task.await.unwrap().unwrap();
    }
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.delete_item("missing").await });
    let q = p.next("conversation.item.delete").await;
    p.send(json!({"type":"error","error":{"code":"item_not_found","scope":"event","owner":{"event_id":q["event_id"]}}})).await;
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        crate::ErrorKind::Realtime
    );
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.get_history().await });
    let q = p.next("prism.conversation.get").await;
    p.send(json!({"type":"prism.conversation","prism_request_event_id":q["event_id"],"items":[]}))
        .await;
    assert!(task.await.unwrap().unwrap().is_empty());
    c.close().await;
}
#[tokio::test]
async fn cancelled_history_result_unblocks_active_tool() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let parent = p.turn(&s, "reply-1").await;
    event(&mut events).await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":"reply-1","item_id":"tool","call_id":"call-1","name":"lookup","arguments":"{}"})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ToolCall { .. }
    ));
    p.ended("reply-1").await;
    event(&mut events).await;
    let clone = s.clone();
    let task = tokio::spawn(async move {
        clone
            .create_item(
                HistoryItem::FunctionResult {
                    id: None,
                    call_id: "call-1".into(),
                    output: "result".into(),
                },
                None,
            )
            .await
    });
    let q = p.next("conversation.item.create").await;
    task.abort();
    let _ = task.await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"function_call_output","id":"result","call_id":"call-1"}})).await;
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.continue_reply(&parent).await });
    let q = p.next("response.create").await;
    p.accept(&q, "reply-2").await;
    task.await.unwrap().unwrap();
    c.close().await;
}
#[test]
fn invalid_history_is_rejected() {
    for item in [
        HistoryItem::Message {
            id: Some(String::new()),
            role: HistoryRole::User,
            text: "x".into(),
        },
        HistoryItem::FunctionCall {
            id: None,
            call_id: String::new(),
            name: "lookup".into(),
            arguments: "{}".into(),
        },
    ] {
        assert!(crate::realtime::history::settings(&item).is_err());
    }
}

#[tokio::test]
async fn deletion_acknowledgment_must_name_the_requested_item() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.delete_item("right").await });
    let q = p.next("conversation.item.delete").await;
    p.send(json!({"type":"conversation.item.deleted","prism_request_event_id":q["event_id"],"item_id":"wrong"})).await;
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        crate::ErrorKind::Stream
    );
    c.close().await;
}

#[tokio::test]
async fn reused_call_id_does_not_change_a_recorded_response() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let parent = p.turn(&s, "reply-1").await;
    event(&mut events).await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":parent.response_id(),"item_id":"old-call-item","call_id":"shared","name":"lookup","arguments":"{}"})).await;
    let RealtimeEvent::ToolCall { call, .. } = event(&mut events).await.payload else {
        panic!("missing call")
    };
    p.ended("reply-1").await;
    event(&mut events).await;
    let clone = s.clone();
    let original = call.clone();
    let task = tokio::spawn(async move { clone.submit_tool_result(&original, "original").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"function_call_output","call_id":"shared"}})).await;
    task.await.unwrap().unwrap();
    let clone = s.clone();
    let task = tokio::spawn(async move { clone.delete_item("old-call-item").await });
    let q = p.next("conversation.item.delete").await;
    p.send(json!({"type":"conversation.item.deleted","prism_request_event_id":q["event_id"],"item_id":"old-call-item"})).await;
    task.await.unwrap().unwrap();
    for item in [
        HistoryItem::FunctionCall {
            id: Some("imported-call".into()),
            call_id: "shared".into(),
            name: "lookup".into(),
            arguments: "{}".into(),
        },
        HistoryItem::FunctionResult {
            id: Some("imported-result".into()),
            call_id: "shared".into(),
            output: "imported".into(),
        },
    ] {
        let clone = s.clone();
        let task = tokio::spawn(async move { clone.create_item(item, None).await });
        let q = p.next("conversation.item.create").await;
        p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":q["item"]})).await;
        task.await.unwrap().unwrap();
    }
    s.submit_tool_result(&call, "original").await.unwrap();
    p.no_request().await;
    c.close().await;
}

#[tokio::test]
async fn creation_checks_client_ids_and_accepts_server_assigned_ids() {
    for client_id in [Some("requested"), None] {
        let mut p = Peer::new().await;
        let (c, s) = p.setup().await;
        let clone = s.clone();
        let task = tokio::spawn(async move {
            clone
                .create_item(
                    HistoryItem::Message {
                        id: client_id.map(str::to_owned),
                        role: HistoryRole::System,
                        text: "Billing.".into(),
                    },
                    None,
                )
                .await
        });
        let q = p.next("conversation.item.create").await;
        assert_eq!(q["item"].get("id").and_then(|v| v.as_str()), client_id);
        p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"message","id":"server-assigned","role":"system"}})).await;
        let result = task.await.unwrap();
        if client_id.is_some() {
            assert_eq!(result.unwrap_err().kind(), crate::ErrorKind::Stream);
        } else {
            assert_eq!(result.unwrap().item_id(), "server-assigned");
        }
        c.close().await;
    }
}
