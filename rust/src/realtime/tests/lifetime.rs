use super::peer::{event, Peer};
use crate::{realtime::*, ErrorKind};
use futures_util::StreamExt;
use serde_json::json;

async fn observed(
    p: &Peer,
    events: &mut RealtimeEvents,
    id: &str,
    tool: bool,
) -> (ResponseRef, Option<ToolCallRef>) {
    p.send(json!({"type":"response.created","response":{"id":id}}))
        .await;
    let RealtimeEvent::ResponseStarted { response, .. } = event(events).await.payload else {
        panic!("missing response")
    };
    let call = if tool {
        p.send(json!({"type":"response.function_call_arguments.done","response_id":id,"item_id":format!("item-{id}"),"call_id":format!("call-{id}"),"name":"lookup","arguments":"{}"})).await;
        let RealtimeEvent::ToolCall { call, .. } = event(events).await.payload else {
            panic!("missing tool")
        };
        Some(call)
    } else {
        None
    };
    p.ended(id).await;
    assert!(matches!(
        event(events).await.payload,
        RealtimeEvent::ResponseEnded { .. }
    ));
    (response, call)
}
async fn age(p: &Peer, events: &mut RealtimeEvents, prefix: &str) {
    for i in 0..140 {
        observed(p, events, &format!("{prefix}-{i}"), false).await;
    }
}
#[tokio::test]
async fn ordinary_expiry_nested_holds_delayed_playback_and_duplicate_completion() {
    let mut p = Peer::new().await;
    let (client, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let (reference, _) = observed(&p, &mut events, "held", false).await;
    let first = s.hold_response(&reference).await.unwrap();
    let second = s.hold_response(&reference).await.unwrap();
    let (ordinary, _) = observed(&p, &mut events, "ordinary", false).await;
    drop(first);
    age(&p, &mut events, "later").await;
    assert_eq!(
        s.hold_response(&ordinary).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    let mut invalid = reference.clone();
    invalid.session_id = "other".into();
    assert_eq!(
        s.hold_response(&invalid).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    invalid = reference.clone();
    invalid.response_id = "missing".into();
    assert_eq!(
        s.hold_response(&invalid).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    p.no_request().await;
    p.ended("held").await;
    p.ended("ordinary").await;
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::InputReady
    ));
    s.report_playback(PlaybackReport::Finished {
        response: reference.clone(),
        played_ms: Some(12.0),
        audible_tail_ms: None,
    })
    .await
    .unwrap();
    assert_eq!(
        p.next("prism.playback.drained").await["response_id"],
        "held"
    );
    drop(second);
    assert_eq!(
        s.hold_response(&reference).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    client.close().await;
}
#[tokio::test]
async fn unresolved_tool_and_hold_cover_result_then_report_after_140_responses() {
    let mut p = Peer::new().await;
    let (client, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let (reference, call) = observed(&p, &mut events, "tool", true).await;
    let call = call.unwrap();
    age(&p, &mut events, "later").await;
    let hold = s.hold_response(call.response()).await.unwrap();
    let clone = s.clone();
    let tool = call.clone();
    let result = tokio::spawn(async move { clone.submit_tool_result(&tool, "done").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"function_call_output","call_id":call.call_id()}})).await;
    result.await.unwrap().unwrap();
    s.submit_tool_result(&call, "done").await.unwrap();
    assert_eq!(
        s.submit_tool_result(&call, "different")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Input
    );
    assert_eq!(
        s.continue_reply(&reference).await.unwrap_err().kind(),
        ErrorKind::Input
    );
    age(&p, &mut events, "after-result").await;
    p.no_request().await;
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    let clone = s.clone();
    let report = tokio::spawn(async move {
        clone
            .request_reply(ReplyOptions {
                tool_call: Some(call),
                ..ReplyOptions::default()
            })
            .await
    });
    let q = p.next("response.create").await;
    assert_eq!(q["response"]["prism_call_id"], "call-tool");
    p.accept(&q, "report").await;
    report.await.unwrap().unwrap();
    drop(hold);
    assert_eq!(
        s.hold_response(&reference).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    client.close().await;
}
#[tokio::test]
async fn protected_capacity_local_refusal_and_incoming_overflow() {
    for late_tool in [false, true] {
        let mut p = Peer::new().await;
        let (client, s) = p.setup().await;
        let mut events = s.events().unwrap();
        let (ordinary, _) = observed(&p, &mut events, "ordinary", false).await;
        let mut holds = vec![];
        for i in 0..128 {
            let (reference, _) = observed(&p, &mut events, &i.to_string(), false).await;
            holds.push(s.hold_response(&reference).await.unwrap());
            if i == 127 {
                drop(s.hold_response(&reference).await.unwrap());
            }
        }
        assert_eq!(
            s.hold_response(&ordinary).await.err().unwrap().kind(),
            ErrorKind::ResourceLimit
        );
        p.no_request().await;
        // A local refusal leaves ordinary operations usable.
        s.set_response_metadata(Labels::new()).await.unwrap();
        if late_tool {
            p.send(json!({"type":"response.function_call_arguments.done","response_id":"ordinary","item_id":"overflow","call_id":"overflow","name":"lookup","arguments":"{}"})).await;
        } else {
            p.send(json!({"type":"response.created","response":{"id":"overflow"}}))
                .await;
        }
        let error = events.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ResourceLimit);
        s.close().await;
        drop(holds);
        client.close().await;
    }
}
#[tokio::test]
async fn hold_does_not_keep_last_session_lease_alive() {
    let mut p = Peer::new().await;
    let (client, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let (reference, _) = observed(&p, &mut events, "held", false).await;
    let hold = s.hold_response(&reference).await.unwrap();
    drop(s);
    p.closed_without_request().await;
    drop(hold);
    client.close().await;
}

#[tokio::test]
async fn active_generation_protects_after_hold_release() {
    let mut p = Peer::new().await;
    let (client, s) = p.setup().await;
    let mut events = s.events().unwrap();
    p.send(json!({"type":"response.created","response":{"id":"active"}}))
        .await;
    let RealtimeEvent::ResponseStarted { response, .. } = event(&mut events).await.payload else {
        panic!("missing response")
    };
    drop(s.hold_response(&response).await.unwrap());
    age(&p, &mut events, "later").await;
    drop(s.hold_response(&response).await.unwrap());
    p.ended("active").await;
    event(&mut events).await;
    assert_eq!(
        s.hold_response(&response).await.err().unwrap().kind(),
        ErrorKind::Input
    );
    p.no_request().await;
    client.close().await;
}
