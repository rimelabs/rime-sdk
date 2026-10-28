use super::peer::{event, Peer};
use crate::realtime::{protocol::decode, Labels, RealtimeEvent, ReplyOptions, ReplyRequestOptions};
use crate::ErrorKind;
use futures_util::StreamExt;
use serde_json::{json, Value};

#[tokio::test]
async fn nudge_instruction_and_usage_reach_public_events() {
    let mut peer = Peer::new().await;
    let (client, session) = peer.setup().await;
    let mut events = session.events().unwrap();
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    let handle = session.clone();
    let task = tokio::spawn(async move {
        handle
            .request_reply(ReplyOptions::default().instruction("Check in."))
            .await
    });
    let request = peer.next("response.create").await;
    peer.send(json!({"type":"response.created","prism_request_event_id":request["event_id"],
        "response":{"id":"reply-1","metadata":{"prism_cause":"proactive","prism_instruction_item_id":"item-instruction"}}})).await;
    let response = task.await.unwrap().unwrap();
    event(&mut events).await;
    let RealtimeEvent::ResponseStarted {
        instruction_item_id,
        ..
    } = event(&mut events).await.payload
    else {
        panic!("missing response start");
    };
    assert_eq!(instruction_item_id.as_deref(), Some("item-instruction"));
    peer.send(
        json!({"type":"response.done","response":{"id":response.response_id(),"status":"completed",
        "usage":{"input_tokens":10,"output_tokens":3,"total_tokens":13,"context_window":65536}}}),
    )
    .await;
    let RealtimeEvent::ResponseEnded { usage, .. } = event(&mut events).await.payload else {
        panic!("missing response end");
    };
    let usage = usage.unwrap();
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.total_tokens,
            usage.context_window
        ),
        (10, 3, 13, 65536)
    );
    let handle = session.clone();
    let task = tokio::spawn(async move { handle.delete_item("item-instruction").await });
    let request = peer.next("conversation.item.delete").await;
    assert_eq!(request["item_id"], "item-instruction");
    peer.send(json!({"type":"conversation.item.deleted","prism_request_event_id":request["event_id"],"item_id":"item-instruction"})).await;
    task.await.unwrap().unwrap();
    client.close().await;
}

#[tokio::test]
async fn request_metadata_labels_one_typed_or_proactive_reply() {
    for kind in ["text", "nudge"] {
        let mut peer = Peer::new().await;
        let (client, session) = peer.setup().await;
        peer.send(json!({"type":"prism.typed_input.ready"})).await;
        let labels = Labels::from([("step".into(), "one".into())]);
        let handle = session.clone();
        let task = if kind == "text" {
            tokio::spawn(async move {
                handle
                    .send_text_with_options(
                        "hello",
                        ReplyRequestOptions::default().metadata(labels),
                    )
                    .await
            })
        } else {
            tokio::spawn(async move {
                handle
                    .request_reply(ReplyOptions::default().metadata(labels))
                    .await
            })
        };
        let request = peer.next("response.create").await;
        let cause = if kind == "text" {
            "user_text"
        } else {
            "proactive"
        };
        assert_eq!(
            request["response"]["metadata"],
            json!({"step":"one","prism_cause":cause})
        );
        peer.accept(&request, "reply-1").await;
        task.await.unwrap().unwrap();
        client.close().await;
    }
}

#[tokio::test]
async fn reserved_request_metadata_is_rejected_before_write() {
    let mut peer = Peer::new().await;
    let (client, session) = peer.setup().await;
    let labels = Labels::from([("prism_cause".into(), "user_text".into())]);
    let error = session
        .request_reply(ReplyOptions::default().metadata(labels))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Input);
    peer.no_request().await;
    client.close().await;
}

#[tokio::test]
async fn idle_timeout_fails_pending_work_and_event_stream() {
    let mut peer = Peer::new().await;
    let (client, session) = peer.setup().await;
    let mut events = session.events().unwrap();
    let handle = session.clone();
    let task = tokio::spawn(async move { handle.add_message("user", "history").await });
    peer.next("conversation.item.create").await;
    peer.send(json!({"type":"session.idle_timeout","reason":"idle_timeout","idle_timeout_s":300}))
        .await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.realtime_idle_timeout().unwrap().idle_timeout_s, 300.0);
    let error = tokio::time::timeout(std::time::Duration::from_secs(3), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.realtime_idle_timeout().unwrap().idle_timeout_s, 300.0);
    client.close().await;
}

#[test]
fn retry_classification_comes_from_the_code() {
    let cases: Value =
        serde_json::from_str(include_str!("../../../testdata/prism/error-retry.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let expected = case["retryable"].as_bool().unwrap();
        let wire = decode(
            &json!({"type": "error", "error": {"code": case["code"], "retryable": !expected}})
                .to_string(),
            "session-1",
        )
        .unwrap()
        .unwrap();
        let Some(RealtimeEvent::Fault(fault)) = wire.payload else {
            panic!("missing fault: {case}")
        };
        assert_eq!(fault.code, case["code"].as_str().unwrap());
        assert_eq!(fault.retryable(), expected, "{case}");
    }
}

#[tokio::test]
async fn response_fault_preserves_details_without_replay() {
    let overflow =
        "the prompt is 35401 tokens; with 512 for the reply it is longer than the context of 32768";
    for (code, retryable) in [
        ("capacity_exceeded", true),
        ("unavailable", true),
        ("response_timeout", true),
        ("context_length_exceeded", false),
        ("future_error", false),
    ] {
        let mut p = Peer::new().await;
        let (client, session) = p.setup().await;
        let mut events = session.events().unwrap();
        let response = p.turn(&session, "reply-1").await;
        let message = if code == "context_length_exceeded" {
            overflow
        } else {
            "reply failed"
        };
        p.send(json!({"type": "error", "error": {
            "code": code, "message": message, "scope": "response",
            "owner": {"kind": "response", "response_id": response.response_id()},
            "correlation_id": "diagnostic-1",
        }}))
        .await;
        let fault = loop {
            if let RealtimeEvent::Fault(fault) = event(&mut events).await.payload {
                break fault;
            }
        };
        assert_eq!(fault.code, code);
        assert_eq!(fault.retryable(), retryable);
        assert_eq!(fault.message, message);
        assert_eq!(fault.scope, "response");
        assert_eq!(fault.response_id.as_deref(), Some(response.response_id()));
        assert_eq!(fault.correlation_id.as_deref(), Some("diagnostic-1"));
        p.send(json!({"type": "response.done", "response": {"id": response.response_id(), "status": "failed", "metadata": {"prism_drain_token": "drain-1"}}})).await;
        assert!(matches!(
            event(&mut events).await.payload,
            RealtimeEvent::ResponseEnded { .. }
        ));
        p.send(json!({"type": "prism.typed_input.ready"})).await;
        assert!(matches!(
            event(&mut events).await.payload,
            RealtimeEvent::InputReady
        ));
        session.close().await;
        p.closed_without_request().await;
        client.close().await;
    }
}

#[tokio::test]
async fn request_error_retains_fault() {
    let mut p = Peer::new().await;
    let (client, session) = p.setup().await;
    let mut events = session.events().unwrap();
    p.send(json!({"type": "prism.typed_input.ready"})).await;
    let pending_session = session.clone();
    let pending = tokio::spawn(async move {
        pending_session
            .request_reply(ReplyOptions {
                instruction: Some("Say hello".into()),
                ..Default::default()
            })
            .await
    });
    let request = p.next("response.create").await;
    p.send(json!({"type": "error", "error": {
        "code": "invalid_instruction", "message": "instruction refused", "scope": "event",
        "owner": {"kind": "event", "event_id": request["event_id"]},
        "param": "response.prism_instruction", "correlation_id": "diagnostic-1",
    }}))
    .await;
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Realtime);
    let fault = error.realtime_fault().expect("missing fault");
    assert!(!fault.retryable());
    assert_eq!(fault.code, "invalid_instruction");
    assert_eq!(fault.message, "instruction refused");
    assert_eq!(fault.request_id.as_deref(), request["event_id"].as_str());
    assert_eq!(
        fault.parameter.as_deref(),
        Some("response.prism_instruction")
    );
    assert_eq!(fault.correlation_id.as_deref(), Some("diagnostic-1"));
    let received = loop {
        if let RealtimeEvent::Fault(fault) = event(&mut events).await.payload {
            break fault;
        }
    };
    assert_eq!(received.retryable(), fault.retryable());
    assert_eq!(received.code, fault.code);
    assert_eq!(received.message, fault.message);
    assert_eq!(received.request_id, fault.request_id);
    p.turn(&session, "after-refusal").await;
    client.close().await;
}

#[tokio::test]
async fn session_fault_is_terminal() {
    let mut p = Peer::new().await;
    let (client, session) = p.setup().await;
    let mut events = session.events().unwrap();
    let pending_session = session.clone();
    let pending = tokio::spawn(async move { pending_session.add_message("user", "history").await });
    p.next("conversation.item.create").await;
    p.send(json!({"type": "error", "error": {"scope": "session", "code": "session_expired"}}))
        .await;
    let received = tokio::time::timeout(std::time::Duration::from_secs(3), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let later = session
        .add_message("user", "after failure")
        .await
        .unwrap_err();
    for error in [pending.await.unwrap().unwrap_err(), received, later] {
        assert_eq!(error.kind(), ErrorKind::Realtime);
        let fault = error.realtime_fault().unwrap();
        assert_eq!(fault.scope, "session");
        assert_eq!(fault.code, "session_expired");
    }
    assert!(events.next().await.is_none());
    client.close().await;
}
