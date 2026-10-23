use super::peer::{event, Peer};
use crate::realtime::{protocol::decode, RealtimeEvent, ReplyOptions};
use serde_json::{json, Value};

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
    client.close().await;
}
