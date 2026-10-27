use super::peer::Peer;
use crate::realtime::{RealtimeConnectOptions, RealtimeUpdateOptions, Truncation};
use serde_json::json;
#[tokio::test]
async fn truncation_connect_update_and_acknowledgment() {
    for (value, wire) in [
        (Truncation::Auto, json!("auto")),
        (Truncation::Disabled, json!("disabled")),
        (
            Truncation::RetentionRatio(0.9),
            json!({"type":"retention_ratio","retention_ratio":0.9}),
        ),
        (
            Truncation::RetentionRatio(1.0),
            json!({"type":"retention_ratio","retention_ratio":1.0}),
        ),
    ] {
        let mut p = Peer::new().await;
        let (c, s) = p
            .setup_with(RealtimeConnectOptions::default().truncation(value.clone()))
            .await;
        assert_eq!(p.update().await["truncation"], wire);
        assert!(s.info().truncation.is_none());
        let clone = s.clone();
        let setting = value.clone();
        let task = tokio::spawn(async move {
            clone
                .update(RealtimeUpdateOptions::default().truncation(setting))
                .await
        });
        let q = p.next("session.update").await;
        assert_eq!(q["session"], json!({"truncation":wire}));
        p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],"session":{"id":"session-1","truncation":wire}})).await;
        assert_eq!(task.await.unwrap().unwrap().truncation, Some(value.clone()));
        assert_eq!(s.info().truncation, Some(value));
        c.close().await;
    }
}
#[test]
fn omitted_and_invalid_truncation() {
    assert!(
        crate::realtime::client::settings(&RealtimeConnectOptions::default())
            .unwrap()
            .get("truncation")
            .is_none()
    );
    for ratio in [0.0, -1.0, 1.1, f64::NAN, f64::INFINITY] {
        assert!(crate::realtime::client::patch(
            RealtimeUpdateOptions::default().truncation(Truncation::RetentionRatio(ratio))
        )
        .is_err());
    }
}

#[tokio::test]
async fn response_truncation_survives_cancellation_acknowledgment() {
    use super::peer::event;
    use crate::realtime::RealtimeEvent;
    for summary in [
        None,
        Some(json!({"items_left_out":12})),
        Some(json!({"items_left_out":12,"first_kept_item_id":"item-kept"})),
    ] {
        for status in ["completed", "cancelled", "failed"] {
            let mut p = Peer::new().await;
            let (c, s) = p.setup().await;
            let mut events = s.events().unwrap();
            let response = p.turn(&s, "reply-1").await;
            event(&mut events).await;
            event(&mut events).await;
            let session = s.clone();
            let task = tokio::spawn(async move { session.cancel(&response).await });
            let q = p.next("response.cancel").await;
            let mut body =
                json!({"id":"reply-1","status":status,"metadata":{"prism_drain_token":"drain-1"}});
            if let Some(value) = &summary {
                body["prism_truncation"] = value.clone();
            }
            p.send(json!({"type":"response.done","event_id":"done-event","prism_request_event_id":q["event_id"],"response":body})).await;
            tokio::time::timeout(std::time::Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let ended = event(&mut events).await;
            assert_eq!(ended.event_id, "done-event");
            assert_eq!(ended.request_id.as_deref(), q["event_id"].as_str());
            let RealtimeEvent::ResponseEnded {
                truncation,
                status: actual,
                ..
            } = ended.payload
            else {
                panic!("missing response completion");
            };
            assert_eq!(actual, status);
            if let Some(value) = &summary {
                let truncation = truncation.unwrap();
                assert_eq!(truncation.items_left_out, 12);
                assert_eq!(
                    truncation.first_kept_item_id.as_deref(),
                    value["first_kept_item_id"].as_str()
                );
            } else {
                assert!(truncation.is_none());
            }
            c.close().await;
        }
    }
}
