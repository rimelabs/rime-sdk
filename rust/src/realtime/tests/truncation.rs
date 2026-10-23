use super::peer::Peer;
use crate::realtime::{RealtimeConnectOptions, RealtimeUpdateOptions, Truncation};
use serde_json::json;
#[tokio::test]
async fn truncation_connect_update_and_acknowledgment() {
    for value in [
        Truncation::Auto,
        Truncation::Disabled,
        Truncation::RetentionRatio(0.9),
        Truncation::RetentionRatio(1.0),
    ] {
        let settings = crate::realtime::client::settings(
            &RealtimeConnectOptions::default().truncation(value.clone()),
        )
        .unwrap();
        let mut p = Peer::new().await;
        let (c, s) = p.setup().await;
        assert!(s.info().truncation.is_none());
        let clone = s.clone();
        let setting = value.clone();
        let task = tokio::spawn(async move {
            clone
                .update(RealtimeUpdateOptions::default().truncation(setting))
                .await
        });
        let q = p.next("session.update").await;
        assert_eq!(q["session"], json!({"truncation":settings["truncation"]}));
        p.send(json!({"type":"session.updated","prism_request_event_id":q["event_id"],"session":{"id":"session-1","truncation":settings["truncation"]}})).await;
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
