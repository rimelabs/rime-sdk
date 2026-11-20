use super::peer::Peer;
use crate::{
    realtime::{RealtimeConnectOptions, RealtimeUpdateOptions},
    PronunciationEntry,
};
use serde_json::json;

fn kubectl() -> PronunciationEntry {
    PronunciationEntry::new("kubectl", r#"" k u b . k @ n . % t r\ oU l"#)
}

#[tokio::test]
async fn context_terms_and_lexicon_are_sent_and_read_back() {
    let mut p = Peer::new().await;
    let (_client, s) = p
        .setup_with(
            RealtimeConnectOptions::default()
                .context_terms(vec!["Skyvale".into(), "Doctor Chen".into()])
                .custom_lexicon(vec![kubectl()]),
        )
        .await;
    let sent = p.update().await;
    assert_eq!(sent["context_terms"], json!(["Skyvale", "Doctor Chen"]));
    assert_eq!(
        sent["custom_lexicon"],
        json!([{"spelling": "kubectl", "pronunciation": kubectl().pronunciation}])
    );

    let clone = s.clone();
    let task = tokio::spawn(async move {
        clone
            .update(
                RealtimeUpdateOptions::default()
                    .context_terms(vec!["Velkora".into()])
                    .custom_lexicon(vec![]),
            )
            .await
    });
    let q = p.next("session.update").await;
    assert_eq!(
        q["session"],
        json!({"context_terms": ["Velkora"], "custom_lexicon": []})
    );
    p.send(json!({"type": "session.updated", "prism_request_event_id": q["event_id"],
        "session": {"id": "session-1", "voice": "test", "context_terms": ["Velkora"], "custom_lexicon": []}}))
        .await;
    let info = task.await.unwrap().unwrap();
    assert_eq!(info.context_terms, Some(vec!["Velkora".to_owned()]));
    // An empty list Prism confirmed is not None, which means not published.
    assert_eq!(info.custom_lexicon, Some(vec![]));
    assert_eq!(s.info().context_terms, info.context_terms);
}

#[tokio::test]
async fn omitted_context_terms_and_lexicon_stay_out_of_the_request() {
    let mut p = Peer::new().await;
    let (_client, s) = p.setup().await;
    let sent = p.update().await;
    assert!(sent.get("context_terms").is_none(), "{sent}");
    assert!(sent.get("custom_lexicon").is_none(), "{sent}");
    // A Prism release without these fields does not publish them.
    assert_eq!(s.info().context_terms, None);
    assert_eq!(s.info().custom_lexicon, None);
}

#[tokio::test]
async fn context_terms_and_lexicon_refusal_reaches_the_caller() {
    for code in ["invalid_client_event", "invalid_session", "turn_failed"] {
        let mut p = Peer::new().await;
        let (_client, s) = p.setup().await;
        let clone = s.clone();
        let task = tokio::spawn(async move {
            clone
                .update(
                    RealtimeUpdateOptions::default()
                        .context_terms(vec!["Velkora".into()])
                        .custom_lexicon(vec![kubectl()]),
                )
                .await
        });
        let q = p.next("session.update").await;
        p.send(
            json!({"type": "error", "error": {"code": code, "message": code, "scope": "event",
            "owner": {"event_id": q["event_id"]}}}),
        )
        .await;
        let error = task.await.unwrap().unwrap_err();
        let fault = error.realtime_fault().expect("missing fault");
        assert_eq!((fault.code.as_str(), fault.scope.as_str()), (code, "event"));
        assert_eq!(s.info().context_terms, None);
    }
}
