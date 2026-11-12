use super::peer::{event, Peer};
use crate::realtime::{client, protocol};
use crate::{
    HistoryItem, HistoryRole, RealtimeConnectOptions, RealtimeEvent, RealtimeUpdateOptions,
    ReplyOptions, Review, ReviewDecision, ReviewTimeout, TurnDetection, TurnDetectionUpdate,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

#[test]
fn optional_settings_preserve_defaults_and_merge_interrupt_fields() {
    let defaults = client::settings(&RealtimeConnectOptions::default()).unwrap();
    assert!(defaults.get("speed").is_none());
    assert!(defaults.get("prism_review").is_none());
    assert_eq!(
        defaults["turn_detection"],
        json!({"interrupt_response":true})
    );
    let configured = client::settings(
        &RealtimeConnectOptions::default()
            .speed(1.2)
            .turn_detection(TurnDetection {
                end_of_turn_min_silence_ms: Some(0),
                interrupt_min_words: Some(2),
                interrupt_phrases: Some(vec!["stop".into()]),
                acknowledgment_phrases: Some(vec![]),
                ..TurnDetection::default()
            })
            .review(
                Review::new(|_| async { Ok(ReviewDecision::Allow) })
                    .on_timeout(ReviewTimeout::Say("One moment.".into())),
            ),
    )
    .unwrap();
    assert_eq!(configured["speed"], 1.2);
    assert_eq!(
        configured["turn_detection"]["end_of_turn_min_silence_ms"],
        0
    );
    assert_eq!(
        configured["turn_detection"]["acknowledgment_phrases"],
        json!([])
    );
    assert_eq!(configured["prism_review"]["timeout_line"], "One moment.");
    let patch = client::patch(
        RealtimeUpdateOptions::default()
            .speed(0.85)
            .interrupt_on_speech(false)
            .turn_detection(TurnDetectionUpdate {
                interrupt_phrases: Some(vec![]),
                ..TurnDetectionUpdate::default()
            }),
    )
    .unwrap();
    assert_eq!(
        patch,
        json!({"speed":0.85,"turn_detection":{"interrupt_response":false,"interrupt_phrases":[]}})
    );
    assert!(client::settings(&RealtimeConnectOptions::default().speed(f64::NAN)).is_err());
    assert!(client::settings(
        &RealtimeConnectOptions::default()
            .review(Review::new(|_| async { Ok(ReviewDecision::Allow) }).timeout_ms(99))
    )
    .is_err());
}

#[test]
fn new_received_values_preserve_absence_and_reject_malformed_fields() {
    let wire = protocol::decode(
        &json!({"type":"session.updated", "session":{
            "id":"session-1", "version":"git-abc", "speed":1.1,
            "prism_review":{"timeout_ms":1000,"on_timeout":"say","timeout_line":"Wait."},
            "turn_detection":{"end_of_turn_min_silence_ms":0,"interrupt_min_words":2,
            "interrupt_phrases":[],"acknowledgment_phrases":["uh-huh"]}
        }})
        .to_string(),
        "session-1",
    )
    .unwrap()
    .unwrap();
    let info = wire.info.unwrap();
    assert_eq!(info.version.as_deref(), Some("git-abc"));
    assert_eq!(info.speed, Some(1.1));
    assert_eq!(info.end_of_turn_min_silence_ms, Some(0));
    assert_eq!(info.interrupt_min_words, Some(2));
    assert_eq!(info.interrupt_phrases, Some(vec![]));
    assert_eq!(
        info.review.unwrap().on_timeout,
        ReviewTimeout::Say("Wait.".into())
    );
    let old = protocol::decode(r#"{"type":"session.created","session":{"id":"s"}}"#, "")
        .unwrap()
        .unwrap()
        .info
        .unwrap();
    assert!(old.version.is_none() && old.speed.is_none() && old.review.is_none());
    for (key, value) in [
        ("version", json!(null)),
        ("speed", json!("1")),
        ("prism_review", json!(null)),
    ] {
        let mut session = json!({"id":"s"});
        session[key] = value;
        assert!(
            protocol::decode(
                &json!({"type":"session.updated","session":session}).to_string(),
                "s"
            )
            .is_err(),
            "{key}"
        );
    }
    for bad in [
        json!(null),
        json!(true),
        json!(1.5),
        json!(-1),
        json!(9007199254740992_u64),
    ] {
        assert!(protocol::decode(
            &json!({"type":"prism.typed_input.ready","server_time_ms":bad}).to_string(),
            "s"
        )
        .is_err());
    }
}

#[test]
fn speed_bounds_apply_to_connect_updates_and_received_settings() {
    for value in [0.85, 1.0, 1.3] {
        assert_eq!(
            client::settings(&RealtimeConnectOptions::default().speed(value)).unwrap()["speed"],
            value
        );
        assert_eq!(
            client::patch(RealtimeUpdateOptions::default().speed(value)).unwrap()["speed"],
            value
        );
        let wire = protocol::decode(
            &json!({"type":"session.updated","session":{"id":"s","speed":value}}).to_string(),
            "s",
        )
        .unwrap()
        .unwrap();
        assert_eq!(wire.info.unwrap().speed, Some(value));
    }
    for value in [0.84, 1.31, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            client::settings(&RealtimeConnectOptions::default().speed(value))
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Input
        );
        assert_eq!(
            client::patch(RealtimeUpdateOptions::default().speed(value))
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Input
        );
        assert!(protocol::decode(
            &json!({"type":"session.updated","session":{"id":"s","speed":value}}).to_string(),
            "s"
        )
        .is_err());
    }
}

#[tokio::test]
async fn turn_setting_limits_apply_before_connect_and_update_writes() {
    let invalid_interrupts = [
        TurnDetectionUpdate {
            interrupt_min_words: Some(0),
            ..TurnDetectionUpdate::default()
        },
        TurnDetectionUpdate {
            interrupt_min_words: Some(11),
            ..TurnDetectionUpdate::default()
        },
        TurnDetectionUpdate {
            interrupt_phrases: Some(vec!["stop".into(); 101]),
            ..TurnDetectionUpdate::default()
        },
        TurnDetectionUpdate {
            acknowledgment_phrases: Some(vec!["yes".into(); 101]),
            ..TurnDetectionUpdate::default()
        },
    ];
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    for invalid in invalid_interrupts {
        let detection = TurnDetection {
            interrupt_min_words: invalid.interrupt_min_words,
            interrupt_phrases: invalid.interrupt_phrases.clone(),
            acknowledgment_phrases: invalid.acknowledgment_phrases.clone(),
            ..TurnDetection::default()
        };
        assert_eq!(
            client::settings(&RealtimeConnectOptions::default().turn_detection(detection))
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Input
        );
        assert_eq!(
            session
                .update(RealtimeUpdateOptions::default().turn_detection(invalid))
                .await
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Input
        );
    }
    assert_eq!(
        client::settings(
            &RealtimeConnectOptions::default().turn_detection(TurnDetection {
                end_of_turn_min_silence_ms: Some(5001),
                ..TurnDetection::default()
            })
        )
        .unwrap_err()
        .kind(),
        crate::ErrorKind::Input
    );
    peer.no_request().await;
    session.close().await;

    for (words, silence, count) in [(1, 0, 0), (10, 5000, 100)] {
        let phrases = vec!["word".into(); count];
        let encoded = client::settings(&RealtimeConnectOptions::default().turn_detection(
            TurnDetection {
                interrupt_min_words: Some(words),
                end_of_turn_min_silence_ms: Some(silence),
                end_of_turn_timeout_ms: Some(10000),
                interrupt_phrases: Some(phrases.clone()),
                acknowledgment_phrases: Some(phrases.clone()),
                ..TurnDetection::default()
            },
        ))
        .unwrap();
        assert_eq!(encoded["turn_detection"]["interrupt_min_words"], words);
        assert_eq!(
            encoded["turn_detection"]["end_of_turn_min_silence_ms"],
            silence
        );
        assert_eq!(
            encoded["turn_detection"]["interrupt_phrases"]
                .as_array()
                .unwrap()
                .len(),
            count
        );
        let patch = client::patch(RealtimeUpdateOptions::default().turn_detection(
            TurnDetectionUpdate {
                interrupt_min_words: Some(words),
                interrupt_phrases: Some(phrases.clone()),
                acknowledgment_phrases: Some(phrases),
            },
        ))
        .unwrap();
        assert_eq!(
            patch["turn_detection"]["acknowledgment_phrases"]
                .as_array()
                .unwrap()
                .len(),
            count
        );
    }
}

#[test]
fn received_turn_settings_reject_out_of_range_values() {
    for (field, value) in [
        ("interrupt_min_words", json!(0)),
        ("interrupt_min_words", json!(11)),
        ("end_of_turn_min_silence_ms", json!(5001)),
        ("interrupt_phrases", json!(vec!["stop"; 101])),
        ("acknowledgment_phrases", json!(vec!["yes"; 101])),
    ] {
        let mut detection = json!({});
        detection[field] = value;
        assert!(
            protocol::decode(
                &json!({"type":"session.updated","session":{"id":"s","turn_detection":detection}})
                    .to_string(),
                "s"
            )
            .is_err(),
            "{field}"
        );
    }
}

#[tokio::test]
async fn unsolicited_review_fails_the_session_without_sending_a_result() {
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    let mut events = session.events().unwrap();
    peer.send(review_event("rev-unconfigured", 0)).await;
    let error = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Stream);
    assert!(error.to_string().contains("without a configured callback"));
    peer.closed_without_request().await;
    session.close().await;
}

#[tokio::test]
async fn confirmed_settings_timestamps_and_transcript_reach_public_interfaces() {
    let mut peer = Peer::with_version_on_creation("created-build").await;
    let (_client, session) = peer.setup().await;
    assert_eq!(session.info().version.as_deref(), Some("created-build"));
    let mut events = session.events().unwrap();
    let update = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .update(RealtimeUpdateOptions::default().speed(1.1))
                .await
        })
    };
    let request = peer.next("session.update").await;
    peer.send(json!({"type":"session.updated","prism_request_event_id":request["event_id"],"session":{"id":"session-1","speed":1.1,"version":"20261104","turn_detection":{"interrupt_min_words":2,"interrupt_phrases":[],"acknowledgment_phrases":[]}}})).await;
    assert_eq!(update.await.unwrap().unwrap().speed, Some(1.1));
    assert_eq!(session.info().version.as_deref(), Some("20261104"));
    let update = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .update(RealtimeUpdateOptions::default().voice("other"))
                .await
        })
    };
    let request = peer.next("session.update").await;
    peer.send(json!({"type":"session.updated","prism_request_event_id":request["event_id"],"session":{"id":"session-1","voice":"other"}})).await;
    assert_eq!(
        update.await.unwrap().unwrap().version.as_deref(),
        Some("20261104")
    );
    for fields in [
        json!({"delta":"hello"}),
        json!({"delta":"","transcript":""}),
    ] {
        let mut message = json!({"type":"conversation.item.input_audio_transcription.delta","event_id":"evt_transcript","item_id":"item-1","content_index":0,"server_time_ms":1792857632418_u64});
        message
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        peer.send(message).await;
        let received = event(&mut events).await;
        assert_eq!(received.server_time_ms, Some(1792857632418));
        match received.payload {
            RealtimeEvent::TranscriptDelta {
                delta, transcript, ..
            } => {
                assert_eq!(delta, fields["delta"]);
                assert_eq!(
                    transcript.as_deref(),
                    fields.get("transcript").and_then(Value::as_str)
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    session.close().await;
}

#[tokio::test]
async fn long_text_reaches_server_on_all_three_paths_and_errors_remain_server_errors() {
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    let long = "🦀".repeat(4001);
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    for proactive in [false, true] {
        let task = {
            let session = session.clone();
            let text = long.clone();
            tokio::spawn(async move {
                if proactive {
                    session
                        .request_reply(ReplyOptions::default().instruction(text))
                        .await
                } else {
                    session.send_text(text).await
                }
            })
        };
        let request = peer.next("response.create").await;
        let field = if proactive {
            "prism_instruction"
        } else {
            "prism_input_text"
        };
        assert_eq!(request["response"][field], long);
        let code = if proactive {
            "invalid_instruction"
        } else {
            "invalid_typed_input"
        };
        peer.send(json!({"type":"error","error":{"code":code,"scope":"event","owner":{"event_id":request["event_id"]}}})).await;
        assert_eq!(
            task.await
                .unwrap()
                .unwrap_err()
                .realtime_fault()
                .unwrap()
                .code,
            code
        );
    }
    let task = {
        let session = session.clone();
        let text = long.clone();
        tokio::spawn(async move {
            session
                .create_item(
                    HistoryItem::Message {
                        id: None,
                        role: HistoryRole::User,
                        text,
                    },
                    None,
                )
                .await
        })
    };
    let request = peer.next("conversation.item.create").await;
    assert_eq!(request["item"]["text"], long);
    peer.send(json!({"type":"conversation.item.created","prism_request_event_id":request["event_id"],"item":{"id":"history-1","type":"message"}})).await;
    assert_eq!(task.await.unwrap().unwrap().item_id(), "history-1");
    session.close().await;
}

fn review_event(id: &str, index: u64) -> Value {
    json!({"type":"prism.review.requested","review_id":id,"response_id":"provisional","item_id":"item-1","index":index,"text":"Proposed words."})
}

#[tokio::test]
async fn review_is_concurrent_before_response_creation_and_needs_no_event_consumer_or_ack() {
    let mut peer = Peer::new().await;
    let release = Arc::new(Notify::new());
    let callback_release = release.clone();
    let review = Review::new(move |request| {
        let release = callback_release.clone();
        async move {
            if request.index == 0 {
                release.notified().await;
            }
            Ok(ReviewDecision::Allow)
        }
    });
    let (_client, session) = peer
        .setup_with(RealtimeConnectOptions::default().review(review))
        .await;
    peer.send(review_event("rev-0", 0)).await;
    peer.send(review_event("rev-1", 1)).await;
    let second = peer.next("prism.review.result").await;
    assert_eq!(second["review_id"], "rev-1");
    release.notify_one();
    let first = peer.next("prism.review.result").await;
    assert_eq!(first["review_id"], "rev-0");
    assert_eq!(first["action"], "allow");
    peer.send(review_event("rev-1", 1)).await;
    peer.no_request().await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // A missing result ACK must not terminate the session.
    let mut events = session.events().unwrap();
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::InputReady
    ));
    session.close().await;
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn timeout_and_shutdown_drop_owned_review_futures_without_sending_results() {
    for expire in [false, true] {
        let mut peer = Peer::new().await;
        let dropped = Arc::new(AtomicUsize::new(0));
        let callback_dropped = dropped.clone();
        let started = Arc::new(Notify::new());
        let callback_started = started.clone();
        let review = Review::new(move |_| {
            let dropped = callback_dropped.clone();
            let started = callback_started.clone();
            async move {
                let _guard = Dropped(dropped);
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(ReviewDecision::Allow)
            }
        })
        .timeout_ms(100)
        .on_timeout(ReviewTimeout::Speak);
        let (_client, session) = peer
            .setup_with(RealtimeConnectOptions::default().review(review))
            .await;
        peer.send(review_event("rev-pending", 0)).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        if expire {
            tokio::time::timeout(Duration::from_secs(1), async {
                while dropped.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            peer.no_request().await;
        }
        session.close().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn callback_error_sends_stop_before_failing_the_session() {
    let mut peer = Peer::new().await;
    let review = Review::new(|_| async { Err(crate::Error::cancelled()) });
    let (_client, session) = peer
        .setup_with(RealtimeConnectOptions::default().review(review))
        .await;
    let mut events = session.events().unwrap();
    peer.send(review_event("rev-error", 0)).await;
    let result = peer.next("prism.review.result").await;
    assert_eq!(result["action"], "stop");
    let error = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("review callback failed"));
    session.close().await;
}

#[tokio::test]
async fn replacement_cancels_later_reviews_but_keeps_earlier_reviews() {
    let mut peer = Peer::new().await;
    let release_early = Arc::new(Notify::new());
    let release_replace = Arc::new(Notify::new());
    let late_started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let review = {
        let early = release_early.clone();
        let replace = release_replace.clone();
        let late = late_started.clone();
        let dropped = dropped.clone();
        Review::new(move |request| {
            let early = early.clone();
            let replace = replace.clone();
            let late = late.clone();
            let dropped = dropped.clone();
            async move {
                match request.index {
                    0 => {
                        early.notified().await;
                        Ok(ReviewDecision::Allow)
                    }
                    1 => {
                        replace.notified().await;
                        Ok(ReviewDecision::Replace("Approved text.".into()))
                    }
                    _ => {
                        let _guard = Dropped(dropped);
                        late.notify_one();
                        std::future::pending().await
                    }
                }
            }
        })
        .timeout_ms(5000)
    };
    let (_client, session) = peer
        .setup_with(RealtimeConnectOptions::default().review(review))
        .await;
    for index in 0..3 {
        peer.send(review_event(&format!("rev-{index}"), index))
            .await;
    }
    tokio::time::timeout(Duration::from_secs(1), late_started.notified())
        .await
        .unwrap();
    release_replace.notify_one();
    let result = peer.next("prism.review.result").await;
    assert_eq!(result["review_id"], "rev-1");
    assert_eq!(result["text"], "Approved text.");
    release_early.notify_one();
    assert_eq!(peer.next("prism.review.result").await["review_id"], "rev-0");
    peer.no_request().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    session.close().await;
}

#[tokio::test]
async fn bounded_reviews_fail_without_leaving_owned_futures() {
    let mut peer = Peer::new().await;
    let dropped = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    let review = {
        let dropped = dropped.clone();
        let running = running.clone();
        Review::new(move |_| {
            let dropped = dropped.clone();
            let running = running.clone();
            async move {
                let _guard = Dropped(dropped);
                running.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        })
        .timeout_ms(5000)
    };
    let (_client, session) = peer
        .setup_with(RealtimeConnectOptions::default().review(review))
        .await;
    let mut events = session.events().unwrap();
    for index in 0..129 {
        peer.send(review_event(&format!("rev-{index}"), index))
            .await;
    }
    let error = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::ResourceLimit);
    session.close().await;
    assert!(running.load(Ordering::SeqCst) <= 128);
    assert_eq!(
        running.load(Ordering::SeqCst),
        dropped.load(Ordering::SeqCst)
    );
    peer.closed_without_request().await;
}

#[tokio::test]
async fn invalid_replacement_stops_instead_of_speaking_on_timeout() {
    let mut peer = Peer::new().await;
    let review = Review::new(|_| async { Ok(ReviewDecision::Replace("  ".into())) })
        .on_timeout(ReviewTimeout::Speak);
    let (_client, session) = peer
        .setup_with(RealtimeConnectOptions::default().review(review))
        .await;
    let mut events = session.events().unwrap();
    peer.send(review_event("rev-invalid", 0)).await;
    assert_eq!(peer.next("prism.review.result").await["action"], "stop");
    let error = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("blank replacement"));
    session.close().await;
}
