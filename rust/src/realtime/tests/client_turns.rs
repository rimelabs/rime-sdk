//! Client-ended turns: the client starts and ends each user turn and asks for the reply.
use super::peer::{event, Peer};
use crate::realtime::client;
use crate::{
    AudioChunk, ErrorKind, PcmFormat, RealtimeConnectOptions, RealtimeEvent, RealtimeSession,
    RealtimeUpdateOptions, ReplyRequestOptions, TurnDetection, TurnDetectionUpdate,
};
use serde_json::json;
use std::time::Duration;

async fn client_turns() -> (Peer, crate::Client, RealtimeSession) {
    let mut peer = Peer::with_client_ended_turns().await;
    let (client, session) = peer
        .setup_with(RealtimeConnectOptions::default().client_ends_turns(true))
        .await;
    assert_eq!(
        peer.update().await["turn_detection"],
        serde_json::Value::Null
    );
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    (peer, client, session)
}

/// End the turn in the background, since end_turn waits for Prism's answer.
fn end_turn(session: &RealtimeSession) -> tokio::task::JoinHandle<Result<(), crate::Error>> {
    let session = session.clone();
    tokio::spawn(async move { session.end_turn().await })
}

async fn committed(peer: &Peer) {
    peer.send(json!({"type":"input_audio_buffer.committed","item_id":"turn-1"}))
        .await;
}

#[tokio::test]
async fn client_ended_turns_send_null_turn_detection() {
    let (_peer, _client, session) = client_turns().await;
    let info = session.info();
    assert!(info.client_ends_turns);
    assert!(!info.interrupt_on_speech);
}

#[tokio::test]
async fn prism_ends_turns_by_default() {
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    assert_eq!(
        peer.update().await["turn_detection"],
        json!({"interrupt_response":true})
    );
    assert!(!session.info().client_ends_turns);
}

#[tokio::test]
async fn push_to_talk_round_trip() {
    let (mut peer, _client, session) = client_turns().await;
    session.start_turn().await.unwrap();
    let start = peer.next("prism.input_audio_buffer.start").await;
    assert_eq!(start.as_object().unwrap().len(), 2);
    session
        .send_audio(AudioChunk {
            data: vec![0; 640].into(),
            format: PcmFormat::default(),
        })
        .await
        .unwrap();
    peer.next("input_audio_buffer.append").await;
    let ending = end_turn(&session);
    let commit = peer.next("input_audio_buffer.commit").await;
    assert_eq!(commit.as_object().unwrap().len(), 2);
    committed(&peer).await;
    ending.await.unwrap().unwrap();
    let pending = tokio::spawn(async move { session.reply_to_turns().await });
    let request = peer.next("response.create").await;
    assert_eq!(request["response"], json!({}));
    peer.accept(&request, "reply-1").await;
    assert_eq!(pending.await.unwrap().unwrap().response_id(), "reply-1");
}

#[tokio::test]
async fn reply_to_turns_carries_labels_without_a_cause() {
    let (mut peer, _client, session) = client_turns().await;
    let labels = [("order".to_string(), "42".to_string())]
        .into_iter()
        .collect();
    let pending = tokio::spawn(async move {
        session
            .reply_to_turns_with_options(ReplyRequestOptions::default().metadata(labels))
            .await
    });
    let request = peer.next("response.create").await;
    assert_eq!(request["response"], json!({"metadata":{"order":"42"}}));
    peer.accept(&request, "reply-1").await;
    pending.await.unwrap().unwrap();
}

#[tokio::test]
async fn reply_to_turns_waits_for_the_active_reply() {
    let (mut peer, _client, session) = client_turns().await;
    let pending = tokio::spawn(async move { session.reply_to_turns().await });
    let first = peer.next("response.create").await;
    peer.send(json!({"type":"error","error":{"code":"conversation_already_has_active_response","scope":"event","owner":{"event_id":first["event_id"]}}}))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    let second = peer.next("response.create").await;
    assert_ne!(second["event_id"], first["event_id"]);
    peer.accept(&second, "reply-1").await;
    pending.await.unwrap().unwrap();
}

#[tokio::test]
async fn end_turn_waits_until_prism_commits_the_turn() {
    let (mut peer, _client, session) = client_turns().await;
    let ending = end_turn(&session);
    peer.next("input_audio_buffer.commit").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!ending.is_finished());
    committed(&peer).await;
    ending.await.unwrap().unwrap();
}

/// A press under 100 ms is refused with an error naming the commit. Speech
/// over the pending audio budget is refused with an error naming nothing.
#[tokio::test]
async fn end_turn_fails_when_prism_refuses_the_commit() {
    for code in ["input_audio_buffer_commit_empty", "pending_speech_overflow"] {
        let (mut peer, _client, session) = client_turns().await;
        let mut events = session.events().unwrap();
        let ending = end_turn(&session);
        let commit = peer.next("input_audio_buffer.commit").await;
        let owner = if code == "input_audio_buffer_commit_empty" {
            json!({"event_id":commit["event_id"]})
        } else {
            json!({})
        };
        peer.send(json!({"type":"error","error":{"code":code,"scope":"event","owner":owner}}))
            .await;
        let error = tokio::time::timeout(Duration::from_secs(3), ending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.realtime_fault().map(|f| f.code.as_str()), Some(code));
        loop {
            if let RealtimeEvent::Fault(fault) = event(&mut events).await.payload {
                assert_eq!(fault.code, code);
                break;
            }
        }
        // The session stays usable.
        let ending = end_turn(&session);
        peer.next("input_audio_buffer.commit").await;
        committed(&peer).await;
        ending.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn end_turn_also_ends_a_turn_prism_detects() {
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    session.end_turn().await.unwrap();
    peer.next("input_audio_buffer.commit").await;
}

#[tokio::test]
async fn client_commit_is_a_known_end_reason() {
    let (peer, _client, session) = client_turns().await;
    let mut events = session.events().unwrap();
    peer.send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"speech","audio_end_ms":1200,"prism_end_reason":"client_commit"}))
        .await;
    loop {
        if let RealtimeEvent::SpeechStopped { end_reason, .. } = event(&mut events).await.payload {
            assert_eq!(end_reason.as_deref(), Some("client_commit"));
            break;
        }
    }
}

#[tokio::test]
async fn turn_starts_and_replies_need_client_ended_turns() {
    let mut peer = Peer::new().await;
    let (_client, session) = peer.setup().await;
    assert_eq!(
        session.start_turn().await.unwrap_err().kind(),
        ErrorKind::Input
    );
    assert_eq!(
        session.reply_to_turns().await.unwrap_err().kind(),
        ErrorKind::Input
    );
    peer.no_request().await;
}

#[test]
fn client_ended_turns_refuse_turn_settings() {
    for options in [
        RealtimeConnectOptions::default()
            .client_ends_turns(true)
            .turn_detection(TurnDetection {
                eagerness: Some("low".into()),
                ..TurnDetection::default()
            }),
        RealtimeConnectOptions::default()
            .client_ends_turns(true)
            .interrupt_on_speech(false),
    ] {
        assert_eq!(
            client::settings(&options).unwrap_err().kind(),
            ErrorKind::Input
        );
    }
}

#[tokio::test]
async fn client_ended_turns_refuse_later_turn_settings() {
    let (mut peer, _client, session) = client_turns().await;
    for options in [
        RealtimeUpdateOptions::default().interrupt_on_speech(true),
        RealtimeUpdateOptions::default().turn_detection(TurnDetectionUpdate {
            interrupt_min_words: Some(2),
            ..TurnDetectionUpdate::default()
        }),
    ] {
        assert_eq!(
            session.update(options).await.unwrap_err().kind(),
            ErrorKind::Input
        );
    }
    peer.no_request().await;
}

#[tokio::test]
async fn reply_to_turns_waits_for_readiness_after_the_turn() {
    // Prism's own order: the turn's start clears readiness, and its end restores it.
    let (mut peer, _client, session) = client_turns().await;
    let mut events = session.events().unwrap();
    session.start_turn().await.unwrap();
    peer.next("prism.input_audio_buffer.start").await;
    peer.send(
        json!({"type":"input_audio_buffer.speech_started","item_id":"turn-1","audio_start_ms":0}),
    )
    .await;
    session
        .send_audio(AudioChunk {
            data: vec![0; 640].into(),
            format: PcmFormat::default(),
        })
        .await
        .unwrap();
    peer.next("input_audio_buffer.append").await;
    let ending = end_turn(&session);
    peer.next("input_audio_buffer.commit").await;
    peer.send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"turn-1","audio_end_ms":40,"prism_end_reason":"client_commit"}))
        .await;
    committed(&peer).await;
    ending.await.unwrap().unwrap();
    while !matches!(
        event(&mut events).await.payload,
        RealtimeEvent::InputCommitted { .. }
    ) {}
    let pending = tokio::spawn(async move { session.reply_to_turns().await });
    // No reply request before Prism is ready.
    peer.no_request().await;
    peer.send(json!({"type":"prism.typed_input.ready"})).await;
    let request = peer.next("response.create").await;
    assert_eq!(request["response"], json!({}));
    peer.accept(&request, "reply-1").await;
    assert_eq!(pending.await.unwrap().unwrap().response_id(), "reply-1");
}

#[tokio::test]
async fn turn_controls_follow_audio_sent_before_them() {
    for kind in [
        "prism.input_audio_buffer.start",
        "input_audio_buffer.commit",
    ] {
        let (mut peer, _client, session) = client_turns().await;
        // A clear waiting for its acknowledgment holds back the audio sent after it.
        let s = session.clone();
        let clearing = tokio::spawn(async move { s.clear_audio().await });
        peer.next("input_audio_buffer.clear").await;
        let s = session.clone();
        let sending = tokio::spawn(async move {
            s.send_audio(AudioChunk {
                data: vec![0; 3840].into(),
                format: PcmFormat::default(),
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let s = session.clone();
        let controlling = tokio::spawn(async move {
            if kind == "prism.input_audio_buffer.start" {
                s.start_turn().await
            } else {
                s.end_turn().await
            }
        });
        // The turn control must not overtake the audio sent before it.
        peer.no_request().await;
        peer.send(json!({"type":"input_audio_buffer.cleared"}))
            .await;
        // 120 ms of audio is three 40 ms appends.
        for _ in 0..3 {
            peer.next("input_audio_buffer.append").await;
        }
        peer.next(kind).await;
        if kind == "input_audio_buffer.commit" {
            committed(&peer).await;
        }
        clearing.await.unwrap().unwrap();
        sending.await.unwrap().unwrap();
        controlling.await.unwrap().unwrap();
    }
}
