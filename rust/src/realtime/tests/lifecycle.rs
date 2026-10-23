use super::peer::{event, Peer};
use crate::{realtime::RealtimeEvent, ErrorKind};
use futures_util::StreamExt;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn dropped_future_before_submission_sends_nothing() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.send_text("hello").await });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    p.no_request().await;
    c.close().await;
}
#[tokio::test]
async fn late_accepted_reply_after_future_drop_is_cancelled() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.send_text("hello").await });
    let q = p.next("response.create").await;
    task.abort();
    let _ = task.await;
    p.accept(&q, "late").await;
    let q = p.next("response.cancel").await;
    assert_eq!(q["response_id"], "late");
    p.ended("late").await;
    loop {
        if matches!(
            event(&mut events).await.payload,
            RealtimeEvent::ResponseAbandoned { .. }
        ) {
            break;
        }
    }
    c.close().await;
}
#[tokio::test]
async fn parent_close_and_last_handle_drop_end_events() {
    for parent in [true, false] {
        let p = Peer::new().await;
        let (c, s) = p.setup().await;
        let mut events = s.events().unwrap();
        if parent {
            c.close().await;
        } else {
            drop(s);
        }
        assert!(tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .is_none());
        c.close().await;
    }
}
#[tokio::test]
async fn queue_overflow_is_terminal() {
    let p = Peer::new().await;
    let (c, s) = p.setup().await;
    for _ in 0..257 {
        p.send(json!({"type":"prism.typed_input.ready"})).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut events = s.events().unwrap();
    let error = events.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
    c.close().await;
}
#[tokio::test]
async fn malformed_event_and_connection_loss_release_pending_requests() {
    for malformed in [true, false] {
        let mut p = Peer::new().await;
        let (c, s) = p.setup().await;
        let session = s.clone();
        let task = tokio::spawn(async move { session.add_message("user", "history").await });
        p.next("conversation.item.create").await;
        if malformed {
            p.send(json!({"type":"response.text.delta"})).await
        } else {
            p.disconnect()
        }
        let error = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Stream);
        c.close().await;
    }
}
#[tokio::test]
async fn structured_fault_reaches_caller() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.add_message("user", "history").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"error","error":{"scope":"event","code":"invalid","message":"bad history","owner":{"event_id":q["event_id"]}}})).await;
    let e = task.await.unwrap().unwrap_err();
    assert_eq!(e.realtime_fault().unwrap().code, "invalid");
    c.close().await;
}

#[tokio::test]
async fn admission_does_not_block_controls() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let waiting = tokio::spawn(async move { session.send_text("waiting for readiness").await });
    let session = s.clone();
    let clear = tokio::spawn(async move { session.clear_audio().await });
    p.next("input_audio_buffer.clear").await;
    p.send(json!({"type":"input_audio_buffer.cleared"})).await;
    clear.await.unwrap().unwrap();
    waiting.abort();
    let _ = waiting.await;
    c.close().await;
}
#[tokio::test]
async fn dropped_clear_keeps_uncorrelated_ack_matching() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let first = tokio::spawn(async move { session.clear_audio().await });
    p.next("input_audio_buffer.clear").await;
    first.abort();
    let _ = first.await;
    let session = s.clone();
    let mut second = tokio::spawn(async move { session.clear_audio().await });
    p.no_request().await;
    p.send(json!({"type":"input_audio_buffer.cleared"})).await;
    p.next("input_audio_buffer.clear").await;
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut second)
        .await
        .is_err());
    p.send(json!({"type":"input_audio_buffer.cleared"})).await;
    second.await.unwrap().unwrap();
    c.close().await;
}
#[tokio::test]
async fn unknown_acknowledgment_outcome_closes_without_replay() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.add_message("user", "history").await });
    p.next("conversation.item.create").await;
    let e = task.await.unwrap().unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Timeout);
    c.close().await;
}
#[tokio::test]
async fn busy_waits_for_new_readiness() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.send_text("hello").await });
    let q = p.next("response.create").await;
    p.send(json!({"type":"error","error":{"scope":"event","code":"typed_turn_busy","owner":{"event_id":q["event_id"]}}})).await;
    p.no_request().await;
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    let q = p.next("response.create").await;
    p.accept(&q, "retry").await;
    task.await.unwrap().unwrap();
    c.close().await;
}
#[tokio::test]
async fn mismatched_acknowledgment_is_terminal() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let task = tokio::spawn(async move { session.add_message("user", "history").await });
    let q = p.next("conversation.item.create").await;
    p.accept(&q, "wrong").await;
    assert_eq!(task.await.unwrap().unwrap_err().kind(), ErrorKind::Stream);
    c.close().await;
}

#[tokio::test]
async fn blocked_write_still_processes_events() {
    let p = Peer::with_paused_reads(true).await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    p.send(json!({"type":"response.created","response":{"id":"r1"}}))
        .await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":"r1","item_id":"tool","call_id":"call-1","name":"tool","arguments":"{}"})).await;
    let RealtimeEvent::ToolCall { call, .. } = event(&mut events).await.payload else {
        panic!("missing tool")
    };
    let session = s.clone();
    let task = tokio::spawn(async move {
        session
            .submit_tool_result(&call, "x".repeat(32 * 1024 * 1024))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    p.send(json!({"type":"prism.input_audio_buffer.speech_hint","audio_start_ms":12}))
        .await;
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::SpeechHint { .. }
    ));
    let error = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Timeout | ErrorKind::Stream
    ));
    c.close().await;
}

#[tokio::test]
async fn terminal_failure_precedes_queued_events_and_reaches_later_operations() {
    let p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    p.send(json!({"type":"prism.typed_input.ready"})).await;
    p.send(json!({"type":"response.text.delta"})).await;
    // A request after the malformed event waits for the worker to publish its failure.
    let error = s.add_message("user", "barrier").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Stream);
    s.close().await;
    let error = s.send_text("after failure").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Stream);
    assert_eq!(
        events.next().await.unwrap().unwrap_err().kind(),
        ErrorKind::Stream
    );
    assert!(events.next().await.is_none());
    c.close().await;
}

#[tokio::test]
async fn queued_audio_drop_preserves_other_work() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let copy = s.clone();
    let clear = tokio::spawn(async move { copy.clear_audio().await });
    p.next("input_audio_buffer.clear").await;
    let copy = s.clone();
    let audio = tokio::spawn(async move {
        copy.send_audio(crate::AudioChunk {
            data: vec![0; 2560].into(),
            format: Default::default(),
        })
        .await
    });
    tokio::task::yield_now().await;
    audio.abort();
    let _ = audio.await;
    p.send(json!({"type":"input_audio_buffer.cleared"})).await;
    clear.await.unwrap().unwrap();
    p.turn(&s, "unrelated").await;
    c.close().await;
}

#[tokio::test]
async fn active_audio_future_drop_closes_session() {
    let p = Peer::with_paused_reads(true).await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    // Fill the socket buffers without relying on a particular OS buffer size.
    for _ in 0..128 {
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            s.send_audio(crate::AudioChunk {
                data: vec![0; 192000].into(),
                format: Default::default(),
            }),
        )
        .await;
        match result {
            Ok(result) => result.unwrap(),
            Err(_) => {
                let error = tokio::time::timeout(Duration::from_millis(500), events.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Stream);
                c.close().await;
                return;
            }
        }
    }
    panic!("peer did not apply socket backpressure");
}

#[tokio::test]
async fn full_command_queue_does_not_block_drop_cancellation() {
    use futures_util::poll;
    let p = Peer::with_paused_reads(true).await;
    let (client, session) = p.setup().await;
    let mut events = session.events().unwrap();
    p.send(json!({"type":"response.created","response":{"id":"r1"}}))
        .await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":"r1","item_id":"tool","call_id":"call-1","name":"tool","arguments":"{}"})).await;
    let RealtimeEvent::ToolCall { call, .. } = event(&mut events).await.payload else {
        panic!("missing tool")
    };
    // Unlike audio, dropping this submitted tool operation retains its write.
    // The last session handle must independently stop that blocked writer.
    let mut active = Box::pin(session.submit_tool_result(&call, "x".repeat(32 * 1024 * 1024)));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), active.as_mut())
            .await
            .is_err()
    );
    // This is a single-thread runtime: no await between filling the channel,
    // dropping all pending futures, and dropping the last session handle.
    let mut queued = Vec::new();
    for _ in 0..128 {
        let mut request = Box::pin(session.send_text("queued"));
        assert!(poll!(request.as_mut()).is_pending());
        queued.push(request);
    }
    assert_eq!(
        session
            .add_message("user", "overflow")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ResourceLimit
    );
    drop(queued);
    drop(active);
    drop(session);
    // No event consumption was needed to deliver any cancellation or close signal.
    // Require normal closure before the one-second request timeout, so an
    // eventual failed write cannot hide a lost last-handle cancellation.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), events.next())
            .await
            .unwrap()
            .is_none()
    );
    client.close().await;
}
