use super::peer::{event, Peer};
use crate::{
    realtime::{AudioChunk, PlaybackReport, RealtimeEvent},
    ErrorKind, PcmFormat,
};
use serde_json::json;

#[tokio::test]
async fn text_history_and_playback() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    assert!(s.events().is_err());
    let response = p.turn(&s, "reply-1").await;
    assert_eq!(response.session_id(), "session-1");
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::InputReady
    ));
    assert!(matches!(
        event(&mut events).await.payload,
        RealtimeEvent::ResponseStarted { .. }
    ));
    let session = s.clone();
    let task = tokio::spawn(async move { session.add_message("assistant", "history").await });
    let q = p.next("conversation.item.create").await;
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"id":"history-1","type":"message"}})).await;
    assert_eq!(task.await.unwrap().unwrap().item_id(), "history-1");
    p.send(json!({"type":"response.output_item.added","response_id":"reply-1","output_index":0,"item":{"type":"message","id":"message-1"}})).await;
    let RealtimeEvent::MessageStarted { output } = event(&mut events).await.payload else {
        panic!("missing message")
    };
    let session = s.clone();
    let task = tokio::spawn(async move {
        session
            .report_playback(PlaybackReport::Interrupted {
                output,
                audio_end_ms: 12,
            })
            .await
    });
    let q = p.next("conversation.item.truncate").await;
    assert_eq!(q["audio_end_ms"], 12);
    p.send(json!({"type":"conversation.item.truncated","item_id":"message-1","content_index":0}))
        .await;
    task.await.unwrap().unwrap();
    p.ended("reply-1").await;
    event(&mut events).await;
    let session = s.clone();
    let task = tokio::spawn(async move {
        session
            .report_playback(PlaybackReport::Finished {
                response,
                played_ms: Some(12.0),
                audible_tail_ms: None,
            })
            .await
    });
    let q = p.next("prism.playback.drained").await;
    assert_eq!(q["drain_token"], "token-reply-1");
    task.await.unwrap().unwrap();
    c.close().await;
}
#[tokio::test]
async fn audio_and_clear() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let session = s.clone();
    let task = tokio::spawn(async move {
        session
            .send_audio(AudioChunk {
                data: vec![0; 2560].into(),
                format: PcmFormat::default(),
            })
            .await
    });
    for _ in 0..2 {
        let q = p.next("input_audio_buffer.append").await;
        assert_eq!(q["audio"].as_str().unwrap().len(), 1708)
    }
    task.await.unwrap().unwrap();
    let session = s.clone();
    let task = tokio::spawn(async move { session.clear_audio().await });
    p.next("input_audio_buffer.clear").await;
    p.send(json!({"type":"input_audio_buffer.cleared"})).await;
    task.await.unwrap().unwrap();
    c.close().await;
}
#[tokio::test]
async fn cancelled_tool_wait_keeps_acknowledgment_and_continuation() {
    let mut p = Peer::new().await;
    let (c, s) = p.setup().await;
    let mut events = s.events().unwrap();
    let parent = p.turn(&s, "reply-1").await;
    event(&mut events).await;
    event(&mut events).await;
    p.send(json!({"type":"response.function_call_arguments.done","response_id":"reply-1","item_id":"tool-item","call_id":"call-1","name":"lookup","arguments":"{}"})).await;
    let RealtimeEvent::ToolCall { call, .. } = event(&mut events).await.payload else {
        panic!("missing tool")
    };
    p.ended("reply-1").await;
    event(&mut events).await;
    let session = s.clone();
    let call_copy = call.clone();
    let task = tokio::spawn(async move { session.submit_tool_result(&call_copy, "result").await });
    let q = p.next("conversation.item.create").await;
    task.abort();
    let _ = task.await;
    let session = s.clone();
    let continuation = tokio::spawn(async move { session.continue_reply(&parent).await });
    p.send(json!({"type":"conversation.item.created","prism_request_event_id":q["event_id"],"item":{"type":"function_call_output","call_id":"call-1"}})).await;
    let q = p.next("response.create").await;
    p.accept(&q, "reply-2").await;
    continuation.await.unwrap().unwrap();
    s.submit_tool_result(&call, "result").await.unwrap();
    assert_eq!(
        s.submit_tool_result(&call, "changed")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Input
    );
    c.close().await;
}
