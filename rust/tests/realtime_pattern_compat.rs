//! Public Rust event pattern and root export compatibility.

use rimelabs_sdk::{RealtimeEvent, RealtimeIdleTimeout, ReplyRequestOptions, ResponseUsage};

// Compile this outside the SDK crate to protect existing exhaustive field patterns.
fn match_previous_fields(event: &RealtimeEvent) {
    match event {
        RealtimeEvent::ResponseStarted {
            settings_revision: _,
            response: _,
            cause: _,
            parent: _,
            input_item_id: _,
            metadata: _,
            input_item_ids: _,
        } => {}
        RealtimeEvent::ResponseEnded {
            settings_revision: _,
            truncation: _,
            response: _,
            status: _,
            reason: _,
        } => {}
        _ => {}
    }
}

#[test]
fn prior_patterns_and_new_root_exports_compile() {
    let _ = match_previous_fields as fn(&RealtimeEvent);
    let _ = ReplyRequestOptions::default();
    let _ = std::mem::size_of::<ResponseUsage>();
    let _ = std::mem::size_of::<RealtimeIdleTimeout>();
}

#[test]
fn migrated_transcript_patterns_and_turn_literals_compile() {
    use rimelabs_sdk::{Review, ReviewDecision, SessionEvent, TurnDetection};
    let detection = TurnDetection {
        interrupt_min_words: Some(2),
        ..TurnDetection::default()
    };
    assert_eq!(detection.interrupt_min_words, Some(2));
    let event = SessionEvent {
        session_id: "session".into(),
        event_id: "event".into(),
        request_id: None,
        server_time_ms: None,
        payload: RealtimeEvent::TranscriptDelta {
            item_id: "item".into(),
            delta: "hello".into(),
            transcript: None,
        },
    };
    if let RealtimeEvent::TranscriptDelta { item_id, delta, .. } = &event.payload {
        assert_eq!((item_id.as_str(), delta.as_str()), ("item", "hello"));
    } else {
        panic!("expected transcript");
    }
    let _ = Review::new(|_| async { Ok(ReviewDecision::Allow) });
}
