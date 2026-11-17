//! Public Rust event pattern, constructor, and root export compatibility.
//!
//! Event variants and read-only structs are non-exhaustive so that Prism can add
//! fields without breaking callers. Compile this outside the SDK crate to protect
//! the supported ways to read and build them: patterns with `..`, constructors,
//! `with_` setters, and assignment to public fields.

use rimelabs_sdk::{
    RealtimeEvent, RealtimeFault, RealtimeIdleTimeout, ReplyRequestOptions, ResponseUsage,
    ReviewRequest, SessionEvent, SessionInfo,
};

fn match_with_rest_patterns(event: &RealtimeEvent) {
    match event {
        RealtimeEvent::ResponseStarted {
            response: _,
            cause: _,
            ..
        } => {}
        RealtimeEvent::ResponseEnded {
            response: _,
            status: _,
            reason: _,
            ..
        } => {}
        RealtimeEvent::SpeechStopped {
            end_reason: _,
            end_of_turn_min_silence_ms: _,
            ..
        } => {}
        _ => {}
    }
}

#[test]
fn rest_patterns_and_root_exports_compile() {
    let _ = match_with_rest_patterns as fn(&RealtimeEvent);
    let _ = ReplyRequestOptions::default();
    let usage = ResponseUsage::new(10, 5, 15, 32_768);
    assert_eq!(usage.total_tokens, 15);
    let mut truncation = rimelabs_sdk::ResponseTruncation::new(2);
    truncation.first_kept_item_id = Some("item".into());
    assert_eq!(truncation.items_left_out, 2);
    let _ = RealtimeIdleTimeout::new(300.0);
}

#[test]
fn applications_can_build_events_for_tests() {
    use rimelabs_sdk::{Review, ReviewDecision, TurnDetection};
    let detection = TurnDetection {
        interrupt_min_words: Some(2),
        ..TurnDetection::default()
    };
    assert_eq!(detection.interrupt_min_words, Some(2));

    let mut event = SessionEvent::new(
        "session",
        "event",
        RealtimeEvent::transcript_delta("item", "hello").with_transcript("hello"),
    );
    event.request_id = Some("request".into());
    event.server_time_ms = Some(1);
    if let RealtimeEvent::TranscriptDelta {
        item_id,
        delta,
        transcript,
        ..
    } = &event.payload
    {
        assert_eq!(
            (item_id.as_str(), delta.as_str(), transcript.as_deref()),
            ("item", "hello", Some("hello"))
        );
    } else {
        panic!("expected transcript");
    }

    let stopped = RealtimeEvent::speech_stopped("item", 50)
        .with_end_reason("semantic")
        .with_end_of_turn_min_silence_ms(300);
    let RealtimeEvent::SpeechStopped {
        audio_end_ms,
        end_reason,
        end_of_turn_min_silence_ms,
        ..
    } = stopped
    else {
        panic!("expected speech stop");
    };
    assert_eq!(
        (
            audio_end_ms,
            end_reason.as_deref(),
            end_of_turn_min_silence_ms
        ),
        (50, Some("semantic"), Some(300))
    );

    let mut fault = RealtimeFault::new("no_transcript", "No speech");
    fault.scope = "utterance".into();
    let failed = RealtimeEvent::transcript_failed("item", fault);
    assert!(matches!(
        failed,
        RealtimeEvent::TranscriptFailed { ref fault, .. } if fault.scope == "utterance"
    ));

    let mut info = SessionInfo::new("session");
    info.voice = "aurora".into();
    assert_eq!((info.model, info.interrupt_on_speech), ("prism", true));

    let request = ReviewRequest::new("review", "reply", "item", 0, "Hello.");
    assert_eq!(request.text, "Hello.");
    let _ = Review::new(|_| async { Ok(ReviewDecision::Allow) });
}

#[test]
#[should_panic(expected = "with_end_reason applies to SpeechStopped")]
fn optional_field_setters_name_their_variant() {
    let _ = RealtimeEvent::speech_started("item", 10).with_end_reason("semantic");
}
