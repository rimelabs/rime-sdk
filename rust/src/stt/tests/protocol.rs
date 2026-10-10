use super::{accepted, partial};
use crate::{stt::protocol::TranscriptState, Error, ErrorKind, TranscriptionUpdate};
use serde_json::{json, Value};

fn as_json(update: TranscriptionUpdate) -> Value {
    match update {
        TranscriptionUpdate::Partial { text } => json!({"kind": "partial", "text": text}),
        TranscriptionUpdate::Final { text, language } => {
            json!({"kind": "final", "text": text, "language": language})
        }
    }
}
#[test]
fn shared_transcript_cases() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../../../testdata/stt/transcripts.json")).unwrap();
    for case in fixtures["cases"].as_array().unwrap() {
        let run = || -> Result<Vec<Value>, Error> {
            let mut state = TranscriptState::default();
            let mut actual = Vec::new();
            for raw in case["messages"].as_array().unwrap() {
                // Unknown protobuf fields decode to no known payload on the wire.
                let mut raw = raw.clone();
                raw.as_object_mut().unwrap().retain(|key, _| {
                    matches!(key.as_str(), "accepted" | "hypothesis" | "done" | "delta")
                });
                let message = serde_json::from_value(raw).unwrap();
                if let Some(update) =
                    state.accept(message, case["inputDone"].as_bool().unwrap_or(true))?
                {
                    actual.push(as_json(update));
                }
            }
            actual.push(as_json(state.finish()?));
            Ok(actual)
        };
        if case.get("error").is_some() {
            assert_eq!(
                run().unwrap_err().kind(),
                ErrorKind::Stream,
                "{}",
                case["name"]
            );
        } else {
            assert_eq!(
                Value::Array(run().unwrap()),
                case["expected"],
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn transcript_text_limit_counts_utf8_bytes() {
    let mut state = TranscriptState::default();
    state.accept(accepted(), false).unwrap();
    let error = state
        .accept(partial("é".repeat(32_769), 1), false)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
}
