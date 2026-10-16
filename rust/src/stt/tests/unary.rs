use super::*;
use crate::{ErrorKind, TranscribeOptions, TranscriptionMode};

#[tokio::test]
async fn sends_complete_recording_and_options() {
    let h = Harness::new(Mode::Echo).await;
    let audio = vec![42; 256 * 1024];
    let result = h
        .client
        .stt()
        .transcribe(
            &audio,
            TranscribeOptions::new("en-GB")
                .mode(TranscriptionMode::Verbatim)
                .context_terms(vec![" Super-G ".into(), "uno,dos".into(), "Español".into()]),
        )
        .await
        .unwrap();
    assert_eq!(result.text, "Ice cream");
    assert_eq!(result.language, "en");
    assert_eq!(result.request_id.as_deref(), Some("test-stt"));
    let calls = h.service.recordings.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].audio, audio);
    let options = calls[0].options.as_ref().unwrap();
    assert_eq!(options.language.as_deref(), Some("en-GB"));
    assert_eq!(options.mode, pb::TranscriptionMode::Verbatim as i32);
    assert_eq!(options.context_terms, [" Super-G ", "uno,dos", "Español"]);
    assert!(h.service.configs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failures_preserve_identity_without_retry() {
    for (mode, kind, id) in [
        (
            Mode::Reject(tonic::Code::Unavailable),
            ErrorKind::Unavailable,
            "rejected-stt",
        ),
        (Mode::MissingFinal, ErrorKind::Stream, "test-stt"),
    ] {
        let h = Harness::new(mode).await;
        let error = h
            .client
            .stt()
            .transcribe(b"wav", TranscribeOptions::new("en"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.request_id(), Some(id));
        assert_eq!(h.service.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn silence_and_written_default() {
    let h = Harness::new(Mode::EarlyFinal).await;
    let result = h
        .client
        .stt()
        .transcribe(b"wav", TranscribeOptions::new("en"))
        .await
        .unwrap();
    assert_eq!(result.text, "");
    assert_eq!(
        h.service.recordings.lock().unwrap()[0]
            .options
            .as_ref()
            .unwrap()
            .mode,
        pb::TranscriptionMode::Written as i32
    );
}

#[tokio::test]
async fn cancellation_reaches_server() {
    for action in ["drop", "close", "deadline", "rpc-deadline"] {
        let mut h = Harness::new(Mode::NeverComplete).await;
        if action == "rpc-deadline" {
            Arc::get_mut(&mut h.client.inner)
                .unwrap()
                .stt_limits
                .completion = Duration::from_millis(80);
        }
        let client = h.client.clone();
        let options = TranscribeOptions::new("en").timeout(if action == "deadline" {
            Some(Duration::from_millis(80))
        } else {
            None
        });
        let task = tokio::spawn(async move { client.stt().transcribe(b"wav", options).await });
        tokio::time::timeout(Duration::from_secs(3), h.service.completed.notified())
            .await
            .unwrap();
        match action {
            "drop" => {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
            "close" => {
                h.client.close().await;
                assert_eq!(
                    task.await.unwrap().unwrap_err().kind(),
                    ErrorKind::Cancelled
                );
            }
            _ => {
                // Pause only after network setup; the configured deadline must
                // expire before the harness's default 200 ms completion budget.
                tokio::time::pause();
                let error = tokio::time::timeout(Duration::from_millis(100), task)
                    .await
                    .expect("configured deadline did not cancel the RPC")
                    .unwrap()
                    .unwrap_err();
                tokio::time::resume();
                assert_eq!(error.kind(), ErrorKind::Timeout);
                assert_eq!(
                    error.message(),
                    if action == "deadline" {
                        "overall transcription deadline expired"
                    } else {
                        "transcription request timed out"
                    }
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(3), h.service.cancelled.notified())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn rejects_invalid_input_without_request() {
    let h = Harness::new(Mode::Echo).await;
    for (audio, kind) in [
        (vec![], ErrorKind::AudioFormat),
        (vec![0; 16 * 1024 * 1024 + 1], ErrorKind::ResourceLimit),
    ] {
        let error = h
            .client
            .stt()
            .transcribe(audio, TranscribeOptions::new("en"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
    }
    assert_eq!(h.service.calls.load(Ordering::SeqCst), 0);
}
