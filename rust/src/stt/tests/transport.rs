use super::*;
use crate::{ErrorKind, PcmFormat, TranscriptionMode};
use std::task::Poll;

#[tokio::test]
async fn lazy_duplex_stream_converts_audio_and_preserves_options() {
    let h = Harness::new(Mode::Echo).await;
    let (send, source) = mpsc::channel(2);
    let mut stream = h
        .client
        .stt()
        .stream(
            ReceiverStream::new(source),
            TranscriptionOptions::new("EN")
                .mode(TranscriptionMode::Verbatim)
                .context_terms(vec![" Super-G ".into(), "Rime".into()])
                .input_format(PcmFormat {
                    sample_rate: 8000,
                    channels: 2,
                }),
        )
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(h.service.calls.load(Ordering::SeqCst), 0);
    send.send(Ok(Bytes::from_static(&[0, 0, 2]))).await.unwrap();
    send.send(Ok(Bytes::from_static(&[0, 4, 0, 6, 0])))
        .await
        .unwrap();
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial {
            text: "I scream".into()
        }
    );
    assert!(!h.service.eof.load(Ordering::SeqCst));
    drop(send);
    let updates = collect(&mut stream).await.unwrap();
    assert_eq!(
        updates.last(),
        Some(&TranscriptionUpdate::Final {
            text: "Ice cream".into(),
            language: "en".into()
        })
    );
    assert_eq!(*h.service.audio.lock().unwrap(), vec![1, 0, 3, 0, 5, 0]);
    let config = h.service.configs.lock().unwrap()[0].clone();
    assert_eq!(config.language.as_deref(), Some("EN"));
    assert_eq!(config.mode, pb::TranscriptionMode::Verbatim as i32);
    assert_eq!(config.context_terms, [" Super-G ", "Rime"]);
    assert_eq!(
        config.output_contract,
        pb::StreamingOutputContract::RevisedHypotheses as i32
    );
    assert_eq!(stream.request_id(), Some("test-stt"));
    h.client.close().await;
}

#[tokio::test]
async fn acceptance_timeout_does_not_poll_source() {
    for mode in [Mode::NoAcceptance, Mode::DelayedHeaders] {
        let h = Harness::new(mode).await;
        let polled = Arc::new(AtomicBool::new(false));
        let flag = polled.clone();
        let mut stream = h.stream(futures_util::stream::poll_fn(move |_| {
            flag.store(true, Ordering::SeqCst);
            Poll::Ready(None)
        }));
        assert_eq!(
            next(&mut stream).await.unwrap().unwrap_err().kind(),
            ErrorKind::Timeout
        );
        assert!(!polled.load(Ordering::SeqCst));
        h.client.close().await;
    }
}

#[tokio::test]
async fn invalid_completion_never_emits_final() {
    for (mode, expected) in [
        (Mode::EarlyFinal, ErrorKind::Stream),
        (Mode::MissingFinal, ErrorKind::Stream),
        (Mode::ErrorAfterDone, ErrorKind::Unavailable),
        (Mode::NeverComplete, ErrorKind::Timeout),
    ] {
        let h = Harness::new(mode).await;
        let source = if matches!(mode, Mode::EarlyFinal) {
            futures_util::stream::pending().boxed()
        } else {
            futures_util::stream::empty().boxed()
        };
        let mut stream = h.stream(source);
        let error = loop {
            match next(&mut stream)
                .await
                .expect("failure must end with an error")
            {
                Ok(update) => assert!(matches!(update, TranscriptionUpdate::Partial { .. })),
                Err(error) => break error,
            }
        };
        assert_eq!(error.kind(), expected);
        if matches!(mode, Mode::ErrorAfterDone) {
            assert_eq!(error.request_id(), Some("failed-stt"));
        }
        assert!(next(&mut stream).await.is_none());
        h.client.close().await;
    }
}

#[tokio::test]
async fn rejection_preserves_error_category_and_request_id() {
    for (code, expected) in [
        (tonic::Code::InvalidArgument, ErrorKind::Input),
        (tonic::Code::Unauthenticated, ErrorKind::Authentication),
        (tonic::Code::PermissionDenied, ErrorKind::Permission),
        (tonic::Code::ResourceExhausted, ErrorKind::ResourceLimit),
        (tonic::Code::Unavailable, ErrorKind::Unavailable),
        (tonic::Code::DeadlineExceeded, ErrorKind::Timeout),
        (tonic::Code::Cancelled, ErrorKind::Cancelled),
    ] {
        let h = Harness::new(Mode::Reject(code)).await;
        let mut stream = h.stream(futures_util::stream::empty());
        let error = next(&mut stream).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.request_id(), Some("rejected-stt"));
        assert_eq!(stream.request_id(), Some("rejected-stt"));
        assert_eq!(h.service.calls.load(Ordering::SeqCst), 1);
        h.client.close().await;
    }
}

#[tokio::test]
async fn source_errors_and_truncated_frames_do_not_commit() {
    for error_source in [false, true] {
        let h = Harness::new(Mode::Echo).await;
        let item = if error_source {
            Err(Error::input(std::io::Error::other("test source")))
        } else {
            Ok(Bytes::from_static(&[0]))
        };
        let mut stream = h.stream(futures_util::stream::iter([item]));
        let expected = if error_source {
            ErrorKind::Input
        } else {
            ErrorKind::AudioFormat
        };
        assert_eq!(
            next(&mut stream).await.unwrap().unwrap_err().kind(),
            expected
        );
        stream.close().await;
        assert!(!h.service.eof.load(Ordering::SeqCst));
        h.client.close().await;
    }
}

#[tokio::test]
async fn validates_options_before_starting_work() {
    let client = Client::builder().api_key("test-key").build().unwrap();
    for format in [
        PcmFormat {
            sample_rate: 44100,
            channels: 1,
        },
        PcmFormat {
            sample_rate: 16000,
            channels: 0,
        },
    ] {
        assert_eq!(
            client
                .stt()
                .stream(
                    futures_util::stream::empty(),
                    TranscriptionOptions::new("en").input_format(format)
                )
                .unwrap_err()
                .kind(),
            ErrorKind::AudioFormat
        );
    }
    assert_eq!(
        client
            .stt()
            .stream(
                futures_util::stream::empty(),
                TranscriptionOptions::new("en").timeout(Some(Duration::ZERO))
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Input
    );
    client.close().await;
    assert_eq!(
        client
            .stt()
            .stream(
                futures_util::stream::empty(),
                TranscriptionOptions::new("en")
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn large_source_chunks_use_bounded_wire_messages() {
    let h = Harness::new(Mode::Echo).await;
    let mut stream = h
        .client
        .stt()
        .stream(
            futures_util::stream::iter([Ok(Bytes::from(vec![0; 200_000]))]),
            TranscriptionOptions::new("en").input_format(PcmFormat {
                sample_rate: 8000,
                channels: 1,
            }),
        )
        .unwrap();
    assert!(matches!(
        collect(&mut stream).await.unwrap().last(),
        Some(TranscriptionUpdate::Final { .. })
    ));
    assert_eq!(h.service.audio.lock().unwrap().len(), 399_998);
    h.client.close().await;
}

#[test]
fn stt_requires_runtime_and_valid_tls_endpoint() {
    let client = Client::builder().api_key("test-key").build().unwrap();
    assert_eq!(
        client
            .stt()
            .stream(
                futures_util::stream::empty(),
                TranscriptionOptions::new("en")
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Input
    );
    for endpoint in ["http://localhost:1234", "host/path", "host:0"] {
        assert_eq!(
            Client::builder()
                .api_key("test-key")
                .stt_endpoint(endpoint)
                .build()
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
    }
}
