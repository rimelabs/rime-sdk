use super::{audio::InputAudio, protocol::TranscriptState, *};
use crate::{Client, Error, ErrorKind};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use rimelabs_api::{
    self as pb,
    speech_to_text_server::{SpeechToText, SpeechToTextServer},
    streaming_transcription_response::Payload,
};
use serde_json::{json, Value};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Notify},
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{
    transport::{Endpoint, Server},
    Request, Response, Status,
};

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
        serde_json::from_str(include_str!("../../testdata/stt/transcripts.json")).unwrap();
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
fn shared_pcm_vectors_at_every_byte_split() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../../testdata/pcm-input.json")).unwrap();
    for vector in fixtures["vectors"].as_array().unwrap() {
        let format = PcmFormat {
            sample_rate: vector["sampleRate"].as_u64().unwrap() as u32,
            channels: vector["channels"].as_u64().unwrap() as u16,
        };
        let bytes = |field: &str| -> Vec<u8> {
            vector[field]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|v| (v.as_i64().unwrap() as i16).to_le_bytes())
                .collect()
        };
        let input = bytes("input");
        for size in 1..=input.len() {
            let mut converter = InputAudio::new(format);
            let output: Vec<_> = input
                .chunks(size)
                .flat_map(|part| converter.feed(part))
                .collect();
            converter.finish().unwrap();
            assert_eq!(output, bytes("output"), "{format:?}, chunk size {size}");
        }
    }
}
#[test]
fn pcm_conversion_bounds_and_truncated_frames() {
    for channels in [1, 2] {
        let mut converter = InputAudio::new(PcmFormat {
            sample_rate: 8000,
            channels,
        });
        for _ in 0..10 {
            let output = converter.feed(&vec![0; super::audio::SOURCE_BYTES]);
            assert!(output.len() <= 65_536);
        }
        converter.finish().unwrap();
        converter.feed(&[0]);
        assert_eq!(
            converter.finish().unwrap_err().kind(),
            ErrorKind::AudioFormat
        );
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

fn language() -> Option<pb::ResolvedLanguage> {
    Some(pb::ResolvedLanguage {
        tag: "en".into(),
        source: pb::LanguageSource::Selected as i32,
    })
}
fn accepted() -> pb::StreamingTranscriptionResponse {
    pb::StreamingTranscriptionResponse {
        payload: Some(Payload::Accepted(pb::StreamingAccepted {
            output_contract: pb::StreamingOutputContract::RevisedHypotheses as i32,
            language: language(),
        })),
    }
}
fn partial(text: String, revision: u64) -> pb::StreamingTranscriptionResponse {
    pb::StreamingTranscriptionResponse {
        payload: Some(Payload::Hypothesis(pb::TranscriptionHypothesis {
            text,
            revision,
        })),
    }
}
fn done(text: &str, revision: u64) -> pb::StreamingTranscriptionResponse {
    pb::StreamingTranscriptionResponse {
        payload: Some(Payload::Done(pb::TranscriptionDone {
            text: text.into(),
            revision,
            language: language(),
        })),
    }
}

type Responses =
    Pin<Box<dyn Stream<Item = Result<pb::StreamingTranscriptionResponse, Status>> + Send>>;
#[derive(Clone, Copy)]
enum Mode {
    Echo,
    NoAcceptance,
    DelayedHeaders,
    EarlyFinal,
    MissingFinal,
    ErrorAfterDone,
    NeverComplete,
    Flood,
    Reject(tonic::Code),
}
#[derive(Clone)]
struct Service {
    mode: Mode,
    calls: Arc<AtomicUsize>,
    configs: Arc<Mutex<Vec<pb::StreamingConfig>>>,
    audio: Arc<Mutex<Vec<u8>>>,
    eof: Arc<AtomicBool>,
    completed: Arc<Notify>,
    cancelled: Arc<Notify>,
}
#[tonic::async_trait]
impl SpeechToText for Service {
    type TranscribeStreamingStream = Responses;
    async fn transcribe(
        &self,
        _: Request<pb::TranscriptionRequest>,
    ) -> Result<Response<pb::TranscriptionResponse>, Status> {
        Err(Status::unimplemented("streaming only"))
    }
    async fn transcribe_streaming(
        &self,
        request: Request<tonic::Streaming<pb::StreamingTranscriptionRequest>>,
    ) -> Result<Response<Responses>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer test-key"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Mode::Reject(code) = self.mode {
            let mut error = Status::new(code, "test rejection");
            error
                .metadata_mut()
                .insert("x-request-id", "rejected-stt".parse().unwrap());
            return Err(error);
        }
        let mut input = request.into_inner();
        let first = input.message().await?.unwrap();
        let Some(pb::streaming_transcription_request::Payload::Config(config)) = first.payload
        else {
            panic!("configuration must be first")
        };
        self.configs.lock().unwrap().push(config);
        if matches!(self.mode, Mode::DelayedHeaders) {
            std::future::pending::<()>().await;
        }
        let (sender, receiver) = mpsc::channel(1);
        let service = self.clone();
        tokio::spawn(async move {
            let work = async {
                if matches!(service.mode, Mode::NoAcceptance) {
                    std::future::pending::<()>().await;
                }
                sender.send(Ok(accepted())).await.ok()?;
                if matches!(service.mode, Mode::EarlyFinal) {
                    sender.send(Ok(done("", 0))).await.ok()?;
                    return Some(());
                }
                if matches!(service.mode, Mode::Flood) {
                    for revision in 1..=1000 {
                        sender.send(Ok(partial("x".into(), revision))).await.ok()?;
                    }
                }
                let mut has_audio = false;
                while let Some(message) = input.message().await.ok()? {
                    let Some(pb::streaming_transcription_request::Payload::Audio(bytes)) =
                        message.payload
                    else {
                        panic!("expected audio")
                    };
                    assert!(bytes.len() <= 65_536);
                    assert_eq!(bytes.len() % 2, 0);
                    service.audio.lock().unwrap().extend(bytes);
                    if !has_audio {
                        sender.send(Ok(partial("I scream".into(), 1))).await.ok()?;
                        has_audio = true;
                    }
                }
                service.eof.store(true, Ordering::SeqCst);
                if matches!(service.mode, Mode::MissingFinal) {
                    return Some(());
                }
                let (text, revision) = if has_audio { ("Ice cream", 2) } else { ("", 0) };
                if has_audio {
                    sender.send(Ok(partial(text.into(), revision))).await.ok()?;
                }
                sender.send(Ok(done(text, revision))).await.ok()?;
                if matches!(service.mode, Mode::ErrorAfterDone) {
                    let mut error = Status::unavailable("error after done");
                    error
                        .metadata_mut()
                        .insert("x-request-id", "failed-stt".parse().unwrap());
                    sender.send(Err(error)).await.ok()?;
                }
                if matches!(service.mode, Mode::NeverComplete) {
                    std::future::pending::<()>().await;
                }
                service.completed.notify_one();
                Some(())
            };
            tokio::select! {
                _ = sender.closed() => {}
                _ = work => {}
            }
            // Cancellation can arrive through either half of the duplex RPC.
            service.cancelled.notify_one();
        });
        let mut response = Response::new(Box::pin(ReceiverStream::new(receiver)) as Responses);
        response
            .metadata_mut()
            .insert("x-request-id", "test-stt".parse().unwrap());
        Ok(response)
    }
}
struct Harness {
    service: Service,
    client: Client,
    stop: CancellationToken,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Harness {
    async fn new(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = Service {
            mode,
            calls: Arc::default(),
            configs: Arc::default(),
            audio: Arc::default(),
            eof: Arc::default(),
            completed: Arc::default(),
            cancelled: Arc::default(),
        };
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let server = service.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(SpeechToTextServer::new(server))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled(),
                )
                .await
                .unwrap();
        });
        let mut client = Client::builder()
            .api_key("test-key")
            .timeout(Duration::from_nanos(1))
            .endpoint("unused.invalid")
            .build()
            .unwrap();
        let inner = Arc::get_mut(&mut client.inner).unwrap();
        inner.stt_endpoint = Endpoint::from_shared(format!("http://{address}")).unwrap();
        inner.stt_limits = Limits {
            acceptance: Duration::from_millis(200),
            completion: Duration::from_millis(200),
        };
        Self {
            service,
            client,
            stop,
        }
    }
    fn stream<S>(&self, source: S) -> TranscriptStream
    where
        S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
    {
        self.client
            .stt()
            .stream(source, TranscriptionOptions::new("en"))
            .unwrap()
    }
}
async fn next(stream: &mut TranscriptStream) -> Option<Result<TranscriptionUpdate, Error>> {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("stream stalled")
}
async fn collect(stream: &mut TranscriptStream) -> Result<Vec<TranscriptionUpdate>, Error> {
    let mut updates = Vec::new();
    while let Some(update) = next(stream).await {
        updates.push(update?);
    }
    Ok(updates)
}

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
use std::task::Poll;
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
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn stuck_source(flag: Arc<AtomicBool>) -> impl Stream<Item = Result<Bytes, Error>> {
    let guard = Dropped(flag);
    futures_util::stream::once(async move {
        let _guard = guard;
        std::future::pending().await
    })
}
#[tokio::test]
async fn cancellation_releases_started_and_unstarted_sources() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut started = h.stream(stuck_source(dropped.clone()));
    assert!(started.next().now_or_never().is_none());
    let lazy_dropped = Arc::new(AtomicBool::new(false));
    let mut lazy = h.stream(stuck_source(lazy_dropped.clone()));
    h.client.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    assert!(lazy_dropped.load(Ordering::SeqCst));
    assert_eq!(
        next(&mut started).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        next(&mut lazy).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}
#[tokio::test]
async fn cancelling_one_stream_preserves_siblings_and_client_reuse() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut cancelled = h.stream(stuck_source(dropped.clone()));
    assert!(cancelled.next().now_or_never().is_none());
    cancelled.cancel();
    assert_eq!(
        next(&mut cancelled).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    cancelled.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    for _ in 0..2 {
        let mut stream = h.stream(futures_util::stream::empty());
        assert_eq!(
            collect(&mut stream).await.unwrap(),
            [TranscriptionUpdate::Final {
                text: "".into(),
                language: "en".into()
            }]
        );
    }
    h.client.close().await;
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
async fn overall_timeout_applies_to_paused_sources() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut stream = h
        .client
        .stt()
        .stream(
            stuck_source(dropped.clone()),
            TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(100))),
        )
        .unwrap();
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
    stream.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    h.client.close().await;
}
#[tokio::test]
async fn queued_final_stays_subject_to_timeout_and_cancellation() {
    for cancel in [false, true] {
        let h = Harness::new(Mode::Echo).await;
        let mut stream = h
            .client
            .stt()
            .stream(
                futures_util::stream::empty(),
                TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(150))),
            )
            .unwrap();
        assert!(stream.next().now_or_never().is_none());
        tokio::time::timeout(Duration::from_secs(2), h.service.completed.notified())
            .await
            .unwrap();
        // Allow trailers to reach the worker without consuming its final.
        tokio::time::sleep(Duration::from_millis(30)).await;
        if cancel {
            stream.cancel();
        } else {
            tokio::time::sleep(Duration::from_millis(160)).await;
        }
        let expected = if cancel {
            ErrorKind::Cancelled
        } else {
            ErrorKind::Timeout
        };
        assert_eq!(
            next(&mut stream).await.unwrap().unwrap_err().kind(),
            expected
        );
        h.client.close().await;
    }
}
#[tokio::test]
async fn completion_timer_stops_after_transport_success() {
    let h = Harness::new(Mode::Echo).await;
    let mut stream = h.stream(futures_util::stream::empty());
    assert!(stream.next().now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(2), h.service.completed.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Final { .. }
    ));
    stream.cancel();
    assert!(next(&mut stream).await.is_none());
    h.client.close().await;
}
#[tokio::test]
async fn slow_consumer_can_cancel_a_full_queue() {
    let h = Harness::new(Mode::Flood).await;
    let mut stream = h.stream(futures_util::stream::pending());
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.cancel();
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    tokio::time::timeout(Duration::from_secs(2), stream.close())
        .await
        .unwrap();
    h.client.close().await;
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

#[tokio::test]
async fn source_panic_becomes_stream_error() {
    let h = Harness::new(Mode::Echo).await;
    let mut stream = h.stream(futures_util::stream::once(async {
        panic!("test audio source panic");
        #[allow(unreachable_code)]
        Ok(Bytes::new())
    }));
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Stream
    );
    stream.close().await;
    assert!(!h.service.eof.load(Ordering::SeqCst));
    h.client.close().await;
}

#[tokio::test]
async fn drop_stream_releases_source_and_rpc() {
    let h = Harness::new(Mode::Echo).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let mut stream = h.stream(
        futures_util::stream::iter([Ok(Bytes::from_static(&[0, 0]))])
            .chain(stuck_source(dropped.clone())),
    );
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), h.service.cancelled.notified())
        .await
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    h.client.close().await;
}

#[tokio::test]
async fn overall_timeout_discards_full_transcript_queue() {
    let h = Harness::new(Mode::Flood).await;
    let mut stream = h
        .client
        .stt()
        .stream(
            futures_util::stream::pending(),
            TranscriptionOptions::new("en").timeout(Some(Duration::from_millis(150))),
        )
        .unwrap();
    assert!(matches!(
        next(&mut stream).await.unwrap().unwrap(),
        TranscriptionUpdate::Partial { .. }
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        next(&mut stream).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
    stream.close().await;
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
