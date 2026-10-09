// Local gRPC conformance tests are kept inside the crate so production callers
// cannot disable TLS through a test transport option.
use crate::*;
use futures_core::Stream;
use futures_util::StreamExt;
use rime_api::{
    self as protocol,
    text_to_speech_server::{TextToSpeech, TextToSpeechServer},
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
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

type Responses =
    Pin<Box<dyn Stream<Item = Result<protocol::SynthesisResponseStream, Status>> + Send>>;

#[derive(Clone, Copy)]
enum Mode {
    Echo,
    Hold,
    PartialError,
    EarlyEnd,
    BadFormat,
    OddFrame,
    DelayedHeaders,
    Reject(tonic::Code),
    Flood,
}

#[derive(Clone)]
struct Service {
    mode: Mode,
    calls: Arc<AtomicUsize>,
    discoveries: Arc<AtomicUsize>,
    texts: Arc<Mutex<Vec<String>>>,
    released: Arc<Notify>,
    cancelled: Arc<Notify>,
}

fn audio(bytes: Vec<u8>) -> Result<protocol::SynthesisResponseStream, Status> {
    Ok(protocol::SynthesisResponseStream {
        payload: Some(protocol::synthesis_response_stream::Payload::Audio(bytes)),
    })
}

#[tonic::async_trait]
impl TextToSpeech for Service {
    type SynthesizeStream = Responses;
    type SynthesizeStreamingStream = Responses;
    async fn synthesize(
        &self,
        _: Request<protocol::SynthesisRequest>,
    ) -> Result<Response<Responses>, Status> {
        Err(Status::unimplemented("streaming only"))
    }

    async fn synthesize_streaming(
        &self,
        request: Request<tonic::Streaming<protocol::StreamingSynthesisRequest>>,
    ) -> Result<Response<Responses>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer test-key"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Mode::Reject(code) = self.mode {
            let mut error = Status::new(code, "test error");
            error
                .metadata_mut()
                .insert("x-request-id", "rejected-request".parse().unwrap());
            return Err(error);
        }
        let mut input = request.into_inner();
        let (sender, receiver) = mpsc::channel(2);
        if matches!(self.mode, Mode::DelayedHeaders) {
            let first = input.message().await?.unwrap();
            assert!(matches!(
                first.payload,
                Some(protocol::streaming_synthesis_request::Payload::Header(_))
            ));
            let text = input.message().await?.unwrap();
            if let Some(protocol::streaming_synthesis_request::Payload::TextChunk(text)) =
                text.payload
            {
                self.texts.lock().unwrap().push(text);
                sender.send(audio(vec![1, 0])).await.unwrap();
            }
        }
        let service = self.clone();
        tokio::spawn(async move {
            let work = async {
                if matches!(service.mode, Mode::EarlyEnd) {
                    return;
                }
                while let Some(message) = input.message().await.unwrap_or(None) {
                    match message.payload {
                        Some(protocol::streaming_synthesis_request::Payload::Header(header)) => {
                            assert_eq!(
                                header
                                    .audio_parameters
                                    .as_ref()
                                    .unwrap()
                                    .audio_format
                                    .as_deref(),
                                Some("audio/pcm")
                            );
                            assert_eq!(
                                header.audio_parameters.as_ref().unwrap().sampling_rate,
                                Some(24_000)
                            );
                        }
                        Some(protocol::streaming_synthesis_request::Payload::TextChunk(text)) => {
                            service.texts.lock().unwrap().push(text);
                            let bytes = if matches!(service.mode, Mode::OddFrame) {
                                vec![1]
                            } else {
                                vec![1, 0, 2, 0]
                            };
                            if sender.send(audio(bytes)).await.is_err() {
                                return;
                            }
                            match service.mode {
                                Mode::Hold => std::future::pending::<()>().await,
                                Mode::PartialError => {
                                    service.released.notified().await;
                                    let mut error = Status::unavailable("test failure after audio");
                                    error
                                        .metadata_mut()
                                        .insert("x-request-id", "trailer-request".parse().unwrap());
                                    let _ = sender.send(Err(error)).await;
                                    return;
                                }
                                Mode::Flood => {
                                    for _ in 0..1000 {
                                        if sender.send(audio(vec![0; 9600])).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        None => {}
                    }
                }
            };
            tokio::select! {
                _ = sender.closed() => { service.cancelled.notify_one(); }
                _ = work => {}
            }
        });
        let mut response = Response::new(Box::pin(ReceiverStream::new(receiver)) as Responses);
        if !matches!(self.mode, Mode::BadFormat) {
            response
                .metadata_mut()
                .insert("x-rime-audio-content-type", "audio/pcm".parse().unwrap());
        }
        response
            .metadata_mut()
            .insert("x-request-id", "test-request".parse().unwrap());
        Ok(response)
    }

    async fn normalize_text(
        &self,
        _: Request<protocol::NormalizeTextRequest>,
    ) -> Result<Response<protocol::NormalizeTextResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn get_supported_languages(
        &self,
        _: Request<protocol::GetSupportedLanguagesRequest>,
    ) -> Result<Response<protocol::GetSupportedLanguagesResponse>, Status> {
        if self.discoveries.fetch_add(1, Ordering::SeqCst) < 2 {
            return Err(Status::unavailable("retry discovery"));
        }
        Ok(Response::new(protocol::GetSupportedLanguagesResponse {
            languages: vec!["en".into()],
        }))
    }
    async fn get_supported_speakers(
        &self,
        request: Request<protocol::GetSupportedSpeakersRequest>,
    ) -> Result<Response<protocol::GetSupportedSpeakersResponse>, Status> {
        assert_eq!(request.into_inner().language.as_deref(), Some("en"));
        Ok(Response::new(protocol::GetSupportedSpeakersResponse {
            speakers: vec!["clementine".into()],
        }))
    }
}

struct Harness {
    service: Service,
    client: Client,
    shutdown: CancellationToken,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Harness {
    async fn new(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = Service {
            mode,
            calls: Arc::new(AtomicUsize::new(0)),
            discoveries: Arc::new(AtomicUsize::new(0)),
            texts: Arc::new(Mutex::new(Vec::new())),
            released: Arc::new(Notify::new()),
            cancelled: Arc::new(Notify::new()),
        };
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let server = service.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(TextToSpeechServer::new(server))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), stop.cancelled())
                .await
                .unwrap();
        });
        let mut client = Client::builder().api_key("test-key").build().unwrap();
        let inner = Arc::get_mut(&mut client.inner).unwrap();
        inner.endpoint = Endpoint::from_shared(format!("http://{address}")).unwrap();
        inner.first_audio_timeout = Duration::from_millis(200);
        inner.progress_timeout = Duration::from_millis(200);
        Self {
            service,
            client,
            shutdown,
        }
    }
}

async fn next(audio: &mut AudioStream) -> Option<Result<bytes::Bytes, Error>> {
    tokio::time::timeout(Duration::from_secs(20), audio.next())
        .await
        .expect("stream hung")
}

#[tokio::test]
async fn complete_text_and_delayed_headers() {
    for mode in [Mode::Echo, Mode::DelayedHeaders] {
        let harness = Harness::new(mode).await;
        let mut audio = harness
            .client
            .synthesize(
                "Hello. This last sentence has enough trailing context.",
                SynthesisOptions::default(),
            )
            .unwrap();
        assert_eq!(audio.format(), AudioFormat::Pcm24000);
        let mut chunks = 0;
        while let Some(chunk) = next(&mut audio).await {
            assert!(!chunk.unwrap().is_empty());
            chunks += 1;
        }
        assert!(chunks >= 1);
        assert_eq!(audio.request_id(), Some("test-request"));
        assert_eq!(
            harness.service.texts.lock().unwrap().concat(),
            "Hello. This last sentence has enough trailing context."
        );
        assert_eq!(harness.service.calls.load(Ordering::SeqCst), 1);
        harness.client.close().await;
    }
}

#[tokio::test]
async fn incremental_audio_arrives_before_input_end_and_pauses_do_not_timeout() {
    let harness = Harness::new(Mode::Echo).await;
    let (sender, receiver) = mpsc::channel(1);
    let mut audio = harness
        .client
        .synthesize_stream(ReceiverStream::new(receiver), SynthesisOptions::default())
        .unwrap();
    sender
        .send(Ok(
            "Hello. The next sentence has enough trailing context.".into()
        ))
        .await
        .unwrap();
    assert!(next(&mut audio).await.unwrap().is_ok());
    tokio::time::sleep(Duration::from_millis(350)).await;
    sender.send(Ok(" More words.".into())).await.unwrap();
    drop(sender);
    while let Some(chunk) = next(&mut audio).await {
        chunk.unwrap();
    }
}

#[tokio::test]
async fn partial_audio_then_error_preserves_status_and_never_replays() {
    let harness = Harness::new(Mode::PartialError).await;
    let mut audio = harness
        .client
        .synthesize("Hello.", SynthesisOptions::default())
        .unwrap();
    next(&mut audio).await.unwrap().unwrap();
    harness.service.released.notify_one();
    let error = next(&mut audio).await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.request_id(), Some("trailer-request"));
    assert!(next(&mut audio).await.is_none());
    assert_eq!(harness.service.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_one_stream_keeps_sibling() {
    let harness = Harness::new(Mode::Echo).await;
    let mut first = harness
        .client
        .synthesize_stream(
            futures_util::stream::pending::<Result<String, Error>>(),
            SynthesisOptions::default(),
        )
        .unwrap();
    first.cancel();
    assert_eq!(
        next(&mut first).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    let mut sibling = harness
        .client
        .synthesize("Hello.", SynthesisOptions::default())
        .unwrap();
    while let Some(chunk) = next(&mut sibling).await {
        chunk.unwrap();
    }
    first.close().await;
}

#[tokio::test]
async fn overall_timeout_applies_while_input_is_paused() {
    let harness = Harness::new(Mode::Echo).await;
    let mut audio = harness
        .client
        .synthesize_stream(
            futures_util::stream::pending::<Result<String, Error>>(),
            SynthesisOptions::default().timeout(Some(Duration::from_millis(100))),
        )
        .unwrap();
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
}

#[tokio::test]
async fn overall_timeout_discards_buffered_audio_when_consumer_is_paused() {
    let harness = Harness::new(Mode::Flood).await;
    let mut audio = harness
        .client
        .synthesize(
            "Hello.",
            SynthesisOptions::default().timeout(Some(Duration::from_secs(2))),
        )
        .unwrap();
    next(&mut audio).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
}

#[tokio::test]
async fn stalled_output_times_out() {
    let harness = Harness::new(Mode::Hold).await;
    let mut audio = harness
        .client
        .synthesize("Hello.", SynthesisOptions::default())
        .unwrap();
    next(&mut audio).await.unwrap().unwrap();
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Timeout
    );
}

#[tokio::test]
async fn early_completion_is_not_success() {
    let harness = Harness::new(Mode::EarlyEnd).await;
    let mut audio = harness
        .client
        .synthesize_stream(
            futures_util::stream::pending::<Result<String, Error>>(),
            SynthesisOptions::default(),
        )
        .unwrap();
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Stream
    );
}

#[tokio::test]
async fn bad_audio_is_rejected() {
    for mode in [Mode::BadFormat, Mode::OddFrame] {
        let harness = Harness::new(mode).await;
        let mut audio = harness
            .client
            .synthesize("Hello.", SynthesisOptions::default())
            .unwrap();
        assert_eq!(
            next(&mut audio).await.unwrap().unwrap_err().kind(),
            ErrorKind::AudioFormat
        );
    }
}

#[tokio::test]
async fn source_error_and_blank_input_stop_the_stream() {
    let harness = Harness::new(Mode::Echo).await;
    for input in [
        Ok("   ".into()),
        Err(Error::input(std::io::Error::other("source failed"))),
    ] {
        let mut audio = harness
            .client
            .synthesize_stream(
                futures_util::stream::iter([input]),
                SynthesisOptions::default(),
            )
            .unwrap();
        assert_eq!(
            next(&mut audio).await.unwrap().unwrap_err().kind(),
            ErrorKind::Input
        );
    }
}

#[tokio::test]
async fn grpc_errors_match_shared_contract() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../testdata/contract.json")).unwrap();
    for (name, code, kind) in [
        (
            "UNAUTHENTICATED",
            tonic::Code::Unauthenticated,
            ErrorKind::Authentication,
        ),
        (
            "PERMISSION_DENIED",
            tonic::Code::PermissionDenied,
            ErrorKind::Permission,
        ),
        (
            "INVALID_ARGUMENT",
            tonic::Code::InvalidArgument,
            ErrorKind::Input,
        ),
        (
            "RESOURCE_EXHAUSTED",
            tonic::Code::ResourceExhausted,
            ErrorKind::ResourceLimit,
        ),
        (
            "UNAVAILABLE",
            tonic::Code::Unavailable,
            ErrorKind::Unavailable,
        ),
        (
            "DEADLINE_EXCEEDED",
            tonic::Code::DeadlineExceeded,
            ErrorKind::Timeout,
        ),
        ("CANCELLED", tonic::Code::Cancelled, ErrorKind::Cancelled),
        ("INTERNAL", tonic::Code::Internal, ErrorKind::Stream),
    ] {
        assert!(fixture["grpc_errors"][name].is_string());
        let harness = Harness::new(Mode::Reject(code)).await;
        let mut audio = harness
            .client
            .synthesize("Hello.", SynthesisOptions::default())
            .unwrap();
        let error = next(&mut audio).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.request_id(), Some("rejected-request"));
        assert_eq!(harness.service.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn discovery_retries_and_client_close_cancels_all_clones() {
    let harness = Harness::new(Mode::Echo).await;
    assert_eq!(harness.client.languages().await.unwrap(), ["en"]);
    assert_eq!(harness.service.discoveries.load(Ordering::SeqCst), 3);
    assert_eq!(
        harness.client.voices(Some("en")).await.unwrap(),
        ["clementine"]
    );
    let clone = harness.client.clone();
    let mut audio = clone
        .synthesize_stream(
            futures_util::stream::pending::<Result<String, Error>>(),
            SynthesisOptions::default(),
        )
        .unwrap();
    harness.client.close().await;
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        clone.languages().await.unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn drop_audio_releases_rpc() {
    let harness = Harness::new(Mode::Hold).await;
    let mut audio = harness
        .client
        .synthesize("Hello.", SynthesisOptions::default())
        .unwrap();
    next(&mut audio).await.unwrap().unwrap();
    drop(audio);
    tokio::time::timeout(Duration::from_secs(5), harness.service.cancelled.notified())
        .await
        .unwrap();
    harness.client.close().await;
}

#[tokio::test]
async fn source_panic_becomes_a_terminal_error() {
    let harness = Harness::new(Mode::Echo).await;
    let source =
        futures_util::stream::poll_fn(|_| -> std::task::Poll<Option<Result<String, Error>>> {
            panic!("broken source")
        });
    let mut audio = harness
        .client
        .synthesize_stream(source, SynthesisOptions::default())
        .unwrap();
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Stream
    );
    audio.close().await;
}

#[tokio::test]
async fn dropping_last_client_cancels_streams() {
    let client = Client::builder().api_key("test-key").build().unwrap();
    let mut audio = client
        .synthesize_stream(
            futures_util::stream::pending::<Result<String, Error>>(),
            SynthesisOptions::default(),
        )
        .unwrap();
    drop(client);
    assert_eq!(
        next(&mut audio).await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    audio.close().await;
}

#[test]
fn configuration_is_typed_validated_and_credentials_are_redacted() {
    for endpoint in [
        "https://example.com",
        "example.com/path",
        "example.com:0",
        "example.com:65536",
        "bad host",
        "-bad.com",
        "example.com:",
    ] {
        assert_eq!(
            Client::builder()
                .api_key("secret")
                .endpoint(endpoint)
                .build()
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
    }
    assert!(Client::builder()
        .api_key("secret")
        .timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(Client::builder().api_key("contains space").build().is_err());
    assert!(!format!("{:?}", Client::builder().api_key("secret")).contains("secret"));
    let client = Client::builder()
        .api_key("secret")
        .model(Model::MistV3)
        .build()
        .unwrap();
    assert_eq!(client.inner.model.voice(), "astra");
    assert!(!format!("{client:?}").contains("secret"));
    assert!(client
        .synthesize("Hello.", SynthesisOptions::default())
        .is_err());
}
