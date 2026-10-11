use super::*;
use crate::{Client, PronunciationEntry, TimestampStatus, WordTimestamp};
use rimelabs_api::{
    self as protocol,
    text_to_speech_server::{TextToSpeech, TextToSpeechServer},
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{net::TcpListener, sync::Notify};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    transport::{Endpoint, Server},
    Request, Response, Status,
};

type Frame = protocol::SynthesisResponseStream;
type Responses = Pin<Box<dyn Stream<Item = Result<Frame, Status>> + Send>>;

fn audio_frame(bytes: Vec<u8>) -> Frame {
    Frame {
        payload: Some(synthesis_response_stream::Payload::Audio(bytes)),
    }
}

fn trailer(value: Option<protocol::Timestamps>) -> Frame {
    Frame {
        payload: Some(synthesis_response_stream::Payload::Trailer(
            protocol::SynthesisResponseTrailer { timestamps: value },
        )),
    }
}

fn words() -> protocol::Timestamps {
    let mut first = protocol::SpanTimestamp {
        text: "twenty".into(),
        ..Default::default()
    };
    first.start.get_or_insert_default().nanos = 90_000_000;
    first.end.get_or_insert_default().nanos = 325_000_000;
    let mut second = protocol::SpanTimestamp {
        text: "two".into(),
        ..Default::default()
    };
    second.start.get_or_insert_default().seconds = 1;
    second.start.as_mut().unwrap().nanos = 125_000_000;
    second.end.get_or_insert_default().seconds = 2;
    protocol::Timestamps {
        status: Some(Default::default()),
        spans: vec![first, second],
    }
}

fn expected_words() -> TimestampResult {
    TimestampResult {
        status: TimestampStatus {
            code: 0,
            message: String::new(),
        },
        spans: vec![
            WordTimestamp {
                text: "twenty".into(),
                start: 0.09,
                end: 0.325,
            },
            WordTimestamp {
                text: "two".into(),
                start: 1.125,
                end: 2.0,
            },
        ],
    }
}

#[derive(Clone)]
struct Script {
    before_gate: Vec<Frame>,
    gated: bool,
    after_gate: Vec<Frame>,
    final_error: Option<Status>,
    // Reject only the first request, before or after response headers.
    rejection: Option<(bool, Status)>,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            before_gate: vec![audio_frame([1, 0].repeat(2400))],
            gated: false,
            after_gate: vec![],
            final_error: None,
            rejection: None,
        }
    }
}

#[derive(Clone)]
struct Service {
    script: Script,
    requests: Arc<Mutex<Vec<protocol::SynthesisRequest>>>,
    texts: Arc<Mutex<Vec<String>>>,
    streaming_calls: Arc<AtomicUsize>,
    released: Arc<Notify>,
}

impl Service {
    async fn response(
        &self,
        header: protocol::SynthesisRequest,
        mut input: tonic::Streaming<protocol::StreamingSynthesisRequest>,
    ) -> Result<Response<Responses>, Status> {
        assert_eq!(
            header.split_strategy,
            Some(protocol::SplitStrategy::AccumulateSentence as i32)
        );
        let first = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(header);
            requests.len() == 1
        };
        let rejection = if first {
            self.script.rejection.clone()
        } else {
            None
        };
        if let Some((false, status)) = &rejection {
            return Err(status.clone());
        }
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            let work = async {
                if let Some((true, status)) = rejection {
                    let _ = sender.send(Err(status)).await;
                    return;
                }
                while let Some(message) = input.message().await.unwrap_or(None) {
                    let Some(Payload::TextChunk(text)) = message.payload else {
                        panic!("expected text chunk")
                    };
                    service.texts.lock().unwrap().push(text);
                }
                for frame in &service.script.before_gate {
                    if sender.send(Ok(frame.clone())).await.is_err() {
                        return;
                    }
                }
                if service.script.gated {
                    service.released.notified().await;
                }
                for frame in &service.script.after_gate {
                    if sender.send(Ok(frame.clone())).await.is_err() {
                        return;
                    }
                }
                if let Some(status) = &service.script.final_error {
                    let _ = sender.send(Err(status.clone())).await;
                }
            };
            tokio::select! {
                _ = sender.closed() => {},
                _ = work => {},
            }
        });
        let mut response = Response::new(Box::pin(ReceiverStream::new(receiver)) as Responses);
        response
            .metadata_mut()
            .insert("x-rime-audio-content-type", "audio/pcm".parse().unwrap());
        response
            .metadata_mut()
            .insert("x-request-id", "feature-request".parse().unwrap());
        Ok(response)
    }
}

fn check_auth<T>(request: &Request<T>) {
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer test-key"
    );
}

#[tonic::async_trait]
impl TextToSpeech for Service {
    type SynthesizeStream = Responses;
    type SynthesizeStreamingStream = Responses;

    async fn synthesize(
        &self,
        _: Request<protocol::SynthesisRequest>,
    ) -> Result<Response<Responses>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn synthesize_streaming(
        &self,
        request: Request<tonic::Streaming<protocol::StreamingSynthesisRequest>>,
    ) -> Result<Response<Responses>, Status> {
        check_auth(&request);
        self.streaming_calls.fetch_add(1, Ordering::SeqCst);
        let mut input = request.into_inner();
        let Some(Payload::Header(header)) = input.message().await?.unwrap().payload else {
            panic!("expected header")
        };
        self.response(header, input).await
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
        Err(Status::unimplemented("not used"))
    }
    async fn get_supported_speakers(
        &self,
        _: Request<protocol::GetSupportedSpeakersRequest>,
    ) -> Result<Response<protocol::GetSupportedSpeakersResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

struct Harness {
    service: Service,
    client: Client,
    shutdown: CancellationToken,
}

impl Harness {
    async fn new(model: Model, script: Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = Service {
            script,
            requests: Default::default(),
            texts: Default::default(),
            streaming_calls: Default::default(),
            released: Default::default(),
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
        let mut client = Client::builder()
            .api_key("test-key")
            .model(model)
            .build()
            .unwrap();
        let inner = Arc::get_mut(&mut client.inner).unwrap();
        inner.endpoint = Endpoint::from_shared(format!("http://{address}")).unwrap();
        Self {
            service,
            client,
            shutdown,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone, Copy, Debug)]
enum InputMode {
    String,
    Incremental,
}
const MODES: [InputMode; 2] = [InputMode::String, InputMode::Incremental];
const FORMATS: [AudioFormat; 2] = [AudioFormat::Pcm24000, AudioFormat::Mulaw8000];

fn start(harness: &Harness, mode: InputMode, options: SynthesisOptions) -> AudioStream {
    match mode {
        InputMode::String => harness
            .client
            .tts()
            .synthesize("First. Twenty two.", options),
        InputMode::Incremental => harness.client.tts().stream(
            futures_util::stream::iter([Ok("First. ".into()), Ok("Twenty two.".into())]),
            options,
        ),
    }
    .unwrap()
}

async fn next(audio: &mut AudioStream) -> Option<Result<Bytes, Error>> {
    tokio::time::timeout(Duration::from_secs(20), audio.next())
        .await
        .expect("audio stream hung")
}

async fn drain(audio: &mut AudioStream) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    while let Some(chunk) = next(audio).await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

async fn finished(audio: &AudioStream) {
    let mut outcome = audio.outcome.clone();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if outcome.borrow_and_update().is_some() {
                break;
            }
            outcome.changed().await.unwrap();
        }
    })
    .await
    .expect("worker hung");
}

#[tokio::test]
async fn lexicons_preserve_fields_order_and_request_isolation_in_all_input_modes() {
    for mode in MODES {
        for format in FORMATS {
            let harness = Harness::new(Model::Coda, Script::default()).await;
            let entries = vec![
                PronunciationEntry::new("Hello", "h @ . \" l oU"),
                PronunciationEntry::new("cafe\u{0301} au lait", " \" k { S "),
                PronunciationEntry::new("Hello", "\" k { S"),
            ];
            let mut options = SynthesisOptions::default()
                .voice("custom")
                .language("es")
                .audio_format(format)
                .custom_lexicon(entries.clone());
            let mut audio = start(&harness, mode, options.clone());
            options.custom_lexicon[0].spelling = "changed".into();
            options.custom_lexicon[1].pronunciation = "changed".into();
            let bytes = drain(&mut audio).await.unwrap();
            assert_eq!(
                bytes,
                Converter::new(format)
                    .process(&[1, 0].repeat(2400), true)
                    .unwrap()
            );
            let request = harness.service.requests.lock().unwrap()[0].clone();
            assert_eq!(request.speaker.as_deref(), Some("custom"));
            assert_eq!(request.language.as_deref(), Some("es"));
            assert_eq!(
                request.audio_parameters.as_ref().unwrap().sampling_rate,
                Some(24000)
            );
            assert!(request.timestamps.is_none());
            assert_eq!(
                request
                    .custom_lexicon
                    .iter()
                    .map(|entry| PronunciationEntry::new(&entry.spelling, &entry.pronunciation))
                    .collect::<Vec<_>>(),
                entries
            );
            assert!(request.text.is_empty());
            assert_eq!(
                harness.service.texts.lock().unwrap().concat(),
                "First. Twenty two."
            );
            assert_eq!(harness.service.streaming_calls.load(Ordering::SeqCst), 1);
            let mut plain = start(&harness, mode, SynthesisOptions::default());
            drain(&mut plain).await.unwrap();
            assert!(harness.service.requests.lock().unwrap()[1]
                .custom_lexicon
                .is_empty());
        }
    }
}

#[tokio::test]
async fn lexicon_linguistic_and_model_validation_belongs_to_the_service() {
    let harness = Harness::new(Model::MistV3, Script::default()).await;
    let entries = vec![PronunciationEntry::new("", "not X-SAMPA 🗣"); 501];
    let mut audio = start(
        &harness,
        InputMode::String,
        SynthesisOptions::default()
            .language("ja")
            .custom_lexicon(entries),
    );
    drain(&mut audio).await.unwrap();
    let requests = harness.service.requests.lock().unwrap();
    assert_eq!(requests[0].custom_lexicon.len(), 501);
    assert_eq!(requests[0].custom_lexicon[0].spelling, "");
    assert_eq!(requests[0].custom_lexicon[0].pronunciation, "not X-SAMPA 🗣");
}

#[tokio::test]
async fn service_rejections_preserve_explanation_id_cause_and_allow_recovery() {
    for mode in MODES {
        for after_headers in [false, true] {
            let message = "custom-lexicon entry \"hello\": no-primary-stress";
            let mut status = Status::invalid_argument(message);
            status
                .metadata_mut()
                .insert("x-request-id", "rejected-request".parse().unwrap());
            let harness = Harness::new(
                Model::Coda,
                Script {
                    rejection: Some((after_headers, status)),
                    ..Default::default()
                },
            )
            .await;
            let options = SynthesisOptions::default()
                .custom_lexicon(vec![PronunciationEntry::new("hello", "h @ . l oU")]);
            // Rejection must arrive even when an incremental source has no text ready.
            let mut audio = if matches!(mode, InputMode::Incremental) {
                harness
                    .client
                    .tts()
                    .stream(futures_util::stream::pending(), options)
                    .unwrap()
            } else {
                start(&harness, mode, options)
            };
            let error = next(&mut audio).await.unwrap().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Input);
            assert_eq!(error.message(), message);
            assert!(error.to_string().contains(message));
            assert_eq!(error.request_id(), Some("rejected-request"));
            assert!(std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<Status>()
                .is_some());
            assert!(next(&mut audio).await.is_none());
            assert_eq!(
                harness.service.requests.lock().unwrap().len(),
                1,
                "must not replay rejected requests"
            );
            let mut recovered = start(&harness, mode, SynthesisOptions::default());
            assert!(!drain(&mut recovered).await.unwrap().is_empty());
        }
    }
}

#[test]
fn empty_service_explanations_keep_the_generic_fallback() {
    for message in ["", "   "] {
        let error = Error::from(Status::invalid_argument(message));
        assert_eq!(error.kind(), ErrorKind::Input);
        assert_eq!(error.message(), "speech service request failed");
    }
}

#[tokio::test]
async fn timestamps_preserve_audio_offsets_and_return_independent_results() {
    for mode in MODES {
        for format in FORMATS {
            let harness = Harness::new(
                Model::MistV3,
                Script {
                    after_gate: vec![trailer(Some(words()))],
                    ..Default::default()
                },
            )
            .await;
            let mut audio = start(
                &harness,
                mode,
                SynthesisOptions::default()
                    .timestamps(true)
                    .audio_format(format),
            );
            let bytes = drain(&mut audio).await.unwrap();
            assert_eq!(
                bytes,
                Converter::new(format)
                    .process(&[1, 0].repeat(2400), true)
                    .unwrap()
            );
            assert!(
                harness.service.requests.lock().unwrap()[0]
                    .timestamps
                    .unwrap()
                    .enable
            );
            let mut result = audio.timestamps().unwrap();
            assert_eq!(result, expected_words());
            result.spans[0].text = "changed".into();
            result.status.message = "changed".into();
            audio.close().await;
            harness.client.close().await;
            assert_eq!(audio.timestamps().unwrap(), expected_words());
        }
    }
}

#[tokio::test]
async fn timestamps_never_consume_audio_or_wait_for_the_trailer() {
    for mode in MODES {
        let harness = Harness::new(
            Model::MistV3,
            Script {
                gated: true,
                after_gate: vec![trailer(Some(words()))],
                ..Default::default()
            },
        )
        .await;
        let mut audio = start(&harness, mode, SynthesisOptions::default().timestamps(true));
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Input);
        assert_eq!(next(&mut audio).await.unwrap().unwrap().len(), 4800);
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Input);
        harness.service.released.notify_one();
        finished(&audio).await;
        // Even a received trailer and successful RPC require consuming the audio EOF.
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Input);
        assert!(next(&mut audio).await.is_none());
        assert_eq!(audio.timestamps().unwrap(), expected_words());
    }
}

#[tokio::test]
async fn alignment_failures_and_empty_success_do_not_fail_audio() {
    for mode in MODES {
        for code in [0, 1, 3, 4, 8, 12, 14] {
            let value = protocol::Timestamps {
                status: Some(protocol::google::rpc::Status {
                    code,
                    message: "alignment explanation".into(),
                    ..Default::default()
                }),
                spans: vec![],
            };
            let harness = Harness::new(
                Model::MistV3,
                Script {
                    after_gate: vec![trailer(Some(value))],
                    ..Default::default()
                },
            )
            .await;
            let mut audio = start(&harness, mode, SynthesisOptions::default().timestamps(true));
            assert_eq!(drain(&mut audio).await.unwrap().len(), 4800);
            let result = audio.timestamps().unwrap();
            assert_eq!(result.status.code, code);
            assert_eq!(result.status.message, "alignment explanation");
            assert!(result.spans.is_empty());
        }
    }
}

#[tokio::test]
async fn malformed_timestamps_fail_only_the_accessor() {
    let mut cases = vec![
        ("missing", vec![]),
        ("empty trailer", vec![trailer(None)]),
        (
            "duplicate",
            vec![trailer(Some(words())), trailer(Some(words()))],
        ),
        (
            "audio after trailer",
            vec![trailer(Some(words())), audio_frame(vec![0, 0])],
        ),
        (
            "empty audio after trailer",
            vec![trailer(Some(words())), audio_frame(vec![])],
        ),
    ];
    for name in [
        "missing status",
        "failed with spans",
        "missing start",
        "missing end",
        "negative seconds",
        "negative nanos",
        "too many seconds",
        "too many nanos",
        "blank word",
        "reversed",
    ] {
        let mut value = words();
        match name {
            "missing status" => value.status = None,
            "failed with spans" => value.status.as_mut().unwrap().code = 14,
            "missing start" => value.spans[0].start = None,
            "missing end" => value.spans[0].end = None,
            "negative seconds" => value.spans[0].start.as_mut().unwrap().seconds = -1,
            "negative nanos" => value.spans[0].start.as_mut().unwrap().nanos = -1,
            "too many seconds" => value.spans[0].end.as_mut().unwrap().seconds = 315_576_000_001,
            "too many nanos" => value.spans[0].end.as_mut().unwrap().nanos = 1_000_000_000,
            "blank word" => value.spans[0].text = " \t\n".into(),
            "reversed" => value.spans[0].start.as_mut().unwrap().seconds = 2,
            _ => unreachable!(),
        }
        cases.push((name, vec![trailer(Some(value))]));
    }
    for mode in MODES {
        for (name, frames) in &cases {
            let harness = Harness::new(
                Model::MistV3,
                Script {
                    after_gate: frames.clone(),
                    ..Default::default()
                },
            )
            .await;
            let mut audio = start(&harness, mode, SynthesisOptions::default().timestamps(true));
            assert!(drain(&mut audio).await.unwrap().len() >= 4800, "{name}");
            let error = audio.timestamps().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Stream, "{name}");
            assert_eq!(error.request_id(), Some("feature-request"), "{name}");
        }
    }
}

#[tokio::test]
async fn timestamp_opt_in_and_model_validation_are_local() {
    let harness = Harness::new(Model::Coda, Script::default()).await;
    let error = harness
        .client
        .tts()
        .synthesize("Hello.", SynthesisOptions::default().timestamps(true))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Input);
    assert!(error.request_id().is_none());
    assert!(harness.service.requests.lock().unwrap().is_empty());
    for model in [Model::Coda, Model::MistV3] {
        let harness = Harness::new(
            model,
            Script {
                after_gate: vec![trailer(None), trailer(None)],
                ..Default::default()
            },
        )
        .await;
        let mut audio = start(&harness, InputMode::String, SynthesisOptions::default());
        drain(&mut audio).await.unwrap();
        assert!(harness.service.requests.lock().unwrap()[0]
            .timestamps
            .is_none());
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Input);
    }
}

#[tokio::test]
async fn rpc_failure_overrides_a_successful_timestamp_trailer_without_replay() {
    for mode in MODES {
        let harness = Harness::new(
            Model::MistV3,
            Script {
                gated: true,
                after_gate: vec![trailer(Some(words()))],
                final_error: Some(Status::unavailable("disconnected after audio")),
                ..Default::default()
            },
        )
        .await;
        let mut audio = start(&harness, mode, SynthesisOptions::default().timestamps(true));
        next(&mut audio).await.unwrap().unwrap();
        harness.service.released.notify_one();
        let error = drain(&mut audio).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(error.message(), "disconnected after audio");
        assert_eq!(error.request_id(), Some("feature-request"));
        assert_eq!(
            audio.timestamps().unwrap_err().kind(),
            ErrorKind::Unavailable
        );
        audio.close().await;
        assert_eq!(
            audio.timestamps().unwrap_err().kind(),
            ErrorKind::Unavailable
        );
        assert_eq!(harness.service.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_before_consumption_cannot_publish_successful_timestamps() {
    for mode in MODES {
        for buffered in [false, true] {
            for cancel in ["stream", "close", "client"] {
                let harness = Harness::new(
                    Model::MistV3,
                    Script {
                        gated: !buffered,
                        after_gate: vec![trailer(Some(words()))],
                        ..Default::default()
                    },
                )
                .await;
                let mut audio = start(&harness, mode, SynthesisOptions::default().timestamps(true));
                if buffered {
                    finished(&audio).await;
                } else {
                    next(&mut audio).await.unwrap().unwrap();
                }
                match cancel {
                    "stream" => audio.cancel(),
                    "close" => audio.close().await,
                    "client" => harness.client.close().await,
                    _ => unreachable!(),
                }
                assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Cancelled);
                if cancel != "close" {
                    assert_eq!(
                        drain(&mut audio).await.unwrap_err().kind(),
                        ErrorKind::Cancelled
                    );
                }
                assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Cancelled);
            }
        }
    }
}

#[tokio::test]
async fn overall_deadline_remains_active_until_audio_is_consumed() {
    for mode in MODES {
        let harness = Harness::new(
            Model::MistV3,
            Script {
                after_gate: vec![trailer(Some(words()))],
                ..Default::default()
            },
        )
        .await;
        let mut audio = start(
            &harness,
            mode,
            SynthesisOptions::default()
                .timestamps(true)
                .timeout(Some(Duration::from_secs(2))),
        );
        finished(&audio).await;
        assert!(audio.outcome.borrow().as_ref().unwrap().is_ok());
        // The RPC and its timer are gone, but unread audio is still subject to
        // the overall deadline. Move time only after all network work settles.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Timeout);
        assert_eq!(
            drain(&mut audio).await.unwrap_err().kind(),
            ErrorKind::Timeout
        );
        audio.close().await;
        assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Timeout);
        tokio::time::resume();
    }
}

#[tokio::test]
async fn source_failure_is_returned_by_both_audio_and_timestamps() {
    let harness = Harness::new(
        Model::MistV3,
        Script {
            after_gate: vec![trailer(Some(words()))],
            ..Default::default()
        },
    )
    .await;
    let mut audio = harness
        .client
        .tts()
        .stream(
            futures_util::stream::iter([
                Ok("Hello. ".to_owned()),
                Err(Error::input(std::io::Error::other("source failed"))),
            ]),
            SynthesisOptions::default().timestamps(true),
        )
        .unwrap();
    assert_eq!(
        drain(&mut audio).await.unwrap_err().kind(),
        ErrorKind::Input
    );
    assert_eq!(audio.timestamps().unwrap_err().kind(), ErrorKind::Input);
    assert_eq!(
        audio.timestamps().unwrap_err().message(),
        "input source failed"
    );
}
