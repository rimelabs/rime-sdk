// Shared local gRPC peer and helpers for the STT transport and lifecycle tests.
mod audio;
mod lifecycle;
mod protocol;
mod transport;
mod unary;

use super::{Limits, TranscriptStream, TranscriptionOptions, TranscriptionUpdate};
use crate::{Client, Error};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use rimelabs_api::{
    self as pb,
    speech_to_text_server::{SpeechToText, SpeechToTextServer},
    streaming_transcription_response::Payload,
};
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
fn delta(text: String) -> pb::StreamingTranscriptionResponse {
    pb::StreamingTranscriptionResponse {
        payload: Some(Payload::Delta(pb::TranscriptionDelta { text })),
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
    recordings: Arc<Mutex<Vec<pb::TranscriptionRequest>>>,
    eof: Arc<AtomicBool>,
    completed: Arc<Notify>,
    cancelled: Arc<Notify>,
}
#[tonic::async_trait]
impl SpeechToText for Service {
    type TranscribeStreamingStream = Responses;
    async fn transcribe(
        &self,
        request: Request<pb::TranscriptionRequest>,
    ) -> Result<Response<pb::TranscriptionResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer test-key"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.recordings.lock().unwrap().push(request.into_inner());
        self.completed.notify_one();
        if let Mode::Reject(code) = self.mode {
            let mut error = Status::new(code, "test rejection");
            error
                .metadata_mut()
                .insert("x-request-id", "rejected-stt".parse().unwrap());
            return Err(error);
        }
        if matches!(self.mode, Mode::NeverComplete) {
            struct Cancelled(Arc<Notify>);
            impl Drop for Cancelled {
                fn drop(&mut self) {
                    self.0.notify_one();
                }
            }
            let _guard = Cancelled(self.cancelled.clone());
            std::future::pending::<()>().await;
        }
        let mut response = Response::new(pb::TranscriptionResponse {
            text: if matches!(self.mode, Mode::EarlyFinal) {
                ""
            } else {
                "Ice cream"
            }
            .into(),
            language: if matches!(self.mode, Mode::MissingFinal) {
                None
            } else {
                language()
            },
        });
        response
            .metadata_mut()
            .insert("x-request-id", "test-stt".parse().unwrap());
        Ok(response)
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
        let append = config.output_contract == pb::StreamingOutputContract::AppendFragments as i32;
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
                let mut acceptance = accepted();
                if let Some(Payload::Accepted(value)) = &mut acceptance.payload {
                    if append {
                        value.output_contract = pb::StreamingOutputContract::AppendFragments as i32;
                    }
                }
                sender.send(Ok(acceptance)).await.ok()?;
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
                        sender
                            .send(Ok(if append {
                                delta("Ice".into())
                            } else {
                                partial("I scream".into(), 1)
                            }))
                            .await
                            .ok()?;
                        has_audio = true;
                    }
                }
                service.eof.store(true, Ordering::SeqCst);
                if matches!(service.mode, Mode::MissingFinal) {
                    return Some(());
                }
                let (text, revision) = if has_audio {
                    if append {
                        ("Ice\n cream", 0)
                    } else {
                        ("Ice cream", 2)
                    }
                } else {
                    ("", 0)
                };
                if has_audio {
                    sender
                        .send(Ok(if append {
                            delta("\n cream".into())
                        } else {
                            partial(text.into(), revision)
                        }))
                        .await
                        .ok()?;
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
            recordings: Arc::default(),
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
