// Owns the duplex RPC: acceptance, bounded audio upload, response validation,
// and completion. The stream owner supplies cancellation and the overall deadline.
use super::{
    audio::{InputAudio, SOURCE_BYTES},
    protocol::TranscriptState,
    queue::TranscriptQueue,
    TranscriptionMode, TranscriptionOptions, TranscriptionUpdate,
};
use crate::{client::Inner, error::request_id, Error, ErrorKind};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use rimelabs_api::{
    streaming_transcription_request::Payload, StreamingConfig, StreamingOutputContract,
    StreamingTranscriptionRequest,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;

pub(super) async fn run<S>(
    client: Arc<Inner>,
    source: S,
    options: TranscriptionOptions,
    shared: Arc<TranscriptQueue>,
) -> Result<TranscriptionUpdate, Error>
where
    S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
{
    let mut stub = client.stt_stub().await?;
    let (requests, incoming) = mpsc::channel(1);
    let (accepted, acceptance) = oneshot::channel();
    let (completion, mut completion_started) = watch::channel(None);
    let input_done = AtomicBool::new(false);
    let acceptance_deadline = Instant::now() + client.stt_limits.acceptance;
    let producer = async {
        let config = StreamingConfig {
            language: Some(options.language),
            mode: match options.mode {
                TranscriptionMode::Written => rimelabs_api::TranscriptionMode::Written as i32,
                TranscriptionMode::Verbatim => rimelabs_api::TranscriptionMode::Verbatim as i32,
            },
            context_terms: options.context_terms,
            output_contract: StreamingOutputContract::RevisedHypotheses as i32,
        };
        send(&requests, Payload::Config(config)).await?;
        // If the reader fails, try_join returns that error and drops this wait.
        if acceptance.await.is_err() {
            return std::future::pending().await;
        }
        let mut source = Box::pin(source);
        let mut audio = InputAudio::new(options.input_format);
        while let Some(chunk) = source.next().await {
            let chunk = chunk?;
            for portion in chunk.chunks(SOURCE_BYTES) {
                let converted = audio.feed(portion);
                if !converted.is_empty() {
                    send(&requests, Payload::Audio(converted)).await?;
                }
                // An always-ready source must not starve the reader or cancellation.
                tokio::task::yield_now().await;
            }
            tokio::task::yield_now().await;
        }
        audio.finish()?;
        input_done.store(true, Ordering::Release);
        completion.send_replace(Some(Instant::now() + client.stt_limits.completion));
        drop(requests); // Half-close only after valid source exhaustion.
        Ok(())
    };
    let receiver = async {
        let response = tokio::time::timeout_at(
            acceptance_deadline,
            stub.transcribe_streaming(client.request(ReceiverStream::new(incoming))),
        )
        .await
        .map_err(|_| Error::new(ErrorKind::Timeout, "transcription acceptance timed out"))??;
        if let Some(id) = request_id(response.metadata()) {
            shared.set_request_id(id);
        }
        let mut responses = response.into_inner();
        let mut state = TranscriptState::default();
        let mut accepted = Some(accepted);
        loop {
            let message = if state.language.is_none() {
                tokio::time::timeout_at(acceptance_deadline, responses.message())
                    .await
                    .map_err(|_| {
                        Error::new(ErrorKind::Timeout, "transcription acceptance timed out")
                    })??
            } else {
                responses.message().await?
            };
            let Some(message) = message else {
                break;
            };
            if let Some(update) = state.accept(message, input_done.load(Ordering::Acquire))? {
                shared.put(update).await?;
            }
            if state.language.is_some() {
                if let Some(accepted) = accepted.take() {
                    let _ = accepted.send(());
                }
            }
        }
        if let Some(trailers) = responses.trailers().await? {
            if let Some(id) = request_id(&trailers) {
                shared.set_request_id(id);
            }
        }
        state.finish()
    };
    let completion_timeout = async {
        loop {
            let deadline = *completion_started.borrow_and_update();
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline).await;
                return;
            }
            if completion_started.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        biased;
        _ = completion_timeout => Err(Error::new(ErrorKind::Timeout, "transcription completion timed out")),
        result = async { tokio::try_join!(producer, receiver) } => result.map(|(_, final_result)| final_result),
    }
}

async fn send(
    sender: &mpsc::Sender<StreamingTranscriptionRequest>,
    payload: Payload,
) -> Result<(), Error> {
    if sender
        .send(StreamingTranscriptionRequest {
            payload: Some(payload),
        })
        .await
        .is_err()
    {
        // Let the response reader report the service status, not an internal channel error.
        std::future::pending::<()>().await;
    }
    Ok(())
}
