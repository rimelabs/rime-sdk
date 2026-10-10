use super::{
    audio::{InputAudio, SOURCE_BYTES},
    protocol::TranscriptState,
    TranscriptionMode, TranscriptionOptions, TranscriptionUpdate,
};
use crate::{client::Inner, error::request_id, Error, ErrorKind};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{task::AtomicWaker, FutureExt, StreamExt};
use rimelabs_api::{
    streaming_transcription_request::Payload, StreamingConfig, StreamingOutputContract,
    StreamingTranscriptionRequest,
};
use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch, Notify},
    task::JoinHandle,
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const QUEUED_UPDATES: usize = 16;

#[derive(Default)]
struct Queue {
    items: VecDeque<TranscriptionUpdate>,
    error: Option<Error>,
    final_consumed: bool,
}
#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    reader: AtomicWaker,
    space: Notify,
    consumed: Notify,
    request_id: OnceLock<String>,
}
impl Shared {
    fn fail(&self, error: Error) {
        if let Some(id) = error.request_id() {
            let _ = self.request_id.set(id.to_owned());
        }
        let mut queue = self.queue.lock().expect("transcript queue poisoned");
        if queue.final_consumed || queue.error.is_some() {
            return;
        }
        queue.items.clear();
        queue.error = Some(error.with_request_id(self.request_id.get().map(String::as_str)));
        drop(queue);
        self.reader.wake();
    }
    async fn put(&self, update: TranscriptionUpdate) -> Result<(), Error> {
        loop {
            {
                let mut queue = self.queue.lock().expect("transcript queue poisoned");
                if let Some(error) = &queue.error {
                    return Err(error.clone());
                }
                if queue.items.len() < QUEUED_UPDATES {
                    queue.items.push_back(update);
                    drop(queue);
                    self.reader.wake();
                    return Ok(());
                }
            }
            self.space.notified().await;
        }
    }
}

/// A single-reader stream of replacement transcripts.
///
/// The first poll starts network work and the overall timeout. Dropping the stream
/// cancels its source and RPC. Use [`Self::close`] to also wait for worker cleanup.
pub struct TranscriptStream {
    shared: Arc<Shared>,
    cancellation: CancellationToken,
    start: Option<oneshot::Sender<Instant>>,
    worker: Option<JoinHandle<()>>,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    finished: bool,
}
impl fmt::Debug for TranscriptStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TranscriptStream")
            .field("request_id", &self.request_id())
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}
impl TranscriptStream {
    /// The request identifier, when supplied by the service.
    pub fn request_id(&self) -> Option<&str> {
        self.shared.request_id.get().map(String::as_str)
    }
    /// Cancel only this utterance, including an unread final result.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    /// Cancel and wait for the worker to release the source and RPC.
    pub async fn close(&mut self) {
        self.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        self.finished = true;
    }
}
impl Drop for TranscriptStream {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Stream for TranscriptStream {
    type Item = Result<TranscriptionUpdate, Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        this.shared.reader.register(cx.waker());
        if this.cancellation.is_cancelled() {
            this.shared
                .fail(Error::new(ErrorKind::Cancelled, "transcription cancelled"));
        } else if this
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            this.shared
                .fail(Error::new(ErrorKind::Timeout, "transcription timed out"));
            this.cancellation.cancel();
        }
        if let Some(start) = this.start.take() {
            let now = Instant::now();
            this.deadline = this.timeout.map(|timeout| now + timeout);
            let _ = start.send(now);
        }
        let mut queue = this.shared.queue.lock().expect("transcript queue poisoned");
        if let Some(error) = &queue.error {
            this.finished = true;
            return Poll::Ready(Some(Err(error.clone())));
        }
        if let Some(update) = queue.items.pop_front() {
            if matches!(update, TranscriptionUpdate::Final { .. }) {
                queue.final_consumed = true;
                this.finished = true;
                this.shared.consumed.notify_one();
            }
            this.shared.space.notify_one();
            return Poll::Ready(Some(Ok(update)));
        }
        Poll::Pending
    }
}

pub(super) fn start<S>(
    client: Arc<Inner>,
    source: S,
    options: TranscriptionOptions,
) -> Result<TranscriptStream, Error>
where
    S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
{
    // Validate runtime time support before registering work, as the TTS API does.
    let _timer = tokio::time::sleep(Duration::ZERO);
    let shared = Arc::new(Shared::default());
    let cancellation = client.cancellation.child_token();
    let (start, started) = oneshot::channel();
    let operation = cancellation.clone();
    let state = shared.clone();
    let timeout = options.timeout;
    let owner = client.clone();
    let worker = owner.spawn(async move {
        let work = async {
            let began = tokio::select! {
                biased;
                _ = operation.cancelled() => return Err(Error::new(ErrorKind::Cancelled, "transcription cancelled")),
                began = started => began.map_err(|_| Error::new(ErrorKind::Cancelled, "transcription dropped"))?,
            };
            let deadline = async {
                match timeout {
                    Some(timeout) => tokio::time::sleep_until(began + timeout).await,
                    None => std::future::pending().await,
                }
            };
            let execute = async {
                let final_result = run(client, source, options, state.clone()).await?;
                state.put(final_result).await?;
                // Keep cancellation and the overall deadline alive until final consumption.
                state.consumed.notified().await;
                Ok(())
            };
            tokio::select! {
                biased;
                _ = operation.cancelled() => Err(Error::new(ErrorKind::Cancelled, "transcription cancelled")),
                _ = deadline => Err(Error::new(ErrorKind::Timeout, "transcription timed out")),
                result = execute => result,
            }
        };
        let result = std::panic::AssertUnwindSafe(work).catch_unwind().await
            .unwrap_or_else(|_| Err(Error::new(ErrorKind::Stream, "audio source panicked")));
        if let Err(error) = result { state.fail(error); }
    })?;
    Ok(TranscriptStream {
        shared,
        cancellation,
        start: Some(start),
        worker: Some(worker),
        timeout,
        deadline: None,
        finished: false,
    })
}

async fn run<S>(
    client: Arc<Inner>,
    source: S,
    options: TranscriptionOptions,
    shared: Arc<Shared>,
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
            let _ = shared.request_id.set(id.to_owned());
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
                let _ = shared.request_id.set(id.to_owned());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_bounds_snapshots_and_wakes_blocked_writer() {
        let shared = Shared::default();
        for n in 0..QUEUED_UPDATES {
            shared
                .put(TranscriptionUpdate::Partial {
                    text: n.to_string(),
                })
                .await
                .unwrap();
        }
        let mut blocked = Box::pin(shared.put(TranscriptionUpdate::Partial {
            text: "next".into(),
        }));
        assert!(blocked.as_mut().now_or_never().is_none());
        {
            let mut queue = shared.queue.lock().unwrap();
            assert_eq!(queue.items.len(), QUEUED_UPDATES);
            assert_eq!(
                queue.items.pop_front(),
                Some(TranscriptionUpdate::Partial { text: "0".into() })
            );
        }
        shared.space.notify_one();
        blocked.await.unwrap();
        assert_eq!(shared.queue.lock().unwrap().items.len(), QUEUED_UPDATES);
        shared.fail(Error::new(ErrorKind::Cancelled, "cancelled"));
        let queue = shared.queue.lock().unwrap();
        assert!(queue.items.is_empty());
        assert_eq!(queue.error.as_ref().unwrap().kind(), ErrorKind::Cancelled);
    }
}
