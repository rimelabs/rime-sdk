use super::{queue::TranscriptQueue, transport, TranscriptionOptions, TranscriptionUpdate};
use crate::{client::Inner, Error, ErrorKind};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::FutureExt;
use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

/// A single-reader stream of replacement transcripts.
///
/// The first poll starts network work and the overall timeout. Dropping the stream
/// cancels its source and RPC. Use [`Self::close`] to also wait for worker cleanup.
pub struct TranscriptStream {
    shared: Arc<TranscriptQueue>,
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
        self.shared.request_id()
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
        match this.shared.poll_next(cx) {
            Poll::Ready(result) => {
                this.finished = matches!(&result, Err(_) | Ok(TranscriptionUpdate::Final { .. }));
                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
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
    let shared = Arc::new(TranscriptQueue::default());
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
                let final_result = transport::run(client, source, options, state.clone()).await?;
                state.put(final_result).await?;
                // Keep cancellation and the overall deadline alive until final consumption.
                state.wait_consumed().await;
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
