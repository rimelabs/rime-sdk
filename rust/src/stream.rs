use crate::{
    audio::Converter,
    client::{validate_timeout, Inner},
    error::request_id,
    sentences::Buffer,
    AudioFormat, Error, ErrorKind, SynthesisOptions,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use rime_api::{
    streaming_synthesis_request::Payload, synthesis_response_stream, AudioParameters,
    StreamingSynthesisRequest, SynthesisRequest,
};
use std::{
    fmt,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::Instant,
};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const QUEUE_CHUNKS: usize = 10;
const CHUNK_BYTES: usize = 9_600;

/// An asynchronous stream of raw audio chunks. An error ends the stream.
///
/// Dropping this value requests cancellation. Use [`Self::close`] to also wait
/// for the stream worker. Cancellation does not affect sibling streams.
pub struct AudioStream {
    receiver: mpsc::Receiver<Bytes>,
    status: WatchStream<Option<Result<(), Error>>>,
    terminal: Option<Result<(), Error>>,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
    request_id: Arc<OnceLock<String>>,
    format: AudioFormat,
    consumed: bool,
}

impl fmt::Debug for AudioStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AudioStream")
            .field("format", &self.format)
            .field("request_id", &self.request_id())
            .field("consumed", &self.consumed)
            .finish_non_exhaustive()
    }
}

impl AudioStream {
    /// The encoding and sample rate of returned chunks.
    pub fn format(&self) -> AudioFormat {
        self.format
    }
    /// The service request ID, once response metadata has arrived.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.get().map(String::as_str)
    }
    /// Request cancellation without waiting for cleanup.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    /// Request cancellation and wait for the worker to release its RPC and source.
    pub async fn close(&mut self) {
        self.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.consumed = true;
    }
}

impl Drop for AudioStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl Stream for AudioStream {
    type Item = Result<Bytes, Error>;
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.consumed {
            return Poll::Ready(None);
        }
        while let Poll::Ready(Some(status)) = Pin::new(&mut this.status).poll_next(context) {
            if status.is_some() {
                this.terminal = status;
            }
        }
        if let Some(Err(error)) = &this.terminal {
            // Match the other SDKs: failure takes priority over queued audio.
            // A paused consumer must observe cancellation or timeout on its next read.
            let error = error.clone();
            this.consumed = true;
            this.receiver.close();
            while this.receiver.try_recv().is_ok() {}
            return Poll::Ready(Some(Err(error)));
        }
        match this.receiver.poll_recv(context) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(None) if this.terminal.is_some() => {
                this.consumed = true;
                Poll::Ready(None)
            }
            // The producer can close the queue before publishing its terminal status.
            _ => Poll::Pending,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InputState {
    Processing,
    Waiting,
    Finished,
}

struct Progress {
    input: InputState,
    sent_text: bool,
    received_audio: bool,
    output_finished: bool,
    writing_audio: bool,
    last_progress: Instant,
}

impl Progress {
    fn new() -> Self {
        Self {
            input: InputState::Processing,
            sent_text: false,
            received_audio: false,
            output_finished: false,
            writing_audio: false,
            last_progress: Instant::now(),
        }
    }
}

pub(crate) fn start<S>(
    client: Arc<Inner>,
    source: S,
    options: SynthesisOptions,
) -> Result<AudioStream, Error>
where
    S: Stream<Item = Result<String, Error>> + Send + 'static,
{
    let timeout = options.timeout.unwrap_or(client.timeout);
    validate_timeout(timeout)?;
    let voice = options.voice.unwrap_or_else(|| client.model.voice().into());
    let language = options.language.unwrap_or_else(|| "en".into());
    if voice.trim().is_empty() || language.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::Input,
            "voice and language must not be blank",
        ));
    }
    let (sender, receiver) = mpsc::channel(QUEUE_CHUNKS);
    let (status_sender, status_receiver) = watch::channel(None);
    let cancellation = client.cancellation.child_token();
    let operation_cancellation = cancellation.clone();
    let request_id = Arc::new(OnceLock::new());
    let operation_id = request_id.clone();
    let format = options.format;
    let progress = Arc::new(Mutex::new(Progress::new()));
    let tracker = client.tasks.clone();
    let deadline = timeout.map(|timeout| Instant::now() + timeout);
    let worker = tracker.spawn(async move {
        let blocking_tasks = TaskTracker::new();
        let run = std::panic::AssertUnwindSafe(run(client.clone(), source, sender.clone(), progress.clone(), operation_id.clone(), format, voice, language, blocking_tasks.clone())).catch_unwind();
        let monitor = monitor(client, progress, sender);
        let timeout = async {
            match deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await }
        };
        let result = tokio::select! {
            biased;
            _ = operation_cancellation.cancelled() => Err(Error::new(ErrorKind::Cancelled, "synthesis cancelled")),
            _ = timeout => Err(Error::new(ErrorKind::Timeout, "synthesis timed out")),
            result = run => result.unwrap_or_else(|_| Err(Error::new(ErrorKind::Stream, "text source panicked"))),
            error = monitor => Err(error),
        };
        let result = result.map_err(|error| {
            if let Some(id) = error.request_id() { let _ = operation_id.set(id.to_owned()); }
            error.with_request_id(operation_id.get().map(String::as_str))
        });
        status_sender.send_replace(Some(result));
        blocking_tasks.close();
        blocking_tasks.wait().await;
    });
    Ok(AudioStream {
        receiver,
        status: WatchStream::new(status_receiver),
        terminal: None,
        cancellation,
        worker: Some(worker),
        request_id,
        format,
        consumed: false,
    })
}

async fn monitor(
    client: Arc<Inner>,
    progress: Arc<Mutex<Progress>>,
    sender: mpsc::Sender<Bytes>,
) -> Error {
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    loop {
        interval.tick().await;
        let mut state = progress.lock().expect("progress mutex poisoned");
        let active = state.sent_text
            && !state.output_finished
            && state.input != InputState::Waiting
            && !state.writing_audio
            && sender.capacity() == QUEUE_CHUNKS;
        if !active {
            state.last_progress = Instant::now();
            continue;
        }
        let limit = if state.received_audio {
            client.progress_timeout
        } else {
            client.first_audio_timeout
        };
        if state.last_progress.elapsed() >= limit {
            return Error::new(
                ErrorKind::Timeout,
                "synthesis output stopped making progress",
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run<S>(
    client: Arc<Inner>,
    source: S,
    audio: mpsc::Sender<Bytes>,
    progress: Arc<Mutex<Progress>>,
    id: Arc<OnceLock<String>>,
    format: AudioFormat,
    voice: String,
    language: String,
    blocking_tasks: TaskTracker,
) -> Result<(), Error>
where
    S: Stream<Item = Result<String, Error>> + Send + 'static,
{
    let mut stub = client.stub().await?;
    let (requests, incoming) = mpsc::channel(2);
    let producer = produce(
        source,
        requests,
        progress.clone(),
        voice,
        language,
        blocking_tasks,
    );
    let receiver = async {
        let response = stub
            .synthesize_streaming(client.request(ReceiverStream::new(incoming)))
            .await
            .map_err(Error::from)?;
        if let Some(value) = request_id(response.metadata()) {
            let _ = id.set(value.into());
        }
        let valid_format = response
            .metadata()
            .get("x-rime-audio-content-type")
            .is_some_and(|value| value == "audio/pcm");
        let mut responses = response.into_inner();
        let mut converter = Converter::new(format);
        while let Some(response) = responses.message().await.map_err(Error::from)? {
            let Some(synthesis_response_stream::Payload::Audio(data)) = response.payload else {
                continue;
            };
            if data.is_empty() {
                continue;
            }
            if !valid_format {
                return Err(Error::new(
                    ErrorKind::AudioFormat,
                    "expected raw audio/pcm from the service",
                ));
            }
            {
                let mut state = progress.lock().expect("progress mutex poisoned");
                state.received_audio = true;
                state.last_progress = Instant::now();
            }
            enqueue(&audio, &progress, converter.process(&data, false)?).await?;
        }
        if let Some(trailers) = responses.trailers().await.map_err(Error::from)? {
            if let Some(value) = request_id(&trailers) {
                let _ = id.set(value.into());
            }
        }
        if !valid_format {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "expected raw audio/pcm from the service",
            ));
        }
        {
            let mut state = progress.lock().expect("progress mutex poisoned");
            if state.input != InputState::Finished {
                return Err(Error::new(
                    ErrorKind::Stream,
                    "service completed before input finished",
                ));
            }
            state.output_finished = true;
        }
        enqueue(&audio, &progress, converter.process(&[], true)?).await
    };
    futures_util::future::try_join(producer, receiver)
        .map_ok(|_| ())
        .await
}

async fn enqueue(
    sender: &mpsc::Sender<Bytes>,
    progress: &Mutex<Progress>,
    bytes: Vec<u8>,
) -> Result<(), Error> {
    progress
        .lock()
        .expect("progress mutex poisoned")
        .writing_audio = true;
    for chunk in bytes.chunks(CHUNK_BYTES) {
        // Copy each chunk so a small queued view cannot retain a large RPC frame.
        sender
            .send(Bytes::copy_from_slice(chunk))
            .await
            .map_err(|_| Error::new(ErrorKind::Cancelled, "audio stream closed"))?;
    }
    progress
        .lock()
        .expect("progress mutex poisoned")
        .writing_audio = false;
    Ok(())
}

async fn produce<S>(
    source: S,
    sender: mpsc::Sender<StreamingSynthesisRequest>,
    progress: Arc<Mutex<Progress>>,
    voice: String,
    language: String,
    blocking_tasks: TaskTracker,
) -> Result<(), Error>
where
    S: Stream<Item = Result<String, Error>> + Send + 'static,
{
    let header = SynthesisRequest {
        speaker: Some(voice),
        language: Some(language),
        audio_parameters: Some(AudioParameters {
            audio_format: Some("audio/pcm".into()),
            sampling_rate: Some(24000),
            ..Default::default()
        }),
        ..Default::default()
    };
    if sender
        .send(StreamingSynthesisRequest {
            payload: Some(Payload::Header(header)),
        })
        .await
        .is_err()
    {
        return Ok(());
    }
    let mut buffer = blocking_tasks
        .spawn_blocking(Buffer::new)
        .await
        .map_err(worker_error)??;
    let mut source = Box::pin(source);
    let mut meaningful = false;
    loop {
        progress.lock().expect("progress mutex poisoned").input = InputState::Waiting;
        let next = source.next().await;
        progress.lock().expect("progress mutex poisoned").input = InputState::Processing;
        let final_chunk = next.is_none();
        let fragment = next.transpose()?.unwrap_or_default();
        meaningful |= !fragment.trim().is_empty();
        if final_chunk && !meaningful {
            return Err(Error::new(
                ErrorKind::Input,
                "text source contained no meaningful text",
            ));
        }
        // Bound blocking work and emitted text even if a source yields a huge fragment.
        let mut remaining = fragment.as_str();
        loop {
            let end = remaining
                .char_indices()
                .nth(1024)
                .map_or(remaining.len(), |(index, _)| index);
            let part = remaining[..end].to_owned();
            remaining = &remaining[end..];
            let final_part = final_chunk && remaining.is_empty();
            let (returned, sentences) = blocking_tasks
                .spawn_blocking(move || {
                    let sentences = buffer.feed(&part, final_part);
                    (buffer, sentences)
                })
                .await
                .map_err(worker_error)?;
            buffer = returned;
            for sentence in sentences? {
                {
                    let mut state = progress.lock().expect("progress mutex poisoned");
                    if !state.sent_text {
                        state.sent_text = true;
                        state.last_progress = Instant::now();
                    }
                }
                // A closed request pipe can precede the server status. Let the
                // receiver report that status instead of hiding it with a send error.
                if sender
                    .send(StreamingSynthesisRequest {
                        payload: Some(Payload::TextChunk(sentence)),
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            if remaining.is_empty() {
                break;
            }
        }
        if final_chunk {
            break;
        }
    }
    progress.lock().expect("progress mutex poisoned").input = InputState::Finished;
    // Dropping the sender half-closes the request stream.
    Ok(())
}

fn worker_error(error: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Stream, "sentence worker failed").caused_by(error)
}
