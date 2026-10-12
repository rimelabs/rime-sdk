use crate::{
    audio::{resolve_audio, Converter},
    client::{validate_timeout, Inner},
    error::request_id,
    timestamps::TimestampTrailer,
    AudioFormat, Error, ErrorKind, Model, SynthesisOptions, TimestampResult,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use rimelabs_api::{
    streaming_synthesis_request::Payload, synthesis_response_stream, AudioParameters,
    StreamingSynthesisRequest, SynthesisRequest, TimestampOptions,
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
use tokio_util::sync::CancellationToken;

const QUEUE_CHUNKS: usize = 10;
const CHUNK_BYTES: usize = 9_600;

/// An asynchronous stream of audio bytes. An error ends the stream.
///
/// Dropping this value requests cancellation. Use [`Self::close`] to also wait
/// for the stream worker. Cancellation does not affect sibling streams.
pub struct AudioStream {
    receiver: mpsc::Receiver<Bytes>,
    status: WatchStream<Option<Result<(), Error>>>,
    outcome: watch::Receiver<Option<Result<(), Error>>>,
    terminal: Option<Result<(), Error>>,
    failure: OnceLock<Error>,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    worker: Option<JoinHandle<()>>,
    request_id: Arc<OnceLock<String>>,
    format: AudioFormat,
    sample_rate: u32,
    consumed: bool,
    completed: bool,
    timestamps: Option<Arc<Mutex<TimestampTrailer>>>,
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
    /// The output encoding or fixed-rate preset.
    pub fn format(&self) -> AudioFormat {
        self.format
    }
    /// Requested output sample rate in Hz. MP3 headers describe the encoded rate.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }
    /// The service request ID, once response metadata has arrived.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.get().map(String::as_str)
    }
    /// Read requested word timings after the audio stream has returned `None`.
    ///
    /// Never waits or consumes audio. Early or non-opted-in calls return
    /// [`ErrorKind::Input`]. Synthesis errors and cancellation are returned as
    /// errors; alignment failures are reported in [`TimestampResult::status`].
    /// Missing or malformed timestamp trailers return [`ErrorKind::Stream`]
    /// without failing audio. Each call returns an independently owned result.
    pub fn timestamps(&self) -> Result<TimestampResult, Error> {
        let timestamps = self.timestamps.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Input,
                "enable timestamps when creating the stream",
            )
        })?;
        if self.completed {
            return timestamps
                .lock()
                .expect("timestamp mutex poisoned")
                .result(self.request_id());
        }
        if let Some(error) = self.error() {
            return Err(error);
        }
        Err(Error::new(
            ErrorKind::Input,
            "consume all audio before reading timestamps",
        ))
    }
    /// Request cancellation without waiting for cleanup.
    pub fn cancel(&self) {
        if !self.completed && self.error().is_none() {
            let _ = self.failure.set(
                Error::new(ErrorKind::Cancelled, "synthesis cancelled")
                    .with_request_id(self.request_id()),
            );
        }
        self.cancellation.cancel();
    }

    // The worker can finish with audio still buffered. Keep cancellation and the
    // overall deadline effective until the caller consumes EOF, and retain the
    // first observed failure for both audio and timestamps, including after close.
    fn error(&self) -> Option<Error> {
        if let Some(error) = self.failure.get() {
            return Some(error.clone());
        }
        let error = if let Some(Err(error)) = &*self.outcome.borrow() {
            Some(error.clone())
        } else if self.cancellation.is_cancelled() {
            Some(
                Error::new(ErrorKind::Cancelled, "synthesis cancelled")
                    .with_request_id(self.request_id()),
            )
        } else if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(
                Error::new(ErrorKind::Timeout, "synthesis timed out")
                    .with_request_id(self.request_id()),
            )
        } else {
            None
        };
        error.map(|error| self.failure.get_or_init(|| error).clone())
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
        if let Some(error) = this.error() {
            // Match the other SDKs: failure takes priority over queued audio.
            // A paused consumer must observe cancellation or timeout on its next read.
            this.consumed = true;
            this.cancellation.cancel();
            this.receiver.close();
            while this.receiver.try_recv().is_ok() {}
            return Poll::Ready(Some(Err(error)));
        }
        match this.receiver.poll_recv(context) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(None) if this.terminal.is_some() => {
                this.consumed = true;
                this.completed = true;
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
    if options.timestamps && client.model != Model::MistV3 {
        return Err(Error::new(
            ErrorKind::Input,
            "word timestamps are supported only with Model::MistV3",
        ));
    }
    let timeout = options.timeout.unwrap_or(client.timeout);
    validate_timeout(timeout)?;
    let (format, sample_rate) = resolve_audio(
        options.format,
        options.sample_rate,
        options.time_scale_factor,
    )?;
    let voice = options.voice.unwrap_or_else(|| client.model.voice().into());
    let language = options.language.unwrap_or_else(|| "en".into());
    if voice.trim().is_empty() || language.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::Input,
            "voice and language must not be blank",
        ));
    }
    let request = SynthesisRequest {
        split_strategy: Some(rimelabs_api::SplitStrategy::AccumulateSentence as i32),
        speaker: Some(voice),
        language: Some(language),
        audio_parameters: Some(AudioParameters {
            audio_format: Some(format.wire_format().into()),
            sampling_rate: Some(if format == AudioFormat::Mulaw8000 {
                24000
            } else {
                sample_rate as i32
            }),
            time_scale_factor: options.time_scale_factor,
        }),
        timestamps: options
            .timestamps
            .then_some(TimestampOptions { enable: true }),
        custom_lexicon: options
            .custom_lexicon
            .into_iter()
            .map(|entry| rimelabs_api::PronunciationEntry {
                spelling: entry.spelling,
                pronunciation: entry.pronunciation,
            })
            .collect(),
        ..Default::default()
    };
    let (sender, receiver) = mpsc::channel(QUEUE_CHUNKS);
    let (status_sender, status_receiver) = watch::channel(None);
    let cancellation = client.cancellation.child_token();
    let operation_cancellation = cancellation.clone();
    let request_id = Arc::new(OnceLock::new());
    let operation_id = request_id.clone();
    let timestamps = options
        .timestamps
        .then(|| Arc::new(Mutex::new(TimestampTrailer::default())));
    let operation_timestamps = timestamps.clone();
    let progress = Arc::new(Mutex::new(Progress::new()));
    // Construct the timer before spawning so disabled runtime time support
    // cannot panic in a worker after an AudioStream has been returned.
    let interval = tokio::time::interval(Duration::from_millis(20));
    let tracker = client.clone();
    let deadline = timeout.map(|timeout| Instant::now() + timeout);
    let worker = tracker.spawn(async move {
        let run = std::panic::AssertUnwindSafe(run(client.clone(), source, request, sender.clone(), progress.clone(), operation_id.clone(), format, operation_timestamps)).catch_unwind();
        let monitor = monitor(client, progress, sender, interval);
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
    })?;
    Ok(AudioStream {
        receiver,
        status: WatchStream::new(status_receiver.clone()),
        outcome: status_receiver,
        terminal: None,
        failure: OnceLock::new(),
        cancellation,
        deadline,
        worker: Some(worker),
        request_id,
        format,
        sample_rate,
        consumed: false,
        completed: false,
        timestamps,
    })
}

async fn monitor(
    client: Arc<Inner>,
    progress: Arc<Mutex<Progress>>,
    sender: mpsc::Sender<Bytes>,
    mut interval: tokio::time::Interval,
) -> Error {
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
    request: SynthesisRequest,
    audio: mpsc::Sender<Bytes>,
    progress: Arc<Mutex<Progress>>,
    id: Arc<OnceLock<String>>,
    format: AudioFormat,
    timestamps: Option<Arc<Mutex<TimestampTrailer>>>,
) -> Result<(), Error>
where
    S: Stream<Item = Result<String, Error>> + Send + 'static,
{
    let mut stub = client.stub().await?;
    let (requests, incoming) = mpsc::channel(2);
    let producer = produce(source, requests, progress.clone(), request);
    let receiver = async {
        let response = stub
            .synthesize_streaming(client.request(ReceiverStream::new(incoming)))
            .await
            .map_err(Error::from)?;
        receive(response, audio, progress.clone(), id, format, timestamps).await
    };
    futures_util::future::try_join(producer, receiver)
        .map_ok(|_| ())
        .await
}

async fn receive(
    response: tonic::Response<tonic::Streaming<rimelabs_api::SynthesisResponseStream>>,
    audio: mpsc::Sender<Bytes>,
    progress: Arc<Mutex<Progress>>,
    id: Arc<OnceLock<String>>,
    format: AudioFormat,
    timestamps: Option<Arc<Mutex<TimestampTrailer>>>,
) -> Result<(), Error> {
    if let Some(value) = request_id(response.metadata()) {
        let _ = id.set(value.into());
    }
    let valid_format = response
        .metadata()
        .get("x-rime-audio-content-type")
        .is_some_and(|value| value == format.wire_format());
    let mut responses = response.into_inner();
    let mut converter = Converter::new(format);
    while let Some(response) = responses.message().await.map_err(Error::from)? {
        let data = match response.payload {
            Some(synthesis_response_stream::Payload::Trailer(trailer)) => {
                if let Some(timestamps) = &timestamps {
                    timestamps
                        .lock()
                        .expect("timestamp mutex poisoned")
                        .accept(trailer);
                }
                continue;
            }
            Some(synthesis_response_stream::Payload::Audio(data)) => data,
            None => continue,
        };
        if let Some(timestamps) = &timestamps {
            let mut timestamps = timestamps.lock().expect("timestamp mutex poisoned");
            if timestamps.seen {
                timestamps.invalid = true;
            }
        }
        if data.is_empty() {
            continue;
        }
        if !valid_format {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                format!("expected {} from the service", format.wire_format()),
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
            format!("expected {} from the service", format.wire_format()),
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
    header: SynthesisRequest,
) -> Result<(), Error>
where
    S: Stream<Item = Result<String, Error>> + Send + 'static,
{
    if sender
        .send(StreamingSynthesisRequest {
            payload: Some(Payload::Header(header)),
        })
        .await
        .is_err()
    {
        return Ok(());
    }
    let mut source = Box::pin(source);
    let mut meaningful = false;
    loop {
        progress.lock().expect("progress mutex poisoned").input = InputState::Waiting;
        let next = source.next().await;
        progress.lock().expect("progress mutex poisoned").input = InputState::Processing;
        let Some(fragment) = next else { break };
        let fragment = fragment?;
        meaningful |= !fragment.trim().is_empty();
        // Bound wire messages; Rime handles sentence accumulation automatically.
        let mut remaining = fragment.as_str();
        while !remaining.is_empty() {
            let mut end = remaining.len().min(4096);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let part = remaining[..end].to_owned();
            remaining = &remaining[end..];
            {
                let mut state = progress.lock().expect("progress mutex poisoned");
                if !state.sent_text {
                    state.sent_text = true;
                    state.last_progress = Instant::now();
                }
            }
            // The receiver preserves the server status when the request pipe closes.
            if sender
                .send(StreamingSynthesisRequest {
                    payload: Some(Payload::TextChunk(part)),
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            progress
                .lock()
                .expect("progress mutex poisoned")
                .last_progress = Instant::now();
        }
    }
    if !meaningful {
        return Err(Error::new(
            ErrorKind::Input,
            "text source contained no meaningful text",
        ));
    }
    progress.lock().expect("progress mutex poisoned").input = InputState::Finished;
    // Dropping the sender half-closes the request stream.
    Ok(())
}

#[cfg(test)]
#[path = "tests/tts_features.rs"]
mod tests;
