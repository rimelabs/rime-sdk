use crate::{
    audio::{AudioProfile, Converter},
    client::Client,
    error::{CoreError, Result, request_id},
    policy,
    sentences::SentenceBuffer,
};
use bytes::Bytes;
use sdk_protocol::{
    AudioParameters, StreamingSynthesisRequest, SynthesisRequest,
    streaming_synthesis_request::Payload,
};
use serde::Deserialize;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
#[serde(default)]
pub struct StreamOptions {
    pub voice: String,
    pub language: String,
    pub profile: String,
    pub timeout: Option<f64>,
    pub inherit_timeout: bool,
}
impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            voice: "clementine".into(),
            language: "en".into(),
            profile: "PCM_24000".into(),
            timeout: None,
            inherit_timeout: true,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputReply {
    Item,
    End,
    Text { text: String, last: bool },
    Utf16 { units: Vec<u16>, last: bool },
    Error,
}
struct InputRequest {
    kind: u32,
    reply: oneshot::Sender<InputReply>,
}
struct State {
    error: Option<CoreError>,
    request_id: Option<String>,
    queue: VecDeque<Bytes>,
    bytes: usize,
    candidate: Option<(u32, usize, bool)>,
    serial: u32,
    reading: bool,
    produced: bool,
    completed: bool,
    source_waiting: bool,
    submitted: bool,
    received: bool,
    input_done: bool,
    activated: Option<tokio::time::Instant>,
    progress: tokio::time::Instant,
}
pub struct ReadCandidate {
    pub data: Option<Bytes>,
    pub ticket: u32,
}
pub struct AudioStream {
    client: Arc<Client>,
    options: StreamOptions,
    profile: AudioProfile,
    state: Mutex<State>,
    changed: Notify,
    stopped: CancellationToken,
    started: AtomicBool,
    input_tx: mpsc::Sender<InputRequest>,
    input_rx: AsyncMutex<mpsc::Receiver<InputRequest>>,
    pending_reply: Mutex<Option<oneshot::Sender<InputReply>>>,
}
impl AudioStream {
    pub(crate) fn new(client: Arc<Client>, mut options: StreamOptions) -> Result<Arc<Self>> {
        if options.inherit_timeout {
            options.timeout = client.default_timeout;
        }
        if options.voice.trim().is_empty() || options.language.trim().is_empty() {
            return Err(CoreError::input(
                "voice and language must be non-empty strings",
            ));
        }
        policy::timeout(options.timeout)?;
        let profile = AudioProfile::parse(&options.profile)?;
        let (input_tx, input_rx) = mpsc::channel(1);
        Ok(Arc::new(Self {
            client,
            options,
            profile,
            state: Mutex::new(State {
                error: None,
                request_id: None,
                queue: VecDeque::new(),
                bytes: 0,
                candidate: None,
                serial: 0,
                reading: false,
                produced: false,
                completed: false,
                source_waiting: false,
                submitted: false,
                received: false,
                input_done: false,
                activated: None,
                progress: tokio::time::Instant::now(),
            }),
            changed: Notify::new(),
            stopped: CancellationToken::new(),
            started: AtomicBool::new(false),
            input_tx,
            input_rx: AsyncMutex::new(input_rx),
            pending_reply: Mutex::new(None),
        }))
    }
    pub fn request_id(&self) -> Option<String> {
        self.state.lock().unwrap().request_id.clone()
    }
    pub fn input_done(&self) -> bool {
        self.state.lock().unwrap().input_done
    }
    pub fn source_chars(&self) -> usize {
        self.client.policy.source_chars
    }
    pub fn cleanup_timeout(&self) -> f64 {
        self.client.policy.cleanup_timeout
    }
    pub async fn wait_produced(&self) {
        loop {
            let change = self.changed.notified();
            if { self.state.lock().unwrap().produced } || self.terminal() {
                break;
            }
            change.await;
        }
    }
    pub fn active(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
    pub fn terminal(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.error.is_some() || s.completed
    }
    fn error(&self) -> Option<CoreError> {
        let s = self.state.lock().unwrap();
        s.error.clone().map(|mut e| {
            e.request_id = s.request_id.clone().or(e.request_id);
            e
        })
    }
    pub fn fail(&self, mut error: CoreError) {
        let mut state = self.state.lock().unwrap();
        if state.completed || state.error.is_some() {
            return;
        }
        if state.request_id.is_none() {
            state.request_id = error.request_id.clone();
        }
        error.request_id = state.request_id.clone();
        state.error = Some(error);
        state.queue.clear();
        state.bytes = 0;
        state.candidate = None;
        state.reading = false;
        drop(state);
        self.stopped.cancel();
        self.changed.notify_waiters();
        self.pending_reply.lock().unwrap().take();
    }
    pub fn cancel(&self) {
        self.fail(CoreError::cancelled());
    }
    pub fn fail_source(&self) {
        self.fail(CoreError::input("The text source failed"));
    }
    pub async fn wait_stopped(&self) {
        self.stopped.cancelled().await;
    }
    pub fn activate(self: &Arc<Self>) -> Result<()> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        self.client.ensure_open()?;
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        {
            let mut s = self.state.lock().unwrap();
            s.activated = Some(now);
            s.progress = now;
        }
        let stream = self.clone();
        tokio::spawn(async move {
            let result = tokio::select! { biased;
                _=stream.stopped.cancelled()=>Ok(()),
                _=stream.client.stopped.cancelled()=>Err(CoreError::cancelled()),
                result=stream.run()=>result,
            };
            if let Err(error) = result {
                stream.fail(error);
            }
        });
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let Some(stream) = weak.upgrade() else {
                    break;
                };
                if stream.terminal() {
                    break;
                }
                if let Some(error) = stream.deadline_error() {
                    stream.fail(error);
                    break;
                }
            }
        });
        Ok(())
    }
    fn deadline_error(&self) -> Option<CoreError> {
        self.deadline_error_locked(&mut self.state.lock().unwrap())
    }
    fn deadline_error_locked(&self, s: &mut State) -> Option<CoreError> {
        let now = tokio::time::Instant::now();
        if s.completed || s.error.is_some() {
            return None;
        }
        if let (Some(start), Some(limit)) = (s.activated, self.options.timeout)
            && now.duration_since(start).as_secs_f64() >= limit
        {
            return Some(CoreError::new(
                "Timeout",
                "Overall synthesis deadline expired",
            ));
        }
        if s.submitted && !s.source_waiting && s.bytes == 0 && !s.produced {
            let limit = if s.received {
                self.client.policy.progress_timeout
            } else {
                self.client.policy.first_audio_timeout
            };
            if now.duration_since(s.progress).as_secs_f64() >= limit {
                return Some(CoreError::new(
                    "Timeout",
                    "Synthesis output stopped making progress",
                ));
            }
        } else {
            s.progress = now;
        }
        None
    }
    pub async fn input_request(&self) -> Option<u32> {
        tokio::select! { biased;
            _=self.stopped.cancelled()=>None,
            request=async {self.input_rx.lock().await.recv().await}=>request.map(|r| { *self.pending_reply.lock().unwrap()=Some(r.reply); r.kind }),
        }
    }
    pub fn input_reply(&self, reply: InputReply) -> Result<()> {
        let valid = match &reply {
            InputReply::Text { text, .. } => text.len() <= self.client.policy.source_chars * 4,
            InputReply::Utf16 { units, .. } => units.len() <= self.client.policy.source_chars,
            _ => true,
        };
        if !valid {
            let error = CoreError::input("Source adapter exceeded fragment budget");
            self.fail(error.clone());
            return Err(error);
        }
        if let Some(sender) = self.pending_reply.lock().unwrap().take() {
            let _ = sender.send(reply);
            return Ok(());
        }
        if self.stopped.is_cancelled() {
            Ok(())
        } else {
            Err(CoreError::input("No pending input request"))
        }
    }
    async fn ask(&self, kind: u32) -> Result<InputReply> {
        let (reply, receiver) = oneshot::channel();
        {
            self.state.lock().unwrap().source_waiting = kind == 0;
        }
        let result = async {
            self.input_tx
                .send(InputRequest { kind, reply })
                .await
                .map_err(|_| CoreError::cancelled())?;
            receiver.await.map_err(|_| CoreError::cancelled())
        }
        .await;
        self.state.lock().unwrap().source_waiting = false;
        result
    }
    async fn produce(&self, sender: mpsc::Sender<StreamingSynthesisRequest>) -> Result<()> {
        let mut buffer = SentenceBuffer::new(self.client.policy.sentence_bytes);
        let mut meaningful = false;
        let mut high_surrogate = None;
        loop {
            match self.ask(0).await? {
                InputReply::End => break,
                InputReply::Item => {}
                _ => return Err(CoreError::input("The text source failed")),
            }
            loop {
                let (text, last) = match self.ask(1).await? {
                    InputReply::Text { text, last } => (text, last),
                    InputReply::Utf16 { mut units, last } => {
                        if let Some(high) = high_surrogate.take() {
                            units.insert(0, high);
                        }
                        if units.last().is_some_and(|v| (0xd800..=0xdbff).contains(v)) {
                            high_surrogate = units.pop();
                        }
                        (
                            String::from_utf16(&units).map_err(|_| {
                                CoreError::input("Text contains an unpaired UTF-16 surrogate")
                            })?,
                            last,
                        )
                    }
                    _ => return Err(CoreError::input("The text source failed")),
                };
                meaningful |= !text.trim().is_empty();
                for sentence in buffer.feed(&text, false)? {
                    self.write_sentence(&sender, sentence).await?;
                }
                if last {
                    break;
                }
            }
        }
        if high_surrogate.is_some() {
            return Err(CoreError::input(
                "Text contains an unpaired UTF-16 surrogate",
            ));
        }
        if !meaningful {
            return Err(CoreError::input(
                "The text source contained no meaningful text",
            ));
        }
        for sentence in buffer.feed("", true)? {
            self.write_sentence(&sender, sentence).await?;
        }
        self.state.lock().unwrap().input_done = true;
        drop(sender);
        Ok(())
    }
    async fn write_sentence(
        &self,
        sender: &mpsc::Sender<StreamingSynthesisRequest>,
        text: String,
    ) -> Result<()> {
        {
            let mut s = self.state.lock().unwrap();
            if !s.submitted {
                s.progress = tokio::time::Instant::now();
            }
            s.submitted = true;
        }
        sender
            .send(StreamingSynthesisRequest {
                payload: Some(Payload::TextChunk(text)),
            })
            .await
            .map_err(|_| CoreError::new("Stream", "The service completed before input finished"))
    }
    async fn run(&self) -> Result<()> {
        let mut connection = self.client.connection().await?;
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(StreamingSynthesisRequest {
                payload: Some(Payload::Header(SynthesisRequest {
                    speaker: Some(self.options.voice.clone()),
                    language: Some(self.options.language.clone()),
                    audio_parameters: Some(AudioParameters {
                        audio_format: Some("audio/pcm".into()),
                        sampling_rate: Some(24000),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            })
            .await
            .map_err(|_| CoreError::cancelled())?;
        let request = self
            .client
            .request(tokio_stream::wrappers::ReceiverStream::new(receiver))?;
        let produce = self.produce(sender);
        let read = async {
            let response = connection
                .synthesize_streaming(request)
                .await
                .map_err(CoreError::status)?;
            let format = response
                .metadata()
                .get("x-rime-audio-content-type")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            self.state.lock().unwrap().request_id = request_id(response.metadata());
            let mut response = response.into_inner();
            let mut converter = Converter::new(self.profile);
            while let Some(message) = response.message().await.map_err(CoreError::status)? {
                if !message.audio.is_empty() {
                    if format.as_deref() != Some("audio/pcm") {
                        return Err(CoreError::new(
                            "AudioFormat",
                            "Expected raw audio/pcm from the service",
                        ));
                    }
                    {
                        let mut s = self.state.lock().unwrap();
                        s.received = true;
                        s.progress = tokio::time::Instant::now();
                    }
                    // Limit intermediate conversion memory independently of the receive frame.
                    for chunk in message.audio.chunks(self.client.policy.output_chunk_bytes) {
                        self.enqueue(converter.process(chunk, false)?).await?;
                    }
                }
            }
            if let Some(trailers) = response.trailers().await.map_err(CoreError::status)? {
                let mut s = self.state.lock().unwrap();
                if s.request_id.is_none() {
                    s.request_id = request_id(&trailers);
                }
            }
            if format.as_deref() != Some("audio/pcm") {
                return Err(CoreError::new(
                    "AudioFormat",
                    "Expected raw audio/pcm from the service",
                ));
            }
            if !self.state.lock().unwrap().input_done {
                return Err(CoreError::new(
                    "Stream",
                    "The service completed before input finished",
                ));
            }
            self.enqueue(converter.process(&[], true)?).await?;
            Ok(())
        };
        // Poll transport first so an early server status wins over a closed input pipe.
        tokio::pin!(read, produce);
        tokio::select! { biased;
            result=&mut read=>{result?;produce.await?;},
            result=&mut produce=>{
                if let Err(error)=result {
                    if error.kind=="Stream" {return read.await;}
                    return Err(error);
                }
                read.await?;
            }
        }
        self.state.lock().unwrap().produced = true;
        self.changed.notify_waiters();
        Ok(())
    }
    async fn enqueue(&self, data: Vec<u8>) -> Result<()> {
        for chunk in data.chunks(self.client.policy.output_chunk_bytes) {
            loop {
                let changed = self.changed.notified();
                {
                    let mut s = self.state.lock().unwrap();
                    if let Some(error) = &s.error {
                        return Err(error.clone());
                    }
                    if s.bytes + chunk.len() <= self.client.policy.output_bytes {
                        s.bytes += chunk.len();
                        s.queue.push_back(Bytes::copy_from_slice(chunk));
                        self.changed.notify_waiters();
                        break;
                    }
                }
                changed.await;
            }
        }
        Ok(())
    }
    pub async fn next(self: &Arc<Self>) -> Result<ReadCandidate> {
        self.activate()?;
        if let Some(error) = self.deadline_error() {
            self.fail(error);
        }
        {
            let mut s = self.state.lock().unwrap();
            if let Some(error) = &s.error {
                return Err(error.clone());
            }
            if s.reading {
                return Err(CoreError::input(
                    "AudioStream permits only one concurrent reader",
                ));
            }
            s.reading = true;
        }
        loop {
            let changed = self.changed.notified();
            {
                let mut s = self.state.lock().unwrap();
                if let Some(error) = &s.error {
                    return Err(error.clone());
                }
                if !s.queue.is_empty() || s.produced || s.completed {
                    let data = s.queue.pop_front();
                    let end = data.is_none();
                    s.serial = s.serial.wrapping_add(1);
                    let ticket = s.serial;
                    s.candidate = Some((ticket, data.as_ref().map_or(0, Bytes::len), end));
                    return Ok(ReadCandidate { data, ticket });
                }
            }
            changed.await;
        }
    }
    pub fn accept_read(&self, ticket: u32) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if let Some(error) = self.deadline_error_locked(&mut s) {
            drop(s);
            self.fail(error);
            return Err(self.error().unwrap());
        }
        if let Some(error) = &s.error {
            return Err(error.clone());
        }
        let Some((expected, size, end)) = s.candidate else {
            return Err(CoreError::input("No pending read"));
        };
        if ticket != expected {
            return Err(CoreError::input("Invalid read ticket"));
        }
        s.candidate = None;
        s.reading = false;
        s.bytes -= size;
        s.progress = tokio::time::Instant::now();
        if end {
            s.completed = true;
            self.stopped.cancel();
        }
        drop(s);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientConfig;
    fn stream() -> Arc<AudioStream> {
        let client = Client::new(ClientConfig {
            api_key: "test".into(),
            model: "coda".into(),
            endpoint: None,
            timeout: Some(1.),
        })
        .unwrap();
        let stream = client.stream(StreamOptions::default()).unwrap();
        stream.started.store(true, Ordering::Release); // no network in state tests
        stream.state.lock().unwrap().activated = Some(tokio::time::Instant::now());
        stream
    }
    #[tokio::test]
    async fn pending_candidate_retains_capacity_and_cancel_invalidates_it() {
        let s = stream();
        s.enqueue(vec![1; 96000]).await.unwrap();
        let read = s.next().await.unwrap();
        assert_eq!(s.state.lock().unwrap().bytes, 96000);
        assert_eq!(s.next().await.err().unwrap().kind, "Input");
        s.cancel();
        assert_eq!(s.accept_read(read.ticket).unwrap_err().kind, "Cancelled");
        assert_eq!(s.state.lock().unwrap().bytes, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn eof_candidate_can_expire_but_accepted_eof_cannot() {
        let s = stream();
        s.state.lock().unwrap().produced = true;
        let read = s.next().await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(s.accept_read(read.ticket).unwrap_err().kind, "Timeout");
        let s = stream();
        s.state.lock().unwrap().produced = true;
        let read = s.next().await.unwrap();
        s.accept_read(read.ticket).unwrap();
        s.cancel();
        assert!(s.error().is_none());
    }
    #[tokio::test]
    async fn failure_wakes_blocked_producer_and_reader_and_first_error_wins() {
        let s = stream();
        let writer = {
            let s = s.clone();
            tokio::spawn(async move { s.enqueue(vec![1; 96001]).await })
        };
        tokio::task::yield_now().await;
        s.fail(CoreError::new("Unavailable", "first"));
        s.cancel();
        assert_eq!(writer.await.unwrap().unwrap_err().kind, "Unavailable");
        assert_eq!(s.next().await.err().unwrap().message, "first");
        let s = stream();
        let reader = {
            let s = s.clone();
            tokio::spawn(async move { s.next().await })
        };
        tokio::task::yield_now().await;
        s.cancel();
        assert!(reader.await.unwrap().is_err());
    }
    #[tokio::test]
    async fn chunks_are_bounded_ordered_and_empty_chunks_are_omitted() {
        let s = stream();
        s.enqueue(vec![]).await.unwrap();
        assert!(s.state.lock().unwrap().queue.is_empty());
        let data: Vec<u8> = (0..150000).map(|i| (i % 251) as u8).collect();
        let writer = {
            let s = s.clone();
            let data = data.clone();
            tokio::spawn(async move {
                s.enqueue(data).await.unwrap();
                s.state.lock().unwrap().produced = true;
                s.changed.notify_waiters();
            })
        };
        let mut output = vec![];
        loop {
            let r = s.next().await.unwrap();
            s.accept_read(r.ticket).unwrap();
            match r.data {
                None => break,
                Some(b) => {
                    assert!(b.len() <= 9600);
                    output.extend(b);
                }
            }
        }
        writer.await.unwrap();
        assert_eq!(output, data);
    }
}
