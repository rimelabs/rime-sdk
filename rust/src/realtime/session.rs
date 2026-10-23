// SPDX-License-Identifier: Apache-2.0
// Prism protocol behavior adapted from the existing SDKs; see NOTICE-PRISM.
use super::{
    client::text,
    protocol::{self, Wire},
    transport::{Transport, Write},
    types::*,
};
use crate::{
    client::Inner,
    pcm::{InputAudio, SOURCE_BYTES},
    Error, ErrorKind,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_core::Stream;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, oneshot, Notify},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

struct Lease(CancellationToken);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.cancel()
    }
}
struct CallGuard {
    status: Arc<AtomicU8>,
    wake: Arc<Notify>,
    cancelled: CancellationToken,
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.status.load(Ordering::Acquire) == 0 {
            self.status.store(2, Ordering::Release)
        }
        if self.status.load(Ordering::Acquire) == 2 {
            self.cancelled.cancel();
        }
        self.wake.notify_one()
    }
}
#[derive(Default)]
struct InterruptPolicies {
    changes: VecDeque<(u64, bool)>,
}
impl InterruptPolicies {
    fn record(&mut self, revision: Option<u64>, interrupt: bool) {
        if self.changes.is_empty() {
            self.changes.push_back((0, interrupt));
        } else if let Some(revision) = revision {
            if self
                .changes
                .back()
                .is_some_and(|(_, value)| *value != interrupt)
            {
                self.changes.push_back((revision, interrupt));
                if self.changes.len() > 128 {
                    self.changes.pop_front();
                }
            }
        }
    }
    fn resolve(&self, revision: Option<u64>, fallback: bool) -> Result<bool, Error> {
        let Some(revision) = revision else {
            return Ok(fallback);
        };
        self.changes
            .iter()
            .rev()
            .find(|(start, _)| *start <= revision)
            .map(|(_, value)| *value)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ResourceLimit,
                    "response interruption policy revision expired",
                )
            })
    }
}

/// A cloneable Prism session handle. Dropping the last handle requests cleanup.
#[derive(Clone)]
pub struct RealtimeSession {
    lease: Arc<Lease>,
    commands: mpsc::Sender<Command>,
    events: Arc<Mutex<Option<RealtimeEvents>>>,
    info: Arc<Mutex<Option<SessionInfo>>>,
    response_policies: Arc<Mutex<HashMap<ResponseRef, bool>>>,
    budgets: RealtimeTimeouts,
    done: CancellationToken,
    wake: Arc<Notify>,
    terminal: Arc<Mutex<Option<Error>>>,
}
/// The session's single owned event stream. Normal closure ends the stream.
/// A terminal failure takes precedence over queued events, then ends the stream.
pub struct RealtimeEvents {
    receiver: mpsc::Receiver<SessionEvent>,
    response_policies: Arc<Mutex<HashMap<ResponseRef, bool>>>,
    previous_response: Option<ResponseRef>,
    terminal: Arc<Mutex<Option<Error>>>,
    ended: bool,
}
impl Stream for RealtimeEvents {
    type Item = Result<SessionEvent, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Keep a delivered response's policy until the consumer asks for another event.
        if let Some(previous) = self.previous_response.take() {
            self.response_policies
                .lock()
                .expect("response policy mutex")
                .remove(&previous);
        }
        if self.ended {
            return Poll::Ready(None);
        }
        let error = self.terminal.lock().expect("terminal mutex").clone();
        if let Some(e) = error {
            self.ended = true;
            return Poll::Ready(Some(Err(e)));
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(e)) => {
                if let RealtimeEvent::ResponseStarted { response, .. } = &e.payload {
                    self.previous_response = Some(response.clone());
                }
                Poll::Ready(Some(Ok(e)))
            }
            Poll::Ready(None) => {
                self.ended = true;
                // Failure can be published after the first check but before the
                // worker drops the sender. Do not turn that race into normal EOF.
                let error = self.terminal.lock().expect("terminal mutex").clone();
                Poll::Ready(error.map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for RealtimeEvents {
    fn drop(&mut self) {
        // Closing first makes concurrent publication fail and release its policy.
        self.receiver.close();
        self.response_policies
            .lock()
            .expect("response policy mutex")
            .clear();
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Text,
    Reply,
    History,
    Continue,
    Tool,
    Cancel,
    Clear,
    Audio,
    Truncate,
    Drain,
    Update,
    Labels,
}
struct Command {
    epoch: u64,
    kind: Kind,
    body: Value,
    reference: Option<ResponseRef>,
    call: Option<ToolCallRef>,
    output: Option<OutputRef>,
    audio: Option<AudioChunk>,
    digest: Option<[u8; 32]>,
    converter: Option<(crate::PcmFormat, InputAudio)>,
    status: Arc<AtomicU8>,
    cancelled: CancellationToken,
    result: Option<oneshot::Sender<Result<Answer, Error>>>,
    deadline: Instant,
    id: u64,
    expected: &'static str,
    target: String,
    item_id: String,
    frames: Vec<String>,
    submitted: bool,
    written: bool,
    finished: bool,
    accepted: Option<ResponseRef>,
    ack: Option<Wire>,
    admission_deadline: Instant,
}
#[derive(Default)]
struct Answer {
    info: Option<SessionInfo>,
    response: Option<ResponseRef>,
    item: Option<ItemRef>,
}
impl Command {
    fn new(kind: Kind, body: Value) -> Self {
        Self {
            epoch: 0,
            kind,
            body,
            reference: None,
            call: None,
            output: None,
            audio: None,
            digest: None,
            converter: None,
            status: Arc::new(AtomicU8::new(0)),
            cancelled: CancellationToken::new(),
            result: None,
            deadline: Instant::now(),
            admission_deadline: Instant::now(),
            id: 0,
            expected: "",
            target: String::new(),
            item_id: String::new(),
            frames: vec![],
            submitted: false,
            written: false,
            finished: false,
            accepted: None,
            ack: None,
        }
    }
    fn finish(&mut self, r: Result<Answer, Error>) {
        if let Some(tx) = self.result.take() {
            let _ = tx.send(r);
        }
        self.finished = true;
    }
    fn abandoned(&self) -> bool {
        self.status.load(Ordering::Acquire) == 2
    }
}
struct Response {
    reference: ResponseRef,
    done: bool,
    continued: bool,
    abandoned: bool,
    token: Option<String>,
    calls: HashMap<String, Option<[u8; 32]>>,
    outputs: HashSet<OutputRef>,
}
struct Worker {
    epoch: u64,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<SessionEvent>,
    pending: Vec<Command>,
    responses: HashMap<String, Response>,
    order: VecDeque<String>,
    ready: bool,
    latest: String,
    id: String,
    sequence: u64,
    converter: Option<(crate::PcmFormat, InputAudio)>,
    timeouts: RealtimeTimeouts,
    interrupt: bool,
    interrupt_policies: InterruptPolicies,
    initialized: Option<oneshot::Sender<Result<SessionInfo, Error>>>,
    info: Arc<Mutex<Option<SessionInfo>>>,
    response_policies: Arc<Mutex<HashMap<ResponseRef, bool>>>,
}
pub(super) async fn connect(
    inner: Arc<Inner>,
    options: RealtimeConnectOptions,
    settings: Value,
) -> Result<RealtimeSession, Error> {
    let budgets = options.timeouts;
    let stop = inner.cancellation.child_token();
    let lease = Arc::new(Lease(stop.clone()));
    let done = CancellationToken::new();
    let finished = done.clone();
    let terminal = Arc::new(Mutex::new(None));
    let error_slot = terminal.clone();
    let (writes, commands) = mpsc::channel(128);
    let (events, receiver) = mpsc::channel(256);
    let (tx, rx) = oneshot::channel();
    let info = Arc::new(Mutex::new(None));
    let worker_info = info.clone();
    let response_policies = Arc::new(Mutex::new(HashMap::new()));
    let worker_response_policies = response_policies.clone();
    let wake = Arc::new(Notify::new());
    let notify = wake.clone();
    let authorization = inner
        .authorization
        .to_str()
        .map_err(|_| Error::new(ErrorKind::Authentication, "invalid credentials"))?
        .to_owned();
    inner.spawn(async move {
        let mut worker = Worker {
            epoch: 0,
            commands,
            events,
            pending: vec![],
            responses: HashMap::new(),
            order: VecDeque::new(),
            ready: false,
            latest: String::new(),
            id: String::new(),
            sequence: 0,
            converter: None,
            timeouts: options.timeouts,
            interrupt: options.interrupt,
            interrupt_policies: InterruptPolicies::default(),
            initialized: Some(tx),
            info: worker_info,
            response_policies: worker_response_policies,
        };
        let result = worker
            .run(
                &options.endpoint,
                &authorization,
                options.request_id.as_deref(),
                settings,
                stop.clone(),
                notify,
            )
            .await;
        if let Err(e) = &result {
            *error_slot.lock().expect("terminal mutex") = Some(e.clone())
        }
        stop.cancel();
        let failure = result.err().unwrap_or_else(Error::realtime_closed);
        if let Some(tx) = worker.initialized.take() {
            let _ = tx.send(Err(failure.clone()));
        }
        for c in &mut worker.pending {
            c.finish(Err(failure.clone()))
        }
        while let Ok(mut c) = worker.commands.try_recv() {
            c.finish(Err(failure.clone()))
        }
        drop(worker);
        finished.cancel();
    })?;
    rx.await
        .map_err(|_| Error::new(ErrorKind::Stream, "realtime worker stopped"))??;
    Ok(RealtimeSession {
        lease,
        commands: writes,
        events: Arc::new(Mutex::new(Some(RealtimeEvents {
            receiver,
            response_policies: response_policies.clone(),
            previous_response: None,
            terminal: terminal.clone(),
            ended: false,
        }))),
        info,
        response_policies,
        budgets,
        done,
        wake,
        terminal,
    })
}
impl RealtimeSession {
    /// An owned snapshot of the current acknowledged server settings.
    pub fn info(&self) -> SessionInfo {
        self.info
            .lock()
            .expect("session info mutex")
            .as_ref()
            .expect("initialized session")
            .clone()
    }
    pub(super) fn response_interrupt_on_speech(
        &self,
        response: &ResponseRef,
    ) -> Result<bool, Error> {
        self.response_policies
            .lock()
            .expect("response policy mutex")
            .get(response)
            .copied()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ResourceLimit,
                    "response interruption policy expired",
                )
            })
    }
    /// Apply supplied settings and wait for acknowledgment. Replies already
    /// started keep their settings. Dropping the future after submission does
    /// not undo an accepted update.
    pub async fn update(&self, options: RealtimeUpdateOptions) -> Result<SessionInfo, Error> {
        let settings = super::client::patch(options)?;
        self.invoke(Command::new(Kind::Update, json!({"session": settings})))
            .await?
            .info
            .ok_or_else(|| failure("missing acknowledged settings"))
    }
    /// Take the only owned event stream. A second call fails.
    pub fn events(&self) -> Result<RealtimeEvents, Error> {
        self.events
            .lock()
            .expect("events mutex")
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Input, "event stream already taken"))
    }
    /// Request shutdown and wait for socket and worker cleanup.
    pub async fn close(&self) {
        self.lease.0.cancel();
        self.done.cancelled().await
    }
    async fn invoke(&self, mut c: Command) -> Result<Answer, Error> {
        if self.lease.0.is_cancelled() {
            return Err(self.failure());
        }
        let (tx, rx) = oneshot::channel();
        c.result = Some(tx);
        let guard = CallGuard {
            status: c.status.clone(),
            wake: self.wake.clone(),
            cancelled: c.cancelled.clone(),
        };
        c.deadline = Instant::now() + self.budgets.ready;
        c.admission_deadline = c.deadline;
        self.commands.try_send(c).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                Error::new(ErrorKind::ResourceLimit, "too many realtime operations")
            }
            mpsc::error::TrySendError::Closed(_) => self.failure(),
        })?;
        let answer = rx.await.map_err(|_| self.failure())?;
        guard.status.store(1, Ordering::Release);
        drop(guard);
        answer
    }
    fn failure(&self) -> Error {
        self.terminal
            .lock()
            .expect("terminal mutex")
            .clone()
            .unwrap_or_else(Error::realtime_closed)
    }
    /// Request a response to a typed user turn after admission.
    pub async fn send_text(&self, value: impl Into<String>) -> Result<ResponseRef, Error> {
        let value = value.into();
        text(&value, 4000)?;
        let a=self.invoke(Command::new(Kind::Text,json!({"response":{"prism_input_text":value,"metadata":{"prism_cause":"user_text"}}}))).await?;
        a.response
            .ok_or_else(|| Error::new(ErrorKind::Stream, "missing accepted response"))
    }
    /// Add a user or assistant history message without a reply.
    pub async fn add_message(
        &self,
        role: &str,
        value: impl Into<String>,
    ) -> Result<ItemRef, Error> {
        let value = value.into();
        text(&value, 4000)?;
        if !matches!(role, "user" | "assistant") {
            return Err(Error::new(
                ErrorKind::Input,
                "history role must be user or assistant",
            ));
        }
        self.invoke(Command::new(
            Kind::History,
            json!({"item":{"type":"message","role":role,"text":value}}),
        ))
        .await?
        .item
        .ok_or_else(|| Error::new(ErrorKind::Stream, "missing history item"))
    }
    /// Request a proactive reply, optionally about a recorded tool result.
    pub async fn request_reply(&self, options: ReplyOptions) -> Result<ResponseRef, Error> {
        let mut body = json!({"response":{"metadata":{"prism_cause":"proactive"}}});
        if let Some(s) = options.instruction {
            text(&s, 4000)?;
            body["response"]["prism_instruction"] = json!(s)
        }
        if let Some(c) = &options.tool_call {
            body["response"]["prism_call_id"] = json!(c.call_id)
        }
        let mut c = Command::new(Kind::Reply, body);
        c.call = options.tool_call;
        self.invoke(c)
            .await?
            .response
            .ok_or_else(|| Error::new(ErrorKind::Stream, "missing response"))
    }
    /// Continue the latest completed tool round once all tool results are recorded.
    pub async fn continue_reply(&self, parent: &ResponseRef) -> Result<ResponseRef, Error> {
        let mut c = Command::new(
            Kind::Continue,
            json!({"response":{"metadata":{"prism_cause":"tool_continuation","prism_parent_response_id":parent.response_id}}}),
        );
        c.reference = Some(parent.clone());
        self.invoke(c)
            .await?
            .response
            .ok_or_else(|| Error::new(ErrorKind::Stream, "missing continuation"))
    }
    /// Record an application tool result. Repeating the same result is harmless.
    pub async fn submit_tool_result(
        &self,
        call: &ToolCallRef,
        output: impl Into<String>,
    ) -> Result<(), Error> {
        let output = output.into();
        let digest = Sha256::digest(output.as_bytes()).into();
        let mut c = Command::new(
            Kind::Tool,
            json!({"item":{"type":"function_call_output","call_id":call.call_id,"output":output}}),
        );
        c.reference = Some(call.response.clone());
        c.call = Some(call.clone());
        c.digest = Some(digest);
        self.invoke(c).await.map(|_| ())
    }
    /// Cancel a response. Repeated cancellation after completion is harmless.
    pub async fn cancel(&self, reference: &ResponseRef) -> Result<(), Error> {
        let mut c = Command::new(Kind::Cancel, json!({"response_id":reference.response_id}));
        c.reference = Some(reference.clone());
        self.invoke(c).await.map(|_| ())
    }
    /// Replace the labels later replies carry; a reply already started keeps
    /// its own. Returns once Prism confirms the change.
    pub async fn set_response_metadata(&self, labels: Labels) -> Result<(), Error> {
        let c = Command::new(
            Kind::Labels,
            json!({"session": {"prism_response_metadata": labels}}),
        );
        self.invoke(c).await.map(|_| ())
    }
    /// Clear input audio and reset its converter after acknowledgment.
    pub async fn clear_audio(&self) -> Result<(), Error> {
        self.invoke(Command::new(Kind::Clear, json!({})))
            .await
            .map(|_| ())
    }
    /// Convert and submit a bounded chunk of complete PCM16 frames.
    /// Dropping this future before submission sends nothing. Dropping it during
    /// socket submission closes the session without replaying the audio.
    pub async fn send_audio(&self, audio: AudioChunk) -> Result<(), Error> {
        audio.format.validate()?;
        if audio.data.len() > 192000
            || !audio
                .data
                .len()
                .is_multiple_of(audio.format.channels as usize * 2)
        {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "audio must have complete PCM16 frames and at most 192000 bytes",
            ));
        }
        let mut c = Command::new(Kind::Audio, json!({}));
        c.audio = Some(audio);
        self.invoke(c).await.map(|_| ())
    }
    /// Report measured playback completion or interruption.
    pub async fn report_playback(&self, report: PlaybackReport) -> Result<(), Error> {
        let c = match report {
            PlaybackReport::Interrupted {
                output,
                audio_end_ms,
            } => {
                let mut c = Command::new(
                    Kind::Truncate,
                    json!({"item_id":output.item_id,"content_index":output.content_index,"audio_end_ms":audio_end_ms}),
                );
                c.reference = Some(output.response.clone());
                c.output = Some(output);
                c
            }
            PlaybackReport::Finished {
                response,
                played_ms,
                audible_tail_ms,
            } => {
                for v in [played_ms, audible_tail_ms].into_iter().flatten() {
                    if !v.is_finite() || v < 0.0 {
                        return Err(Error::new(
                            ErrorKind::Input,
                            "playback times must be finite and nonnegative",
                        ));
                    }
                }
                let mut c = Command::new(
                    Kind::Drain,
                    json!({"response_id":response.response_id,"played_ms":played_ms,"audible_tail_ms":audible_tail_ms}),
                );
                c.reference = Some(response);
                c
            }
        };
        self.invoke(c).await.map(|_| ())
    }
}
fn failure(message: &'static str) -> Error {
    Error::new(ErrorKind::Stream, message)
}

#[path = "dispatch.rs"]
mod dispatch;
#[path = "worker.rs"]
mod worker;
