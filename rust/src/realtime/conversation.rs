use super::{
    AudioChunk, OutputRef, PlaybackReport, RealtimeEvent, RealtimeSession, ResponseRef,
    SessionEvent,
};
use crate::{Error, ErrorKind};
use futures_util::{
    future::{BoxFuture, FusedFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// Application-owned capture or recorded input. Read must settle after cancellation.
pub trait ConversationSource: Send + 'static {
    /// Return a complete PCM chunk, or None at EOF. The application owns the device.
    fn read(
        &mut self,
        stop: CancellationToken,
    ) -> impl Future<Output = Result<Option<AudioChunk>, Error>> + Send;
}
/// One playback output and its independent device receipt.
/// Completion can be pending while write, end, playback interruption, or stop
/// runs. Do not hold a lock across the wait if those operations need that lock.
///
/// A receipt can remain borrowed while the output and playback adapter are used:
///
/// ```no_run
/// use rimelabs_sdk::{AudioChunk, ConversationOutput, ConversationPlayback, Error, OutputRef};
/// use tokio_util::sync::CancellationToken;
///
/// async fn interrupt_after_queueing<P: ConversationPlayback>(
///     playback: &mut P, reference: &OutputRef, audio: AudioChunk,
/// ) -> Result<f64, Error> {
///     let output = playback.open(reference)?;
///     let receipt = output.completion(CancellationToken::new());
///     let queue_and_interrupt = async {
///         output.write(audio)?;
///         output.end()?;
///         playback.interrupt(std::slice::from_ref(reference))
///     };
///     // Poll the receipt first, then feed the output while that wait is pending.
///     let (played, queued) = tokio::join!(biased; receipt, queue_and_interrupt);
///     queued?;
///     played
/// }
/// ```
pub trait ConversationOutput: Send + Sync + 'static {
    /// Queue audio without blocking. Fail explicitly on overflow.
    fn write(&self, chunk: AudioChunk) -> Result<(), Error>;
    /// Seal the output without waiting for its device to drain.
    fn end(&self) -> Result<(), Error>;
    /// Measured played milliseconds, including after interruption. Cancellation
    /// releases this wait without cancelling the device receipt.
    fn completion(
        &self,
        stop: CancellationToken,
    ) -> impl Future<Output = Result<f64, Error>> + Send;
}
/// Application-owned playback. Outputs play in open order and never overlap.
/// Open, interrupt, and stop are serialized, but can overlap pending receipts.
pub trait ConversationPlayback: Send + 'static {
    /// Concrete receipt type for this adapter.
    type Output: ConversationOutput;
    /// Queue an independent output in FIFO order.
    fn open(&mut self, output: &OutputRef) -> Result<Arc<Self::Output>, Error>;
    /// Discard selected outputs as a batch, including sealed outputs, and resume
    /// the remaining FIFO queue. Repeated interruption must be harmless.
    fn interrupt(&mut self, outputs: &[OutputRef]) -> Result<(), Error>;
    /// Confirm all playback has stopped and queued audio is discarded. Keep
    /// caller resources open. Settle cancellation cleanup before returning.
    fn stop(&mut self, stop: CancellationToken) -> impl Future<Output = Result<(), Error>> + Send;
}
type EventCallback = Box<dyn FnMut(&SessionEvent) -> Result<(), Error> + Send>;

/// Coordinates capture, ordered playback, interruption, and playback reports.
/// The application executes tools. The runner owns the session's active lifetime
/// and its event stream. Limits: 64 replies, 128 outputs, 256 pending operations.
pub struct ConversationRunner<S, P> {
    session: RealtimeSession,
    source: S,
    playback: P,
    on_event: Option<EventCallback>,
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel()
    }
}
struct Output<O> {
    reference: OutputRef,
    stream: Arc<O>,
    sealed: bool,
    measured: Option<f64>,
    cut: bool,
}
struct Reply<O> {
    interrupt_on_speech: bool,
    reference: ResponseRef,
    outputs: Vec<Output<O>>,
    ended: bool,
    interrupted: bool,
    reporting: bool,
    finished_report: CancellationToken,
}
impl<O> Reply<O> {
    fn pending_playback(&self) -> bool {
        !self.interrupted && (!self.ended || self.outputs.iter().any(|o| o.measured.is_none()))
    }
}
enum Completion {
    Measured(OutputRef, f64),
    Reported(String),
    Control,
}
fn completion_error(completed: Result<Completion, Error>) -> Option<Error> {
    match completed {
        Err(error) => Some(error),
        Ok(Completion::Measured(_, played)) if !played.is_finite() || played < 0.0 => {
            Some(Error::new(
                ErrorKind::Input,
                "playback measurement must be finite and nonnegative",
            ))
        }
        _ => None,
    }
}
type Tasks = FuturesUnordered<BoxFuture<'static, Result<Completion, Error>>>;
fn launch(
    tasks: &Tasks,
    task: impl Future<Output = Result<Completion, Error>> + Send + 'static,
) -> Result<(), Error> {
    if tasks.len() >= 256 {
        return Err(Error::new(
            ErrorKind::ResourceLimit,
            "conversation pending operation limit exceeded",
        ));
    }
    tasks.push(task.boxed());
    Ok(())
}
fn end<O: ConversationOutput>(o: &mut Output<O>) -> Result<(), Error> {
    if !o.sealed {
        o.sealed = true;
        o.stream.end()?
    }
    Ok(())
}
fn cut<O: ConversationOutput>(
    o: &mut Output<O>,
    session: &RealtimeSession,
    tasks: &Tasks,
) -> Result<(), Error> {
    if o.cut {
        return Ok(());
    }
    if let Some(played) = o.measured {
        if played > u32::MAX as f64 {
            return Err(Error::new(
                ErrorKind::Input,
                "interrupted playback exceeds uint32 milliseconds",
            ));
        }
        o.cut = true;
        let report = PlaybackReport::Interrupted {
            output: o.reference.clone(),
            audio_end_ms: played as u32,
        };
        let session = session.clone();
        launch(tasks, async move {
            match session.report_playback(report).await {
                Err(e)
                    if e.realtime_fault().is_some_and(|f| {
                        f.scope == "event" && f.code == "truncate_not_current"
                    }) => {}
                r => r?,
            }
            Ok(Completion::Control)
        })?
    }
    Ok(())
}
fn blocked(id: &str, parents: &HashMap<String, String>, interrupted: &HashSet<String>) -> bool {
    let mut current = id;
    for _ in 0..128 {
        if interrupted.contains(current) {
            return true;
        }
        match parents.get(current) {
            Some(s) if !s.is_empty() => current = s,
            None | Some(_) => return false,
        }
    }
    false
}

struct PlaybackState<O> {
    replies: Vec<Reply<O>>,
    parents: HashMap<String, String>,
    interrupted: HashSet<String>,
    history: VecDeque<String>,
}
impl<O: ConversationOutput> PlaybackState<O> {
    fn new() -> Self {
        Self {
            replies: vec![],
            parents: HashMap::new(),
            interrupted: HashSet::new(),
            history: VecDeque::new(),
        }
    }
    fn complete(
        &mut self,
        completed: Completion,
        session: &RealtimeSession,
        tasks: &Tasks,
    ) -> Result<(), Error> {
        match completed {
            Completion::Measured(reference, played) => {
                if !played.is_finite() || played < 0.0 {
                    return Err(Error::new(
                        ErrorKind::Input,
                        "playback measurement must be finite and nonnegative",
                    ));
                }
                for reply in &mut self.replies {
                    for output in &mut reply.outputs {
                        if output.reference == reference {
                            output.measured = Some(played);
                            if reply.interrupted {
                                cut(output, session, tasks)?;
                            }
                        }
                    }
                }
            }
            Completion::Reported(id) => self.replies.retain(|r| r.reference.response_id != id),
            Completion::Control => {}
        }
        Ok(())
    }
    fn event<P: ConversationPlayback<Output = O>>(
        &mut self,
        event: &SessionEvent,
        playback: &mut P,
        session: &RealtimeSession,
        tasks: &Tasks,
        receipts: &CancellationToken,
    ) -> Result<(), Error> {
        match &event.payload {
            RealtimeEvent::ResponseStarted {
                response, parent, ..
            } => {
                if self.replies.len() >= 64 {
                    return Err(Error::new(
                        ErrorKind::ResourceLimit,
                        "conversation response limit exceeded",
                    ));
                }
                let id = response.response_id.clone();
                self.parents.insert(
                    id.clone(),
                    parent
                        .as_ref()
                        .map(|p| p.response_id.clone())
                        .unwrap_or_default(),
                );
                self.history.push_back(id.clone());
                if self.history.len() > 128 {
                    let old = self.history.pop_front().expect("self.history");
                    self.parents.remove(&old);
                    self.interrupted.remove(&old);
                }
                let cut = blocked(&id, &self.parents, &self.interrupted);
                if cut {
                    self.interrupted.insert(id);
                    let s = session.clone();
                    let r = response.clone();
                    launch(tasks, async move {
                        s.cancel(&r).await?;
                        Ok(Completion::Control)
                    })?
                }
                self.replies.push(Reply {
                    interrupt_on_speech: session.response_interrupt_on_speech(response)?,
                    reference: response.clone(),
                    outputs: vec![],
                    ended: false,
                    interrupted: cut,
                    reporting: false,
                    finished_report: CancellationToken::new(),
                });
            }
            RealtimeEvent::SpeechStarted { .. } | RealtimeEvent::ResponseAbandoned { .. } => {
                let interrupt_on_speech = self
                    .replies
                    .iter()
                    .rev()
                    .find(|reply| reply.pending_playback())
                    .is_some_and(|reply| reply.interrupt_on_speech);
                let all = matches!(&event.payload, RealtimeEvent::SpeechStarted { .. })
                    && interrupt_on_speech;
                if let RealtimeEvent::ResponseAbandoned { response } = &event.payload {
                    self.interrupted.insert(response.response_id.clone());
                }
                let mut outputs = vec![];
                for reply in &mut self.replies {
                    if !reply.interrupted
                        && ((all && reply.pending_playback())
                            || blocked(
                                &reply.reference.response_id,
                                &self.parents,
                                &self.interrupted,
                            ))
                    {
                        reply.interrupted = true;
                        reply.finished_report.cancel();
                        self.interrupted.insert(reply.reference.response_id.clone());
                        outputs.extend(reply.outputs.iter().map(|o| o.reference.clone()))
                    }
                }
                if !outputs.is_empty() {
                    playback.interrupt(&outputs)?
                }
                for reply in &mut self.replies {
                    if reply.interrupted {
                        for o in &mut reply.outputs {
                            end(o)?;
                            cut(o, session, tasks)?
                        }
                    }
                }
            }
            RealtimeEvent::ResponseEnded { response, .. } => {
                for reply in &mut self.replies {
                    if reply.reference == *response {
                        reply.ended = true;
                        for o in &mut reply.outputs {
                            end(o)?
                        }
                    }
                }
            }
            RealtimeEvent::MessageStarted { output }
            | RealtimeEvent::AudioDelta { output, .. }
            | RealtimeEvent::AudioDone { output } => {
                let count: usize = self.replies.iter().map(|r| r.outputs.len()).sum();
                if let Some(reply) = self
                    .replies
                    .iter_mut()
                    .find(|r| r.reference == output.response && !r.ended)
                {
                    let index = if let Some(i) =
                        reply.outputs.iter().position(|o| o.reference == *output)
                    {
                        i
                    } else {
                        if count >= 128 {
                            return Err(Error::new(
                                ErrorKind::ResourceLimit,
                                "conversation output limit exceeded",
                            ));
                        }
                        let stream = playback.open(output)?;
                        let receipt = stream.clone();
                        let reference = output.clone();
                        let cancellation = receipts.clone();
                        launch(tasks, async move {
                            let played = receipt.completion(cancellation).await?;
                            Ok(Completion::Measured(reference, played))
                        })?;
                        reply.outputs.push(Output {
                            reference: output.clone(),
                            stream,
                            sealed: false,
                            measured: None,
                            cut: false,
                        });
                        if reply.interrupted {
                            playback.interrupt(std::slice::from_ref(output))?;
                            end(reply.outputs.last_mut().expect("new output"))?
                        }
                        reply.outputs.len() - 1
                    };
                    let o = &mut reply.outputs[index];
                    match &event.payload {
                        RealtimeEvent::AudioDelta { audio, .. }
                            if !reply.interrupted && !o.sealed =>
                        {
                            o.stream.write(audio.clone())?
                        }
                        RealtimeEvent::AudioDone { .. } => end(o)?,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl<S: ConversationSource, P: ConversationPlayback> ConversationRunner<S, P> {
    /// Use application-owned source and playback adapters.
    pub fn new(session: RealtimeSession, source: S, playback: P) -> Self {
        Self {
            session,
            source,
            playback,
            on_event: None,
        }
    }
    /// Receive events inline. The callback must not block. Start application tool
    /// work separately and return tool results through a session handle.
    pub fn on_event(
        mut self,
        callback: impl FnMut(&SessionEvent) -> Result<(), Error> + Send + 'static,
    ) -> Self {
        self.on_event = Some(Box::new(callback));
        self
    }
    /// Run until input EOF, closure, error, or cancellation. Dropping this future
    /// requests shutdown; the owned task confirms playback stop and closes the session.
    /// Keep this future and cancel `stop` if the caller needs to await confirmation.
    /// The Tokio runtime must remain alive for cleanup after this future is dropped.
    /// The five-second stop token is cooperative. Adapters that ignore cancellation
    /// can prevent cleanup from finishing; they are not forcibly dropped.
    pub async fn run(self, stop: CancellationToken) -> Result<(), Error> {
        let lifetime = stop.child_token();
        let guard = CancelOnDrop(lifetime.clone());
        let task = tokio::spawn(self.owned(lifetime, stop));
        let result = task
            .await
            .map_err(|_| Error::new(ErrorKind::Stream, "conversation worker failed"))?;
        drop(guard);
        result
    }
    async fn owned(
        mut self,
        stop: CancellationToken,
        caller: CancellationToken,
    ) -> Result<(), Error> {
        let receipts = CancellationToken::new();
        let mut tasks = Tasks::new();
        let input_stop = stop.clone();
        let session = self.session.clone();
        // Input is polled beside events and receipts, so slow capture cannot block them.
        let source = &mut self.source;
        let input = async move {
            loop {
                match source.read(input_stop.clone()).await? {
                    Some(chunk) => tokio::select! {
                        _ = input_stop.cancelled() => return Err(Error::cancelled()),
                        result = session.send_audio(chunk) => result?,
                    },
                    None => return Ok::<(), Error>(()),
                }
            }
        }
        .fuse();
        tokio::pin!(input);
        let events = self.session.events();
        let mut state = PlaybackState::<P::Output>::new();
        let mut result=async {
            let mut events=events?;
            loop {
                if let Some(reply) = state.replies.first_mut().filter(|reply| {
                    reply.ended && !reply.reporting && reply.outputs.iter().all(|o| o.measured.is_some())
                }) {
                    reply.reporting = true;
                    let session = self.session.clone();
                    let response = reply.reference.clone();
                    let interrupted = reply.interrupted;
                    let cancelled = reply.finished_report.clone();
                    let played = reply.outputs.iter().map(|o| o.measured.unwrap_or_default()).sum();
                    launch(&tasks, async move {
                        if !interrupted {
                            tokio::select! {
                                biased;
                                _ = cancelled.cancelled() => {},
                                result = session.report_playback(PlaybackReport::Finished {
                                    response: response.clone(), played_ms: Some(played), audible_tail_ms: None,
                                }) => result?,
                            }
                        }
                        Ok(Completion::Reported(response.response_id))
                    })?;
                }
                tokio::select! {
                    _=stop.cancelled()=>return Err(Error::new(ErrorKind::Cancelled,"conversation cancelled")),
                    r=&mut input=>return r,
                    completed = tasks.next(), if !tasks.is_empty() => {
                        state.complete(completed.expect("pending task")?, &self.session, &tasks)?;
                    },
                    event=events.next()=>{
                        let Some(event)=event else{return Ok(())};let event=event?;
                        state.event(&event, &mut self.playback, &self.session, &tasks, &receipts)?;
                        if let Some(callback)=&mut self.on_event{callback(&event)?}
                    }
                }
            }
        }.await;
        // Poll ready receipts before cancelling them so a failure cannot be lost to EOF.
        while let Some(Some(completed)) = tasks.next().now_or_never() {
            if let Some(error) = completion_error(completed) {
                if result.is_ok() && !error.is_realtime_closed() {
                    result = Err(error);
                }
            }
        }
        if result.as_ref().is_err_and(Error::is_realtime_closed) {
            result = Ok(());
        }
        stop.cancel();
        let shutdown = CancellationToken::new();
        let shutdown_timer = shutdown.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            shutdown_timer.cancel();
        });
        let release_input = async {
            if !input.is_terminated() {
                input.await
            } else {
                Ok(())
            }
        };
        let (input_result, mut cleanup) =
            tokio::join!(release_input, self.playback.stop(shutdown.clone()));
        if shutdown.is_cancelled() {
            cleanup = Err(Error::new(
                ErrorKind::Timeout,
                "playback shutdown confirmation timed out",
            ));
        }
        timer.abort();
        let _ = timer.await;
        if let Err(error) = input_result {
            if result.is_ok() && error.kind() != ErrorKind::Cancelled && !error.is_realtime_closed()
            {
                result = Err(error);
            }
        }
        while let Some(Some(completed)) = tasks.next().now_or_never() {
            if let Some(error) = completion_error(completed) {
                if result.is_ok() && !error.is_realtime_closed() {
                    result = Err(error);
                }
            }
        }
        receipts.cancel();
        self.session.close().await;
        while let Some(completed) = tasks.next().await {
            if let Some(error) = completion_error(completed) {
                if result.is_ok()
                    && error.kind() != ErrorKind::Cancelled
                    && !error.is_realtime_closed()
                {
                    result = Err(error);
                }
            }
        }
        if result.is_ok() && caller.is_cancelled() {
            result = Err(Error::cancelled());
        }
        result.and(cleanup)
    }
}

#[cfg(test)]
pub(super) fn assert_measured_playback_is_not_interrupted<P: ConversationPlayback>(
    session: &RealtimeSession,
    playback: &mut P,
    newer_reply: bool,
) {
    let mut state = PlaybackState::new();
    for (id, measured) in [("drained", Some(10.0)), ("playing", None)] {
        if measured.is_none() && !newer_reply {
            break;
        }
        let response = ResponseRef {
            session_id: session.info().session_id,
            response_id: id.into(),
        };
        let reference = OutputRef {
            response: response.clone(),
            item_id: format!("{id}-output"),
            output_index: 0,
            content_index: 0,
        };
        state.replies.push(Reply {
            reference: response,
            interrupt_on_speech: true,
            outputs: vec![Output {
                stream: playback.open(&reference).unwrap(),
                reference,
                sealed: measured.is_some(),
                measured,
                cut: false,
            }],
            ended: measured.is_some(),
            interrupted: false,
            reporting: false,
            finished_report: CancellationToken::new(),
        });
    }
    let event = SessionEvent {
        session_id: session.info().session_id,
        event_id: String::new(),
        request_id: None,
        payload: RealtimeEvent::SpeechStarted {
            item_id: "speech".into(),
            audio_start_ms: 0,
        },
    };
    let tasks = Tasks::new();
    state
        .event(&event, playback, session, &tasks, &CancellationToken::new())
        .unwrap();
    let drained = &state.replies[0];
    assert!(!drained.interrupted);
    assert!(!drained.finished_report.is_cancelled());
    assert!(!drained.outputs[0].cut);
    assert!(
        tasks.is_empty(),
        "measured output must not get a cut report"
    );
    if newer_reply {
        let playing = &state.replies[1];
        assert!(playing.interrupted);
        assert!(playing.finished_report.is_cancelled());
    }
}
