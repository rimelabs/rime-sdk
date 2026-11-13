use crate::PcmFormat;
use bytes::Bytes;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

/// Your labels for replies, keys to strings, such as `{"step": "3"}`. Prism
/// takes at most 16, keys up to 64 characters without the `prism_` prefix and
/// values up to 512.
pub type Labels = BTreeMap<String, String>;

pub(super) const SPEED_LIMITS: std::ops::RangeInclusive<f64> = 0.85..=1.3;
pub(super) const INTERRUPT_WORD_LIMITS: std::ops::RangeInclusive<u64> = 1..=10;
pub(super) const MIN_SILENCE_LIMITS: std::ops::RangeInclusive<u64> = 0..=5000;
pub(super) const MAX_PHRASES: usize = 100;

/// Independent Prism connection, admission, and acknowledgment timeouts.
#[derive(Clone, Copy, Debug)]
pub struct RealtimeTimeouts {
    /// Handshake and session creation budget. Defaults to ten seconds.
    pub connect: Duration,
    /// Turn admission budget. Defaults to thirty seconds.
    pub ready: Duration,
    /// Write and acknowledgment budget. Defaults to ten seconds.
    pub request: Duration,
}
impl Default for RealtimeTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            ready: Duration::from_secs(30),
            request: Duration::from_secs(10),
        }
    }
}
/// Turn detection timing. Omitted fields keep the server default.
#[derive(Clone, Debug, Default)]
pub struct TurnDetection {
    /// Low, medium, high, or auto.
    pub eagerness: Option<String>,
    /// Maximum silence, from 1000 through 10000 milliseconds.
    pub end_of_turn_timeout_ms: Option<u32>,
    /// Minimum silence before a turn may end. Fixed when the session starts.
    pub end_of_turn_min_silence_ms: Option<u32>,
    /// Words needed to interrupt an active reply.
    pub interrupt_min_words: Option<u32>,
    /// Phrases that interrupt without the word threshold. Empty clears the list.
    pub interrupt_phrases: Option<Vec<String>>,
    /// Phrases that do not interrupt. Empty clears the list.
    pub acknowledgment_phrases: Option<Vec<String>>,
}
/// Mutable turn settings. Omitted fields keep their values.
#[derive(Clone, Debug, Default)]
pub struct TurnDetectionUpdate {
    /// Words needed to interrupt an active reply.
    pub interrupt_min_words: Option<u32>,
    /// Phrases that interrupt without the word threshold.
    pub interrupt_phrases: Option<Vec<String>>,
    /// Phrases that do not interrupt.
    pub acknowledgment_phrases: Option<Vec<String>>,
}
/// One provisional speech chunk. Its response need not have started yet.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReviewRequest {
    /// Review identifier, unique in this session.
    pub review_id: String,
    /// Raw response identifier, including provisional replies.
    pub response_id: String,
    /// Message identifier.
    pub item_id: String,
    /// Zero-based chunk index within the reply.
    pub index: u64,
    /// Proposed speech, not proof that the text was spoken.
    pub text: String,
}
impl ReviewRequest {
    /// Build a request, for example to test a review callback.
    pub fn new(
        review_id: impl Into<String>,
        response_id: impl Into<String>,
        item_id: impl Into<String>,
        index: u64,
        text: impl Into<String>,
    ) -> Self {
        Self {
            review_id: review_id.into(),
            response_id: response_id.into(),
            item_id: item_id.into(),
            index,
            text: text.into(),
        }
    }
}
/// A decision on one proposed chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReviewDecision {
    /// Speak the proposed text.
    Allow,
    /// Speak this text and end the rest of the reply.
    Replace(String),
    /// Do not speak this chunk or later chunks.
    Stop,
}
/// Server action when a review has no answer in time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReviewTimeout {
    /// Stop the reply.
    Stop,
    /// Speak the proposed text.
    Speak,
    /// Speak this text and end the rest of the reply.
    Say(String),
}
/// Review settings confirmed by the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewSettings {
    /// Server budget for each chunk, in milliseconds.
    pub timeout_ms: u32,
    /// Action when no result arrives in time.
    pub on_timeout: ReviewTimeout,
}
type ReviewFuture = Pin<Box<dyn Future<Output = Result<ReviewDecision, crate::Error>> + Send>>;
/// Session-owned asynchronous review. Callbacks must yield and support cancellation.
/// Up to 128 reviews run concurrently, including provisional replies. A callback
/// error sends Stop when possible and then fails the session. Server timeout
/// policy controls expired reviews; the SDK never retries a result.
#[derive(Clone)]
pub struct Review {
    pub(crate) callback: Arc<dyn Fn(ReviewRequest) -> ReviewFuture + Send + Sync>,
    pub(crate) settings: ReviewSettings,
}
impl fmt::Debug for Review {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Review")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}
impl Review {
    /// Register a callback. Defaults to a 1000 ms budget and Stop on timeout.
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(ReviewRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ReviewDecision, crate::Error>> + Send + 'static,
    {
        Self {
            callback: Arc::new(move |request| Box::pin(callback(request))),
            settings: ReviewSettings {
                timeout_ms: 1000,
                on_timeout: ReviewTimeout::Stop,
            },
        }
    }
    /// Set the server budget, from 100 through 5000 ms.
    pub fn timeout_ms(mut self, value: u32) -> Self {
        self.settings.timeout_ms = value;
        self
    }
    /// Set the action the server takes on timeout.
    pub fn on_timeout(mut self, value: ReviewTimeout) -> Self {
        self.settings.on_timeout = value;
        self
    }
}
/// Speech to use while an application tool runs.
#[derive(Clone, Debug)]
pub enum PrismFiller {
    /// No filler speech.
    None,
    /// Fixed text, at most 500 Unicode scalar values.
    Text(String),
    /// Generated speech with optional instructions, at most 500 scalar values.
    Auto(Option<String>),
}
/// An application-owned function. The SDK never executes tools.
#[derive(Clone, Debug)]
pub struct ToolDefinition {
    /// Function name.
    pub name: String,
    /// Function description.
    pub description: String,
    /// JSON schema for function arguments.
    pub parameters: Map<String, Value>,
    /// Optional speech while the function runs.
    pub prism_filler: Option<PrismFiller>,
}
/// History policy when the context fills. Omit it to keep the server default.
#[derive(Clone, Debug, PartialEq)]
pub enum Truncation {
    /// Drop oldest turns, retaining 0.8 of available history capacity.
    Auto,
    /// Keep history; an oversized prompt fails with context_length_exceeded.
    Disabled,
    /// Retain this fraction of available history capacity, above zero and at most one.
    RetentionRatio(f64),
}
/// Prism settings, independent of TTS configuration.
#[derive(Clone, Debug)]
pub struct RealtimeConnectOptions {
    pub(crate) endpoint: String,
    pub(crate) voice: Option<String>,
    pub(crate) instructions: Option<String>,
    pub(crate) tools: Vec<ToolDefinition>,
    pub(crate) interrupt: bool,
    pub(crate) detection: TurnDetection,
    pub(crate) client_ends_turns: bool,
    pub(crate) speed: Option<f64>,
    pub(crate) review: Option<Review>,
    pub(crate) truncation: Option<Truncation>,
    pub(crate) timeouts: RealtimeTimeouts,
    pub(crate) request_id: Option<String>,
    pub(crate) response_metadata: Option<Labels>,
}
impl Default for RealtimeConnectOptions {
    fn default() -> Self {
        Self {
            endpoint: "wss://prism.api.rime.ai/v1/realtime".into(),
            voice: None,
            instructions: None,
            tools: vec![],
            interrupt: true,
            detection: TurnDetection::default(),
            client_ends_turns: false,
            speed: None,
            review: None,
            truncation: None,
            timeouts: RealtimeTimeouts::default(),
            request_id: None,
            response_metadata: None,
        }
    }
}
impl RealtimeConnectOptions {
    /// Set the speech speed multiplier, from 0.85 through 1.3. Omission keeps the server default.
    pub fn speed(mut self, value: f64) -> Self {
        self.speed = Some(value);
        self
    }
    /// Review each spoken chunk before Prism speaks it. Fixed at connection.
    pub fn review(mut self, value: Review) -> Self {
        self.review = Some(value);
        self
    }
    /// Set history truncation, also changeable during a call.
    pub fn truncation(mut self, value: Truncation) -> Self {
        self.truncation = Some(value);
        self
    }
    /// Override the Prism endpoint. Plain ws is restricted to localhost.
    pub fn endpoint(mut self, value: impl Into<String>) -> Self {
        self.endpoint = value.into();
        self
    }
    /// Select a Prism voice.
    pub fn voice(mut self, value: impl Into<String>) -> Self {
        self.voice = Some(value.into());
        self
    }
    /// Set session instructions.
    pub fn instructions(mut self, value: impl Into<String>) -> Self {
        self.instructions = Some(value.into());
        self
    }
    /// Register application-owned functions.
    pub fn tools(mut self, value: Vec<ToolDefinition>) -> Self {
        self.tools = value;
        self
    }
    /// Interrupt playback on detected speech. Defaults to true.
    pub fn interrupt_on_speech(mut self, value: bool) -> Self {
        self.interrupt = value;
        self
    }
    /// Set speech endpoint timing.
    pub fn turn_detection(mut self, value: TurnDetection) -> Self {
        self.detection = value;
        self
    }
    /// Prism detects no turns: the session starts and ends each one with
    /// `start_turn` and `end_turn`, and asks for replies with `reply_to_turns`.
    /// Turn detection settings and `interrupt_on_speech(false)` do not apply
    /// then. This cannot change later in the session.
    pub fn client_ends_turns(mut self, value: bool) -> Self {
        self.client_ends_turns = value;
        self
    }
    /// Set positive operation budgets.
    pub fn timeouts(mut self, value: RealtimeTimeouts) -> Self {
        self.timeouts = value;
        self
    }
    /// Send the upgrade's x-request-id. It must be nonblank printable ASCII, or `connect`
    /// returns an input error. Prism replaces IDs longer than 128 characters.
    /// A gateway can replace shorter IDs too. `info().request_id` reports the ID
    /// Prism actually used.
    pub fn request_id(mut self, value: impl Into<String>) -> Self {
        self.request_id = Some(value.into());
        self
    }
    /// Label every reply. Change the labels with `set_response_metadata`.
    pub fn response_metadata(mut self, value: Labels) -> Self {
        self.response_metadata = Some(value);
        self
    }
}
/// Partial settings for an existing session. Omitted fields keep their values.
#[derive(Clone, Debug, Default)]
pub struct RealtimeUpdateOptions {
    pub(crate) speed: Option<f64>,
    pub(crate) detection: Option<TurnDetectionUpdate>,
    pub(crate) instructions: Option<String>,
    pub(crate) tools: Option<Vec<ToolDefinition>>,
    pub(crate) voice: Option<String>,
    pub(crate) interrupt: Option<bool>,
    pub(crate) truncation: Option<Truncation>,
}
impl RealtimeUpdateOptions {
    /// Set the speech speed for subsequent replies, from 0.85 through 1.3.
    pub fn speed(mut self, value: f64) -> Self {
        self.speed = Some(value);
        self
    }
    /// Change interrupt settings for subsequent replies.
    pub fn turn_detection(mut self, value: TurnDetectionUpdate) -> Self {
        self.detection = Some(value);
        self
    }
    /// Set history truncation, also changeable during a call.
    pub fn truncation(mut self, value: Truncation) -> Self {
        self.truncation = Some(value);
        self
    }
    /// Replace instructions. An empty string clears them.
    pub fn instructions(mut self, value: impl Into<String>) -> Self {
        self.instructions = Some(value.into());
        self
    }
    /// Replace the complete tool list. An empty list removes all tools.
    pub fn tools(mut self, value: Vec<ToolDefinition>) -> Self {
        self.tools = Some(value);
        self
    }
    /// Change voice. An empty string selects the deployment default.
    pub fn voice(mut self, value: impl Into<String>) -> Self {
        self.voice = Some(value.into());
        self
    }
    /// Set whether user speech interrupts subsequent replies.
    pub fn interrupt_on_speech(mut self, value: bool) -> Self {
        self.interrupt = Some(value);
        self
    }
}
/// Proactive reply options.
#[derive(Clone, Debug, Default)]
pub struct ReplyOptions {
    pub(crate) instruction: Option<String>,
    pub(crate) tool_call: Option<ToolCallRef>,
    pub(crate) metadata: Labels,
}
impl ReplyOptions {
    /// Set instructions for this reply.
    pub fn instruction(mut self, value: impl Into<String>) -> Self {
        self.instruction = Some(value.into());
        self
    }
    /// Request a report about an already recorded tool result.
    pub fn tool_call(mut self, value: ToolCallRef) -> Self {
        self.tool_call = Some(value);
        self
    }
    /// Label only this reply; request labels override matching session labels.
    pub fn metadata(mut self, value: Labels) -> Self {
        self.metadata = value;
        self
    }
}
/// Options for a typed reply or tool continuation.
#[derive(Clone, Debug, Default)]
pub struct ReplyRequestOptions {
    pub(crate) metadata: Labels,
}
impl ReplyRequestOptions {
    /// Label only this reply; request labels override matching session labels.
    pub fn metadata(mut self, value: Labels) -> Self {
        self.metadata = value;
        self
    }
}
/// Complete signed little-endian PCM16 frames, at most 192000 bytes.
#[derive(Clone, Debug)]
pub struct AudioChunk {
    /// Raw PCM data without headers.
    pub data: Bytes,
    /// Source sample rate and channel count. Output is 24 kHz mono.
    pub format: PcmFormat,
}
/// A retained response in one session. Fields cannot be forged by callers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResponseRef {
    pub(crate) session_id: String,
    pub(crate) response_id: String,
}
impl ResponseRef {
    /// Owning session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    /// Service response ID.
    pub fn response_id(&self) -> &str {
        &self.response_id
    }
}
/// A history message in one session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemRef {
    pub(crate) session_id: String,
    pub(crate) item_id: String,
}
impl ItemRef {
    /// Owning session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    /// Service item ID.
    pub fn item_id(&self) -> &str {
        &self.item_id
    }
}
/// A function call in one response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCallRef {
    pub(crate) response: ResponseRef,
    pub(crate) call_id: String,
}
impl ToolCallRef {
    /// Owning response.
    pub fn response(&self) -> &ResponseRef {
        &self.response
    }
    /// Service function call ID.
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
}
/// One assistant output's audio content.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OutputRef {
    pub(crate) response: ResponseRef,
    pub(crate) item_id: String,
    pub(crate) output_index: u64,
    pub(crate) content_index: u64,
}
impl OutputRef {
    /// Owning response.
    pub fn response(&self) -> &ResponseRef {
        &self.response
    }
    /// Service message ID.
    pub fn item_id(&self) -> &str {
        &self.item_id
    }
    /// Position in the response.
    pub fn output_index(&self) -> u64 {
        self.output_index
    }
    /// Content position. Prism currently requires zero.
    pub fn content_index(&self) -> u64 {
        self.content_index
    }
}
/// Effective settings supplied by Prism. None means the field was not published.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SessionInfo {
    /// Opaque server release or commit, absent on older images.
    pub version: Option<String>,
    /// Applied speech speed multiplier.
    pub speed: Option<f64>,
    /// Applied review settings, absent when unavailable or disabled.
    pub review: Option<ReviewSettings>,
    /// Applied minimum silence, fixed at session setup.
    pub end_of_turn_min_silence_ms: Option<u64>,
    /// Applied interruption word threshold.
    pub interrupt_min_words: Option<u64>,
    /// Applied interrupt phrases. None differs from an empty list.
    pub interrupt_phrases: Option<Vec<String>>,
    /// Applied acknowledgment phrases. None differs from an empty list.
    pub acknowledgment_phrases: Option<Vec<String>>,
    /// History policy, absent when an older server does not publish it.
    pub truncation: Option<Truncation>,
    /// Settings revision published by the server, if available.
    pub settings_revision: Option<u64>,
    /// Model name, always prism.
    pub model: &'static str,
    /// Session ID.
    pub session_id: String,
    /// Selected voice.
    pub voice: String,
    /// Effective interruption setting.
    pub interrupt_on_speech: bool,
    /// Tool result budget in seconds.
    pub tool_result_timeout_s: Option<f64>,
    /// Tool continuation budget in seconds.
    pub tool_continuation_timeout_s: Option<f64>,
    /// Effective eagerness. Future values become unknown.
    pub eagerness: Option<String>,
    /// Effective maximum silence.
    pub end_of_turn_timeout_ms: Option<u64>,
    /// The x-request-id Prism's logs name the session by.
    pub request_id: Option<String>,
    /// The labels replies carry, as Prism last confirmed them.
    pub response_metadata: Option<Labels>,
    /// True when the client starts and ends each user turn; Prism then detects none.
    pub client_ends_turns: bool,
}
impl SessionInfo {
    /// Build session settings with no published values, for example in tests.
    ///
    /// Interruption is on and the voice is empty. Set other public fields after
    /// construction.
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            version: None,
            speed: None,
            review: None,
            end_of_turn_min_silence_ms: None,
            interrupt_min_words: None,
            interrupt_phrases: None,
            acknowledgment_phrases: None,
            truncation: None,
            settings_revision: None,
            model: "prism",
            session_id: session_id.into(),
            voice: String::new(),
            interrupt_on_speech: true,
            tool_result_timeout_s: None,
            tool_continuation_timeout_s: None,
            eagerness: None,
            end_of_turn_timeout_ms: None,
            request_id: None,
            response_metadata: None,
            client_ends_turns: false,
        }
    }
}
/// Structured service error, available from Error::realtime_fault.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RealtimeFault {
    /// Service error code.
    pub code: String,
    /// Service explanation.
    pub message: String,
    /// Event, utterance, response, tool_roundtrip, review, session, or unknown.
    pub scope: String,
    /// Owning request ID.
    pub request_id: Option<String>,
    /// Owning response ID.
    pub response_id: Option<String>,
    /// Reviewed chunk identifier; absent for faults without review ownership.
    pub review_id: Option<String>,
    /// Owning item ID.
    pub item_id: Option<String>,
    /// Owning function calls.
    pub call_ids: Vec<String>,
    /// Diagnostic correlation ID.
    pub correlation_id: Option<String>,
    /// Invalid parameter, when supplied.
    pub parameter: Option<String>,
}
impl RealtimeFault {
    /// Build a fault with an unknown scope and no owners, for example in tests.
    /// Set other public fields after construction.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            scope: "unknown".into(),
            request_id: None,
            response_id: None,
            review_id: None,
            item_id: None,
            call_ids: Vec::new(),
            correlation_id: None,
            parameter: None,
        }
    }
    /// Whether the same request, unchanged, may succeed on another attempt.
    ///
    /// Only `capacity_exceeded`, `unavailable`, and `response_timeout` return true.
    /// Unknown codes return false. The application owns retry timing, limits,
    /// and recovery after partial output; this method does not replay a request.
    pub fn retryable(&self) -> bool {
        matches!(
            self.code.as_str(),
            "capacity_exceeded" | "unavailable" | "response_timeout"
        )
    }
}
impl fmt::Display for RealtimeFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for RealtimeFault {}
/// Prism's idle limit when it closes a conversation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RealtimeIdleTimeout {
    /// Seconds of inactivity allowed by the server.
    pub idle_timeout_s: f64,
}
impl RealtimeIdleTimeout {
    /// Build an idle timeout, for example in tests.
    pub fn new(idle_timeout_s: f64) -> Self {
        Self { idle_timeout_s }
    }
}
impl fmt::Display for RealtimeIdleTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "realtime session idle for {} seconds",
            self.idle_timeout_s
        )
    }
}
impl std::error::Error for RealtimeIdleTimeout {}
/// One event with protocol correlation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SessionEvent {
    /// Server send time as Unix milliseconds. Absent for old images and local events.
    pub server_time_ms: Option<u64>,
    /// Owning session.
    pub session_id: String,
    /// Server event identifier, if supplied.
    pub event_id: String,
    /// Echoed client request identifier.
    pub request_id: Option<String>,
    /// Typed event contents.
    pub payload: RealtimeEvent,
}
impl SessionEvent {
    /// Build an event without a request ID or server time, for example in tests.
    /// Set those public fields after construction.
    pub fn new(
        session_id: impl Into<String>,
        event_id: impl Into<String>,
        payload: RealtimeEvent,
    ) -> Self {
        Self {
            server_time_ms: None,
            session_id: session_id.into(),
            event_id: event_id.into(),
            request_id: None,
            payload,
        }
    }
}
/// History items excluded from a reply's prompt, not deleted from history.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResponseTruncation {
    /// Number of excluded items, not turns. Already hidden items are not counted.
    pub items_left_out: u64,
    /// Oldest retained item; None when no earlier item was retained.
    pub first_kept_item_id: Option<String>,
}
impl ResponseTruncation {
    /// Build a summary with no retained item, for example in tests.
    /// Set `first_kept_item_id` after construction.
    pub fn new(items_left_out: u64) -> Self {
        Self {
            items_left_out,
            first_kept_item_id: None,
        }
    }
}
/// Model tokens counted for a reply.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResponseUsage {
    /// Tokens in the prompt, including speech.
    pub input_tokens: u64,
    /// Tokens generated by the model.
    pub output_tokens: u64,
    /// Input plus output tokens.
    pub total_tokens: u64,
    /// Maximum shared context size.
    pub context_window: u64,
}
impl ResponseUsage {
    /// Build token counts, for example in tests.
    pub fn new(
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
        context_window: u64,
    ) -> Self {
        Self {
            input_tokens,
            output_tokens,
            total_tokens,
            context_window,
        }
    }
}
/// Typed Prism event contents. Unknown wire events are ignored.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RealtimeEvent {
    /// A playback report was recorded, with the text retained in history.
    #[non_exhaustive]
    ItemTruncated {
        /// Conversation item identifier. No retained response reference is needed.
        item_id: String,
        /// Content part reported by the server.
        content_index: u64,
        /// Reported playback time in milliseconds.
        audio_end_ms: u64,
        /// None when unavailable, including during generation; empty when no words
        /// remain. Tool-call replies retain all words, which can differ from audible text.
        kept_text: Option<String>,
    },
    /// A typed or proactive turn can be submitted.
    InputReady,
    /// Possible speech; applications can lower playback volume.
    #[non_exhaustive]
    SpeechHint {
        /// Audio start time.
        audio_start_ms: u64,
    },
    /// Confirmed user speech.
    #[non_exhaustive]
    SpeechStarted {
        /// Input item.
        item_id: String,
        /// Audio start time.
        audio_start_ms: u64,
    },
    /// End of a user speech turn.
    #[non_exhaustive]
    SpeechStopped {
        /// Input item.
        item_id: String,
        /// Audio end time.
        audio_end_ms: u64,
        /// Semantic, timeout, silence, max_duration, detector_changed,
        /// client_commit (the client's `end_turn`), or unknown.
        end_reason: Option<String>,
        /// Minimum silence in force when the turn ended, absent on older images.
        /// With `eagerness` `auto` and no configured minimum, Prism adjusts it
        /// during the call.
        end_of_turn_min_silence_ms: Option<u64>,
    },
    /// Input audio committed.
    #[non_exhaustive]
    InputCommitted {
        /// Input item.
        item_id: String,
    },
    /// Incremental input transcription.
    #[non_exhaustive]
    TranscriptDelta {
        /// All words so far, when supplied. Replace captions even when this is empty.
        transcript: Option<String>,
        /// Input item.
        item_id: String,
        /// Text to append.
        delta: String,
    },
    /// Final input transcription.
    #[non_exhaustive]
    TranscriptFinal {
        /// Input item.
        item_id: String,
        /// Complete text.
        text: String,
    },
    /// Input transcription failed.
    #[non_exhaustive]
    TranscriptFailed {
        /// Input item.
        item_id: String,
        /// Service fault.
        fault: RealtimeFault,
    },
    /// A new response was accepted.
    #[non_exhaustive]
    ResponseStarted {
        /// Settings revision the reply began with, if published.
        settings_revision: Option<u64>,
        /// New response.
        response: ResponseRef,
        /// Speech, text, tools, proactive, or unknown.
        cause: String,
        /// Parent tool response.
        parent: Option<ResponseRef>,
        /// Typed input history item.
        input_item_id: Option<String>,
        /// The session's labels when the reply began.
        metadata: Labels,
        /// The user items the reply answers; empty when Prism names none.
        input_item_ids: Vec<String>,
    },
    /// A nudge instruction was saved for later replies.
    #[non_exhaustive]
    ResponseInstructionSaved {
        /// Response that used the instruction.
        response: ResponseRef,
        /// Saved item, removable with delete_item after the response ends.
        item_id: String,
    },
    /// A caller cancelled a submitted reply request.
    #[non_exhaustive]
    ResponseAbandoned {
        /// Accepted response to discard.
        response: ResponseRef,
    },
    /// Assistant output began.
    #[non_exhaustive]
    MessageStarted {
        /// Output reference.
        output: OutputRef,
    },
    /// Incremental assistant text.
    #[non_exhaustive]
    TextDelta {
        /// Output reference.
        output: OutputRef,
        /// Text to append.
        delta: String,
    },
    /// Complete assistant text.
    #[non_exhaustive]
    TextDone {
        /// Output reference.
        output: OutputRef,
        /// Complete text.
        text: String,
    },
    /// Assistant PCM audio.
    #[non_exhaustive]
    AudioDelta {
        /// Output reference.
        output: OutputRef,
        /// Mono PCM16 at 24 kHz.
        audio: AudioChunk,
    },
    /// No more audio for this output.
    #[non_exhaustive]
    AudioDone {
        /// Output reference.
        output: OutputRef,
    },
    /// An application must execute a function.
    #[non_exhaustive]
    ToolCall {
        /// Function reference.
        call: ToolCallRef,
        /// Function item.
        item_id: String,
        /// Function name.
        name: String,
        /// Parsed JSON object.
        arguments: Map<String, Value>,
    },
    /// A response finished generating. Audio can still be playing.
    #[non_exhaustive]
    ResponseEnded {
        /// Settings revision the reply began with, if published.
        settings_revision: Option<u64>,
        /// None when the server reports no exclusions or the information is unavailable.
        truncation: Option<ResponseTruncation>,
        /// Response reference.
        response: ResponseRef,
        /// Completed, cancelled, failed, or unknown.
        status: String,
        /// Why it ended, as Prism sent it, for example `stop`, `max_output_tokens`,
        /// `audio_cut`, or `review_stopped`. Prism may add values.
        reason: Option<String>,
    },
    /// Prism counted model tokens for a completed reply.
    #[non_exhaustive]
    ResponseUsageRecorded {
        /// Response whose tokens were counted.
        response: ResponseRef,
        /// Input, output, and total tokens with the context window.
        usage: ResponseUsage,
    },
    /// A nonterminal service error.
    Fault(RealtimeFault),
}
/// Constructors for events an application can build, for example in tests.
///
/// Events that hold response, output, or tool references come only from a
/// session. Optional fields start as `None`; set them with the `with_` methods.
impl RealtimeEvent {
    /// A recorded playback report.
    pub fn item_truncated(
        item_id: impl Into<String>,
        content_index: u64,
        audio_end_ms: u64,
    ) -> Self {
        Self::ItemTruncated {
            item_id: item_id.into(),
            content_index,
            audio_end_ms,
            kept_text: None,
        }
    }
    /// Possible speech.
    pub fn speech_hint(audio_start_ms: u64) -> Self {
        Self::SpeechHint { audio_start_ms }
    }
    /// Confirmed user speech.
    pub fn speech_started(item_id: impl Into<String>, audio_start_ms: u64) -> Self {
        Self::SpeechStarted {
            item_id: item_id.into(),
            audio_start_ms,
        }
    }
    /// The end of a user speech turn.
    pub fn speech_stopped(item_id: impl Into<String>, audio_end_ms: u64) -> Self {
        Self::SpeechStopped {
            item_id: item_id.into(),
            audio_end_ms,
            end_reason: None,
            end_of_turn_min_silence_ms: None,
        }
    }
    /// Committed input audio.
    pub fn input_committed(item_id: impl Into<String>) -> Self {
        Self::InputCommitted {
            item_id: item_id.into(),
        }
    }
    /// Incremental input transcription.
    pub fn transcript_delta(item_id: impl Into<String>, delta: impl Into<String>) -> Self {
        Self::TranscriptDelta {
            transcript: None,
            item_id: item_id.into(),
            delta: delta.into(),
        }
    }
    /// Final input transcription.
    pub fn transcript_final(item_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self::TranscriptFinal {
            item_id: item_id.into(),
            text: text.into(),
        }
    }
    /// Failed input transcription.
    pub fn transcript_failed(item_id: impl Into<String>, fault: RealtimeFault) -> Self {
        Self::TranscriptFailed {
            item_id: item_id.into(),
            fault,
        }
    }
    /// Set the text a playback report kept.
    ///
    /// # Panics
    ///
    /// Panics unless the event is `ItemTruncated`.
    #[must_use]
    pub fn with_kept_text(mut self, text: impl Into<String>) -> Self {
        match &mut self {
            Self::ItemTruncated { kept_text, .. } => *kept_text = Some(text.into()),
            _ => panic!("with_kept_text applies to ItemTruncated"),
        }
        self
    }
    /// Set what ended a speech turn.
    ///
    /// # Panics
    ///
    /// Panics unless the event is `SpeechStopped`.
    #[must_use]
    pub fn with_end_reason(mut self, reason: impl Into<String>) -> Self {
        match &mut self {
            Self::SpeechStopped { end_reason, .. } => *end_reason = Some(reason.into()),
            _ => panic!("with_end_reason applies to SpeechStopped"),
        }
        self
    }
    /// Set the minimum silence in force when a speech turn ended.
    ///
    /// # Panics
    ///
    /// Panics unless the event is `SpeechStopped`.
    #[must_use]
    pub fn with_end_of_turn_min_silence_ms(mut self, silence_ms: u64) -> Self {
        match &mut self {
            Self::SpeechStopped {
                end_of_turn_min_silence_ms,
                ..
            } => *end_of_turn_min_silence_ms = Some(silence_ms),
            _ => panic!("with_end_of_turn_min_silence_ms applies to SpeechStopped"),
        }
        self
    }
    /// Set all transcribed words so far.
    ///
    /// # Panics
    ///
    /// Panics unless the event is `TranscriptDelta`.
    #[must_use]
    pub fn with_transcript(mut self, text: impl Into<String>) -> Self {
        match &mut self {
            Self::TranscriptDelta { transcript, .. } => *transcript = Some(text.into()),
            _ => panic!("with_transcript applies to TranscriptDelta"),
        }
        self
    }
}
/// Device playback measurements.
#[derive(Clone, Debug)]
pub enum PlaybackReport {
    /// Discard the unplayed portion of a message.
    Interrupted {
        /// A known output.
        output: OutputRef,
        /// Measured played time.
        audio_end_ms: u32,
    },
    /// Confirm that a response has fully drained from the device.
    Finished {
        /// Response reference.
        response: ResponseRef,
        /// Measured total played time.
        played_ms: Option<f64>,
        /// Measured audible tail.
        audible_tail_ms: Option<f64>,
    },
}
