use crate::PcmFormat;
use bytes::Bytes;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt, time::Duration};

/// Your labels for replies, keys to strings, such as `{"step": "3"}`. Prism
/// takes at most 16, keys up to 64 characters without the `prism_` prefix and
/// values up to 512.
pub type Labels = BTreeMap<String, String>;

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
/// Prism settings, independent of TTS configuration.
#[derive(Clone, Debug)]
pub struct RealtimeConnectOptions {
    pub(crate) endpoint: String,
    pub(crate) voice: Option<String>,
    pub(crate) instructions: Option<String>,
    pub(crate) tools: Vec<ToolDefinition>,
    pub(crate) interrupt: bool,
    pub(crate) detection: TurnDetection,
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
            timeouts: RealtimeTimeouts::default(),
            request_id: None,
            response_metadata: None,
        }
    }
}
impl RealtimeConnectOptions {
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
    /// Set positive operation budgets.
    pub fn timeouts(mut self, value: RealtimeTimeouts) -> Self {
        self.timeouts = value;
        self
    }
    /// Send the upgrade's x-request-id. It must be nonblank printable ASCII, or `connect`
    /// returns an input error. Prism uses it if it is at most 128 characters,
    /// else makes its own; `info().request_id` reports the one it used. For
    /// now, hosted Prism always makes its own.
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
/// Proactive reply options.
#[derive(Clone, Debug, Default)]
pub struct ReplyOptions {
    pub(crate) instruction: Option<String>,
    pub(crate) tool_call: Option<ToolCallRef>,
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
pub struct SessionInfo {
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
}
/// Structured service error, available from Error::realtime_fault.
#[derive(Clone, Debug)]
pub struct RealtimeFault {
    /// Service error code.
    pub code: String,
    /// Service explanation.
    pub message: String,
    /// Event, utterance, response, tool_roundtrip, session, or unknown.
    pub scope: String,
    /// Owning request ID.
    pub request_id: Option<String>,
    /// Owning response ID.
    pub response_id: Option<String>,
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
/// One event with protocol correlation.
#[derive(Clone, Debug)]
pub struct SessionEvent {
    /// Owning session.
    pub session_id: String,
    /// Server event identifier, if supplied.
    pub event_id: String,
    /// Echoed client request identifier.
    pub request_id: Option<String>,
    /// Typed event contents.
    pub payload: RealtimeEvent,
}
/// Typed Prism event contents. Unknown wire events are ignored.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RealtimeEvent {
    /// A typed or proactive turn can be submitted.
    InputReady,
    /// Possible speech; applications can lower playback volume.
    SpeechHint {
        /// Audio start time.
        audio_start_ms: u64,
    },
    /// Confirmed user speech.
    SpeechStarted {
        /// Input item.
        item_id: String,
        /// Audio start time.
        audio_start_ms: u64,
    },
    /// End of a user speech turn.
    SpeechStopped {
        /// Input item.
        item_id: String,
        /// Audio end time.
        audio_end_ms: u64,
        /// Semantic, timeout, silence, max_duration, detector_changed, or unknown.
        end_reason: Option<String>,
    },
    /// Input audio committed.
    InputCommitted {
        /// Input item.
        item_id: String,
    },
    /// Incremental input transcription.
    TranscriptDelta {
        /// Input item.
        item_id: String,
        /// Text to append.
        delta: String,
    },
    /// Final input transcription.
    TranscriptFinal {
        /// Input item.
        item_id: String,
        /// Complete text.
        text: String,
    },
    /// Input transcription failed.
    TranscriptFailed {
        /// Input item.
        item_id: String,
        /// Service fault.
        fault: RealtimeFault,
    },
    /// A new response was accepted.
    ResponseStarted {
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
    /// A caller cancelled a submitted reply request.
    ResponseAbandoned {
        /// Accepted response to discard.
        response: ResponseRef,
    },
    /// Assistant output began.
    MessageStarted {
        /// Output reference.
        output: OutputRef,
    },
    /// Incremental assistant text.
    TextDelta {
        /// Output reference.
        output: OutputRef,
        /// Text to append.
        delta: String,
    },
    /// Complete assistant text.
    TextDone {
        /// Output reference.
        output: OutputRef,
        /// Complete text.
        text: String,
    },
    /// Assistant PCM audio.
    AudioDelta {
        /// Output reference.
        output: OutputRef,
        /// Mono PCM16 at 24 kHz.
        audio: AudioChunk,
    },
    /// No more audio for this output.
    AudioDone {
        /// Output reference.
        output: OutputRef,
    },
    /// An application must execute a function.
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
    ResponseEnded {
        /// Response reference.
        response: ResponseRef,
        /// Completed, cancelled, failed, or unknown.
        status: String,
        /// Optional reason.
        reason: Option<String>,
    },
    /// A nonterminal service error.
    Fault(RealtimeFault),
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
