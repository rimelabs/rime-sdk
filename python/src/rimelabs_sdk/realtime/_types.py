"""Values exchanged by a realtime session. Audio is signed little-endian PCM16."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from .._errors import RimeAudioFormatError, RimeError, RimeInputError, RimeTimeoutError
from .._pcm import PCMFormat


class RealtimeAdmissionTimeout(RimeTimeoutError):
    """Readiness expired with no request outstanding. The session remains usable."""


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]
TurnEagerness: TypeAlias = Literal["low", "medium", "high", "auto"]
# What ended the user's turn:
# - semantic: Prism judged from the user's speech that they had finished;
#   eagerness sets how readily it does.
# - timeout: the user was silent for end_of_turn_timeout_ms before Prism judged that
#   they had finished.
# - silence: Prism was using its backup turn detection, which ends a turn at a short
#   pause and ignores the session's turn detection settings.
# - max_duration: the user spoke for the longest turn Prism takes; what they say next
#   starts a new turn.
# - detector_changed: Prism switched between its main and backup turn detection while
#   the user spoke; nothing the user did ended the turn.
SpeechEndReason: TypeAlias = Literal[
    "semantic", "timeout", "silence", "max_duration", "detector_changed"
]


@dataclass(frozen=True, kw_only=True)
class TurnDetection:
    """Optional turn timing overrides. None leaves a setting to the server.

    Eagerness controls how readily Prism ends a turn. The silence limit is
    an integer from 1,000 to 10,000 milliseconds, for example 2,000.
    """

    eagerness: TurnEagerness | None = None
    end_of_turn_timeout_ms: int | None = None

    def __post_init__(self):
        if self.eagerness is not None and self.eagerness not in ("low", "medium", "high", "auto"):
            raise RimeInputError("Turn eagerness must be low, medium, high, or auto")
        value = self.end_of_turn_timeout_ms
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or not 1000 <= value <= 10000
        ):
            raise RimeInputError("End-of-turn timeout must be an integer from 1000 to 10000 ms")


@dataclass(frozen=True, kw_only=True)
class TurnDetectionInfo:
    """Server-reported timing. None means the server did not publish the field."""

    eagerness: TurnEagerness | Literal["unknown"] | None = None
    end_of_turn_timeout_ms: int | None = None


@dataclass(frozen=True, kw_only=True)
class RealtimeTimeouts:
    connect_s: float = 10.0
    ready_s: float = 30.0
    request_s: float = 10.0

    def __post_init__(self):
        for value in (self.connect_s, self.ready_s, self.request_s):
            if not math.isfinite(value) or value <= 0:
                raise RimeInputError("Timeouts must be finite and positive")


@dataclass(frozen=True, kw_only=True)
class AudioChunk:
    data: bytes
    format: PCMFormat = field(default_factory=PCMFormat)

    def __post_init__(self):
        if not isinstance(self.data, bytes) or len(self.data) % (2 * self.format.channels):
            raise RimeAudioFormatError("Audio must contain complete PCM16 frames as bytes")


@dataclass(frozen=True, kw_only=True)
class ToolDefinition:
    name: str
    parameters: JsonObject
    description: str = ""


@dataclass(frozen=True, kw_only=True)
class ResponseRef:
    session_id: str
    response_id: str


@dataclass(frozen=True, kw_only=True)
class ItemRef:
    session_id: str
    item_id: str


@dataclass(frozen=True, kw_only=True)
class ToolCallRef:
    session_id: str
    response_id: str
    call_id: str


@dataclass(frozen=True, kw_only=True)
class OutputRef:
    response: ResponseRef
    item_id: str
    output_index: int
    content_index: int


@dataclass(frozen=True, kw_only=True)
class SessionInfo:
    session_id: str
    model: Literal["prism"]
    voice: str
    interrupt_on_speech: bool
    tool_result_timeout_s: float | None = None
    tool_continuation_timeout_s: float | None = None
    turn_detection: TurnDetectionInfo = field(default_factory=TurnDetectionInfo)


@dataclass(frozen=True, kw_only=True)
class RealtimeFault:
    code: str
    message: str
    scope: Literal["event", "utterance", "response", "tool_roundtrip", "session", "unknown"]
    request_id: str | None = None
    response_id: str | None = None
    item_id: str | None = None
    call_ids: tuple[str, ...] = ()
    correlation_id: str | None = None
    parameter: str | None = None


class RimeRealtimeError(RimeError):
    def __init__(self, fault: RealtimeFault):
        self.fault = fault
        super().__init__(f"{fault.code}: {fault.message}", request_id=fault.request_id)


@dataclass(frozen=True, kw_only=True)
class InputReady:
    kind: Literal["input.ready"] = field(default="input.ready", init=False)


@dataclass(frozen=True, kw_only=True)
class SpeechHint:
    """The user may have started speaking over a playing reply.

    Prism sends it before ``SpeechStarted`` when it may interrupt the reply. It
    can be a false alarm, so lower playback on it rather than stopping.
    """

    audio_start_ms: int
    kind: Literal["speech.hint"] = field(default="speech.hint", init=False)


@dataclass(frozen=True, kw_only=True)
class SpeechStarted:
    item_id: str
    audio_start_ms: int
    kind: Literal["speech.started"] = field(default="speech.started", init=False)


@dataclass(frozen=True, kw_only=True)
class SpeechStopped:
    """None in end_reason means the server did not publish the field."""

    item_id: str
    audio_end_ms: int
    end_reason: SpeechEndReason | Literal["unknown"] | None = None
    kind: Literal["speech.stopped"] = field(default="speech.stopped", init=False)


@dataclass(frozen=True, kw_only=True)
class InputCommitted:
    item_id: str
    kind: Literal["input.committed"] = field(default="input.committed", init=False)


@dataclass(frozen=True, kw_only=True)
class TranscriptDelta:
    item_id: str
    delta: str
    kind: Literal["transcript.delta"] = field(default="transcript.delta", init=False)


@dataclass(frozen=True, kw_only=True)
class TranscriptFinal:
    item_id: str
    text: str
    kind: Literal["transcript.final"] = field(default="transcript.final", init=False)


@dataclass(frozen=True, kw_only=True)
class TranscriptFailed:
    item_id: str
    error: RealtimeFault
    kind: Literal["transcript.failed"] = field(default="transcript.failed", init=False)


@dataclass(frozen=True, kw_only=True)
class ResponseStarted:
    response: ResponseRef
    cause: Literal["speech", "text", "proactive", "tools", "unknown"]
    parent: ResponseRef | None = None
    input_item_id: str | None = None
    kind: Literal["response.started"] = field(default="response.started", init=False)


@dataclass(frozen=True, kw_only=True)
class MessageStarted:
    output: OutputRef
    kind: Literal["message.started"] = field(default="message.started", init=False)


@dataclass(frozen=True, kw_only=True)
class TextDelta:
    output: OutputRef
    delta: str
    kind: Literal["text.delta"] = field(default="text.delta", init=False)


@dataclass(frozen=True, kw_only=True)
class TextDone:
    output: OutputRef
    text: str
    kind: Literal["text.done"] = field(default="text.done", init=False)


@dataclass(frozen=True, kw_only=True)
class AudioDelta:
    output: OutputRef
    audio: AudioChunk
    kind: Literal["audio.delta"] = field(default="audio.delta", init=False)


@dataclass(frozen=True, kw_only=True)
class AudioDone:
    output: OutputRef
    kind: Literal["audio.done"] = field(default="audio.done", init=False)


@dataclass(frozen=True, kw_only=True)
class ToolCall:
    call: ToolCallRef
    item_id: str
    name: str
    arguments: JsonObject
    kind: Literal["tool.call"] = field(default="tool.call", init=False)


@dataclass(frozen=True, kw_only=True)
class ResponseAbandoned:
    """A cancelled request accepted a response that its caller did not receive.

    The SDK requests cancellation. Event consumers still own playback reporting.
    This local notice can follow ResponseEnded when completion races cancellation.
    """

    response: ResponseRef
    kind: Literal["response.abandoned"] = field(default="response.abandoned", init=False)


@dataclass(frozen=True, kw_only=True)
class ResponseEnded:
    response: ResponseRef
    status: Literal["completed", "cancelled", "failed", "unknown"]
    reason: str | None = None
    kind: Literal["response.ended"] = field(default="response.ended", init=False)


@dataclass(frozen=True, kw_only=True)
class FaultEvent:
    error: RealtimeFault
    kind: Literal["error"] = field(default="error", init=False)


InputEvent: TypeAlias = (
    InputReady
    | SpeechHint
    | SpeechStarted
    | SpeechStopped
    | InputCommitted
    | TranscriptDelta
    | TranscriptFinal
    | TranscriptFailed
)
ResponseEvent: TypeAlias = (
    ResponseStarted
    | ResponseAbandoned
    | MessageStarted
    | TextDelta
    | TextDone
    | AudioDelta
    | AudioDone
    | ToolCall
    | ResponseEnded
)


@dataclass(frozen=True, kw_only=True)
class SessionEvent:
    session_id: str
    event_id: str
    request_id: str | None
    payload: InputEvent | ResponseEvent | FaultEvent


@dataclass(frozen=True, kw_only=True)
class PlaybackInterrupted:
    output: OutputRef
    audio_end_ms: int
    kind: Literal["interrupted"] = field(default="interrupted", init=False)

    def __post_init__(self):
        if (
            isinstance(self.audio_end_ms, bool)
            or not isinstance(self.audio_end_ms, int)
            or not 0 <= self.audio_end_ms <= 4294967295
        ):
            raise RimeInputError("audio_end_ms must be an unsigned 32-bit integer")


@dataclass(frozen=True, kw_only=True)
class PlaybackFinished:
    response: ResponseRef
    played_ms: float | None = None
    audible_tail_ms: float | None = None
    kind: Literal["finished"] = field(default="finished", init=False)

    def __post_init__(self):
        for value in (self.played_ms, self.audible_tail_ms):
            if value is not None and (not math.isfinite(value) or value < 0):
                raise RimeInputError("Playback times must be finite and nonnegative")


PlaybackReport: TypeAlias = PlaybackInterrupted | PlaybackFinished
