# SPDX-License-Identifier: Apache-2.0
"""Validated protocol values. Unknown event kinds and extra fields remain open."""

from __future__ import annotations

import base64
import json
import math
import re
from dataclasses import dataclass, field
from typing import Generic, Literal, NoReturn, NotRequired, TypeAlias, TypedDict, TypeVar, cast

from .._errors import RimeInputError, RimeStreamError
from . import _types as t
from ._history import listed_item


class _TurnDetection(TypedDict):
    interrupt_response: bool
    eagerness: NotRequired[t.TurnEagerness]
    prism_end_of_turn_timeout_ms: NotRequired[int]


class _Function(TypedDict):
    name: str
    description: str
    parameters: t.JsonObject


class _Tool(TypedDict):
    type: Literal["function"]
    function: _Function
    prism_filler: NotRequired[t.JsonObject]


class SessionSettings(TypedDict):
    modalities: list[Literal["text", "audio"]]
    input_audio_format: Literal["pcm16"]
    turn_detection: _TurnDetection
    tools: list[_Tool]
    voice: NotRequired[str]
    instructions: NotRequired[str]
    prism_response_metadata: NotRequired[t.Labels]
    truncation: NotRequired[t.JsonValue]


def response_metadata(value: object) -> t.Labels:
    """A copy of the client's labels for every reply: keys to strings."""
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(label, str) for key, label in value.items()
    ):
        raise RimeInputError("response_metadata must map keys to strings")
    return dict(value)


def request_metadata(value: t.Labels | None, reserved: dict[str, str]) -> dict[str, str]:
    """Copy one reply's labels and add protocol fields owned by the SDK."""
    labels = {} if value is None else response_metadata(value)
    if any(key.startswith("prism_") for key in labels):
        raise RimeInputError("request metadata keys cannot start with prism_")
    return {**labels, **reserved}


class SessionPatch(TypedDict, total=False):
    truncation: t.JsonValue
    voice: str
    instructions: str
    tools: list[_Tool]
    turn_detection: dict[str, bool]


@dataclass(frozen=True, kw_only=True)
class SessionView:
    truncation: t.Truncation | None = None
    settings_revision: int | None = None
    id: str
    request_id: str | None
    response_metadata: t.Labels | None
    voice: str
    interrupt_on_speech: bool | None
    tool_result_timeout_s: float | None
    tool_continuation_timeout_s: float | None
    turn_detection: t.TurnDetectionInfo


@dataclass(frozen=True, kw_only=True)
class SessionCreated:
    session: SessionView
    kind: Literal["session.created"] = field(default="session.created", init=False)


@dataclass(frozen=True, kw_only=True)
class SessionUpdated:
    session: SessionView
    info: t.SessionInfo | None = None
    kind: Literal["session.updated"] = field(default="session.updated", init=False)


@dataclass(frozen=True, kw_only=True)
class ItemCreated:
    item_id: str
    kind: Literal["item.created"] = field(default="item.created", init=False)


@dataclass(frozen=True, kw_only=True)
class ToolResultCreated:
    item_id: str | None = None
    call_id: str
    kind: Literal["tool.result.created"] = field(default="tool.result.created", init=False)


@dataclass(frozen=True, kw_only=True)
class HistoryListed:
    items: tuple[t.HistoryItem, ...]
    kind: Literal["history.listed"] = field(default="history.listed", init=False)


@dataclass(frozen=True, kw_only=True)
class ItemDeleted:
    item_id: str
    kind: Literal["item.deleted"] = field(default="item.deleted", init=False)


@dataclass(frozen=True, kw_only=True)
class AudioCleared:
    kind: Literal["audio.cleared"] = field(default="audio.cleared", init=False)


@dataclass(frozen=True, kw_only=True)
class SessionIdleTimeout:
    idle_timeout_s: float
    kind: Literal["session.idle_timeout"] = field(default="session.idle_timeout", init=False)


@dataclass(frozen=True, kw_only=True)
class ResponseEnded(t.ResponseEnded):
    drain_token: str | None


Acknowledgment: TypeAlias = (
    SessionUpdated
    | HistoryListed
    | ItemDeleted
    | ItemCreated
    | ToolResultCreated
    | AudioCleared
    | t.ItemTruncated
    | t.ResponseStarted
    | ResponseEnded
)
ServerPayload: TypeAlias = (
    SessionCreated
    | Acknowledgment
    | t.InputEvent
    | t.MessageStarted
    | t.TextDelta
    | t.TextDone
    | t.AudioDelta
    | t.AudioDone
    | t.ToolCall
    | t.FaultEvent
    | SessionIdleTimeout
)


@dataclass(frozen=True, kw_only=True)
class ServerEvent:
    event_id: str
    request_id: str | None
    payload: ServerPayload


RequestKind: TypeAlias = Literal[
    "session.update",
    "conversation.item.create",
    "prism.conversation.get",
    "conversation.item.delete",
    "response.create",
    "response.cancel",
    "input_audio_buffer.clear",
    "conversation.item.truncate",
]
ResultT_co = TypeVar("ResultT_co", bound=Acknowledgment, covariant=True)


@dataclass(frozen=True)
class Request(Generic[ResultT_co]):
    """Each request carries and checks the type of its acknowledgment."""

    kind: RequestKind
    result_type: type[ResultT_co]
    allows_item_created: bool = False

    def result(self, event: Acknowledgment) -> ResultT_co:
        if not isinstance(event, self.result_type):
            raise RimeStreamError(f"Unexpected {event.kind} acknowledgment for {self.kind}")
        return event


UPDATE = Request("session.update", SessionUpdated)
GET_HISTORY = Request("prism.conversation.get", HistoryListed)
DELETE_ITEM = Request("conversation.item.delete", ItemDeleted)
ITEM = Request("conversation.item.create", ItemCreated)
TOOL_RESULT = Request("conversation.item.create", ToolResultCreated)
HISTORY_ITEM = Request("conversation.item.create", ItemCreated)
HISTORY_RESULT = Request("conversation.item.create", ToolResultCreated)
CREATE = Request("response.create", t.ResponseStarted)
TEXT = Request("response.create", t.ResponseStarted, allows_item_created=True)
CANCEL = Request("response.cancel", ResponseEnded)
CLEAR = Request("input_audio_buffer.clear", AudioCleared)
TRUNCATE = Request("conversation.item.truncate", t.ItemTruncated)


def _invalid(name: str) -> NoReturn:
    raise RimeStreamError(f"Invalid realtime field: {name}")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        _invalid(name)
    return cast(dict[str, object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        _invalid(name)
    return value


def _id(value: object, name: str) -> str:
    result = _string(value, name)
    if not result:
        _invalid(name)
    return result


def _optional_string(value: object, name: str) -> str | None:
    return None if value is None else _string(value, name)


def _optional_id(value: object, name: str) -> str | None:
    return None if value is None else _id(value, name)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(name)
    if not 0 <= value <= 9007199254740991 or int(value) != value:
        _invalid(name)
    return int(value)


def _revision(value: dict, name: str) -> int | None:
    return (
        _integer(value["prism_settings_revision"], name)
        if "prism_settings_revision" in value
        else None
    )


def _response_truncation(response: dict) -> t.ResponseTruncation | None:
    if "prism_truncation" not in response:
        return None
    value = _object(response["prism_truncation"], "response.prism_truncation")
    count = _integer(value.get("items_left_out"), "prism_truncation.items_left_out")
    if count == 0:
        _invalid("prism_truncation.items_left_out")
    return t.ResponseTruncation(
        items_left_out=count,
        first_kept_item_id=(
            _string(value["first_kept_item_id"], "prism_truncation.first_kept_item_id")
            if "first_kept_item_id" in value
            else None
        ),
    )


def _response_usage(response: dict[str, object]) -> t.ResponseUsage | None:
    value = response.get("usage")
    if value is None:
        return None
    usage = _object(value, "response.usage")
    return t.ResponseUsage(
        input_tokens=_integer(usage.get("input_tokens"), "response.usage.input_tokens"),
        output_tokens=_integer(usage.get("output_tokens"), "response.usage.output_tokens"),
        total_tokens=_integer(usage.get("total_tokens"), "response.usage.total_tokens"),
        context_window=_integer(usage.get("context_window"), "response.usage.context_window"),
    )


def _duration(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(name)
    if not math.isfinite(value) or value <= 0:
        _invalid(name)
    return float(value)


def truncation_settings(value: t.Truncation) -> t.JsonValue:
    """Serialize an explicit setting; callers omit None for older servers."""
    if isinstance(value, str) and value in ("auto", "disabled"):
        return value
    if isinstance(value, t.RetentionRatio):
        return {"type": "retention_ratio", "retention_ratio": value.retention_ratio}
    raise RimeInputError("truncation must be auto, disabled, or RetentionRatio")


def _truncation(value: object) -> t.Truncation:
    if isinstance(value, str) and value in ("auto", "disabled"):
        return cast(t.Truncation, value)
    if isinstance(value, dict) and value.get("type") == "retention_ratio":
        try:
            return t.RetentionRatio(cast(float, value.get("retention_ratio")))
        except RimeInputError:
            pass
    _invalid("session.truncation")


def _session(value: object) -> SessionView:
    view = _object(value, "session")
    detection = (
        _object(view["turn_detection"], "session.turn_detection")
        if "turn_detection" in view
        else {}
    )
    eagerness = None
    if "eagerness" in detection:
        value = _string(detection["eagerness"], "session.turn_detection.eagerness")
        eagerness = value if value in ("low", "medium", "high", "auto") else "unknown"
    end_timeout = None
    if "prism_end_of_turn_timeout_ms" in detection:
        end_timeout = _integer(
            detection["prism_end_of_turn_timeout_ms"],
            "session.turn_detection.prism_end_of_turn_timeout_ms",
        )
        if not 1000 <= end_timeout <= 10000:
            _invalid("session.turn_detection.prism_end_of_turn_timeout_ms")
    waits = (
        _object(view["prism_tool_waits"], "session.prism_tool_waits")
        if "prism_tool_waits" in view
        else {}
    )
    if "prism_tool_waits" in view and (
        "result_timeout_s" not in waits or "continuation_timeout_s" not in waits
    ):
        _invalid("session.prism_tool_waits")
    interrupt = view.get("prism_effective_interrupt_response")
    if not isinstance(interrupt, bool):
        if "prism_effective_interrupt_response" in view:
            _invalid("session.prism_effective_interrupt_response")
        interrupt = None
    return SessionView(
        truncation=_truncation(view["truncation"]) if "truncation" in view else None,
        settings_revision=_revision(view, "session.prism_settings_revision"),
        id=_id(view.get("id"), "session.id"),
        request_id=_optional_string(view.get("request_id"), "session.request_id"),
        response_metadata=_client_metadata(
            _object(view["prism_response_metadata"], "session.prism_response_metadata")
        )
        if "prism_response_metadata" in view
        else None,
        voice=_string(view.get("voice", ""), "session.voice"),
        interrupt_on_speech=interrupt,
        turn_detection=t.TurnDetectionInfo(
            eagerness=cast(t.TurnEagerness | Literal["unknown"] | None, eagerness),
            end_of_turn_timeout_ms=end_timeout,
        ),
        tool_result_timeout_s=_duration(waits.get("result_timeout_s"), "result_timeout_s"),
        tool_continuation_timeout_s=_duration(
            waits.get("continuation_timeout_s"), "continuation_timeout_s"
        ),
    )


def _fault(value: object) -> t.RealtimeFault:
    error = _object(value, "error")
    owner = _object(error["owner"], "error.owner") if error.get("owner") is not None else {}
    scope = _string(error.get("scope", "unknown"), "error.scope")
    calls = owner.get("call_ids")
    if calls is None:
        calls = []
    if not isinstance(calls, list):
        _invalid("error.owner.call_ids")
    return t.RealtimeFault(
        code=_string(error.get("code", "unknown"), "error.code"),
        message=_string(error.get("message", ""), "error.message"),
        scope=cast(
            Literal["event", "utterance", "response", "tool_roundtrip", "session", "unknown"],
            scope
            if scope in ("event", "utterance", "response", "tool_roundtrip", "session")
            else "unknown",
        ),
        request_id=_optional_string(owner.get("event_id"), "error.owner.event_id"),
        response_id=_optional_string(owner.get("response_id"), "error.owner.response_id"),
        item_id=_optional_string(owner.get("item_id"), "error.owner.item_id"),
        call_ids=tuple(_id(value, "error.owner.call_ids") for value in calls),
        correlation_id=_optional_string(error.get("correlation_id"), "error.correlation_id"),
        parameter=_optional_string(error.get("param"), "error.param"),
    )


def _json(value: object) -> t.JsonValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_json(item) for item in value]
    return {key: _json(item) for key, item in _object(value, "arguments").items()}


def _client_metadata(metadata: dict[str, object]) -> t.Labels:
    """The client's labels in metadata: its string values under keys that are not Prism's."""
    return {
        key: label
        for key, label in metadata.items()
        if not key.startswith("prism_") and isinstance(label, str)
    }


def _arguments(value: object) -> t.JsonObject:
    result = _json(json.loads(_string(value, "arguments"), parse_constant=_invalid))
    if not isinstance(result, dict):
        _invalid("arguments")
    return result


def _audio(value: object) -> t.AudioChunk:
    encoded = _string(value, "delta")
    if not re.fullmatch(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?", encoded):
        _invalid("audio.delta")
    data = base64.b64decode(encoded, validate=True)
    if len(data) % 2:
        _invalid("audio.delta")
    return t.AudioChunk(data=data, format=t.PCMFormat(sample_rate=24000))


def decode(raw: str | bytes, session_id: str | None) -> ServerEvent | None:
    try:
        if not isinstance(raw, str):
            raise RimeStreamError("Expected a JSON text event")
        return _decode_value(json.loads(raw, parse_constant=_invalid), session_id)
    except RimeStreamError:
        raise
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError):
        raise RimeStreamError("Invalid realtime event") from None


def _decode_value(value: object, session_id: str | None) -> ServerEvent | None:
    event = _object(value, "event")
    kind = _string(event.get("type"), "type")

    def ref(value: object) -> t.ResponseRef:
        if session_id is None:
            raise RimeStreamError("Event arrived before session.created")
        return t.ResponseRef(session_id=session_id, response_id=_id(value, "response.id"))

    payload: ServerPayload
    if kind == "session.created":
        payload = SessionCreated(session=_session(event.get("session")))
    elif kind == "session.updated":
        payload = SessionUpdated(session=_session(event.get("session")))
    elif kind == "conversation.item.created":
        ack_item = _object(event.get("item"), "item")
        if ack_item.get("type") == "function_call_output":
            payload = ToolResultCreated(
                call_id=_id(ack_item.get("call_id"), "item.call_id"),
                item_id=_optional_string(ack_item.get("id"), "item.id"),
            )
        else:
            if "type" in ack_item and ack_item["type"] not in ("message", "function_call"):
                _invalid("item.type")
            payload = ItemCreated(item_id=_id(ack_item.get("id"), "item.id"))
    elif kind == "prism.conversation":
        items = event.get("items")
        if not isinstance(items, list):
            _invalid("items")
        payload = HistoryListed(items=tuple(listed_item(item) for item in items))
    elif kind == "conversation.item.deleted":
        payload = ItemDeleted(item_id=_id(event.get("item_id"), "item_id"))
    elif kind == "input_audio_buffer.cleared":
        payload = AudioCleared()
    elif kind == "session.idle_timeout":
        if event.get("reason") != "idle_timeout":
            _invalid("reason")
        timeout = event.get("idle_timeout_s")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            _invalid("idle_timeout_s")
        try:
            idle_timeout_s = float(timeout)
        except OverflowError:
            _invalid("idle_timeout_s")
        if not math.isfinite(idle_timeout_s) or idle_timeout_s < 1:
            _invalid("idle_timeout_s")
        payload = SessionIdleTimeout(idle_timeout_s=idle_timeout_s)
    elif kind == "conversation.item.truncated":
        payload = t.ItemTruncated(
            item_id=_id(event.get("item_id"), "item_id"),
            content_index=_integer(event.get("content_index"), "content_index"),
            audio_end_ms=_integer(event.get("audio_end_ms"), "audio_end_ms"),
            kept_text=(
                _string(event["prism_kept_text"], "prism_kept_text")
                if "prism_kept_text" in event
                else None
            ),
        )
    elif kind == "prism.typed_input.ready":
        payload = t.InputReady()
    elif kind == "prism.input_audio_buffer.speech_hint":
        payload = t.SpeechHint(
            audio_start_ms=_integer(event.get("audio_start_ms"), "audio_start_ms"),
        )
    elif kind == "input_audio_buffer.speech_started":
        payload = t.SpeechStarted(
            item_id=_id(event.get("item_id"), "item_id"),
            audio_start_ms=_integer(event.get("audio_start_ms"), "audio_start_ms"),
        )
    elif kind == "input_audio_buffer.speech_stopped":
        reason = (
            _string(event["prism_end_reason"], "prism_end_reason")
            if "prism_end_reason" in event
            else None
        )
        payload = t.SpeechStopped(
            item_id=_id(event.get("item_id"), "item_id"),
            audio_end_ms=_integer(event.get("audio_end_ms"), "audio_end_ms"),
            end_reason=cast(
                t.SpeechEndReason | Literal["unknown"] | None,
                reason
                if reason
                in (None, "semantic", "timeout", "silence", "max_duration", "detector_changed")
                else "unknown",
            ),
        )
    elif kind == "input_audio_buffer.committed":
        payload = t.InputCommitted(item_id=_id(event.get("item_id"), "item_id"))
    elif kind == "conversation.item.input_audio_transcription.delta":
        payload = t.TranscriptDelta(
            item_id=_id(event.get("item_id"), "item_id"), delta=_string(event.get("delta"), "delta")
        )
    elif kind == "conversation.item.input_audio_transcription.completed":
        payload = t.TranscriptFinal(
            item_id=_id(event.get("item_id"), "item_id"),
            text=_string(event.get("transcript"), "transcript"),
        )
    elif kind == "conversation.item.input_audio_transcription.failed":
        error = _object(event.get("error"), "error")
        item_id = _id(event.get("item_id"), "item_id")
        payload = t.TranscriptFailed(
            item_id=item_id,
            error=_fault(
                {
                    "code": _string(error.get("code"), "error.code"),
                    "message": _string(error.get("message"), "error.message"),
                    "scope": "utterance",
                    "owner": {"item_id": item_id},
                }
            ),
        )
    elif kind == "response.created":
        response = _object(event.get("response"), "response")
        metadata = (
            _object(response["metadata"], "metadata")
            if response.get("metadata") is not None
            else {}
        )
        cause = (
            _string(metadata["prism_cause"], "prism_cause") if "prism_cause" in metadata else None
        )
        parent = _optional_string(
            metadata.get("prism_parent_response_id"), "prism_parent_response_id"
        )
        input_item_id = _optional_string(metadata.get("prism_input_item_id"), "prism_input_item_id")
        input_item_ids = response.get("prism_input_item_ids")
        if input_item_ids is None:
            input_item_ids = [] if input_item_id is None else [input_item_id]
        if not isinstance(input_item_ids, list):
            _invalid("prism_input_item_ids")
        payload = t.ResponseStarted(
            settings_revision=_revision(response, "response.prism_settings_revision"),
            response=ref(response.get("id")),
            cause=cast(
                Literal["speech", "text", "tools", "proactive", "unknown"],
                "speech"
                if cause is None
                else {
                    "user_text": "text",
                    "tool_continuation": "tools",
                    "proactive": "proactive",
                }.get(cause, "unknown"),
            ),
            parent=ref(parent) if parent is not None else None,
            input_item_id=input_item_id,
            instruction_item_id=_optional_id(
                metadata.get("prism_instruction_item_id"), "prism_instruction_item_id"
            ),
            metadata=_client_metadata(metadata),
            input_item_ids=tuple(_id(value, "prism_input_item_ids") for value in input_item_ids),
        )
    elif kind == "response.done":
        response = _object(event.get("response"), "response")
        metadata = (
            _object(response["metadata"], "metadata")
            if response.get("metadata") is not None
            else {}
        )
        details = (
            _object(response["status_details"], "status_details")
            if response.get("status_details") is not None
            else {}
        )
        status = _string(response.get("status"), "response.status")
        payload = ResponseEnded(
            settings_revision=_revision(response, "response.prism_settings_revision"),
            truncation=_response_truncation(response),
            usage=_response_usage(response),
            response=ref(response.get("id")),
            status=cast(
                Literal["completed", "cancelled", "failed", "unknown"],
                status if status in ("completed", "cancelled", "failed") else "unknown",
            ),
            reason=_optional_string(details.get("reason"), "reason"),
            drain_token=_optional_string(metadata.get("prism_drain_token"), "prism_drain_token"),
        )
    elif kind == "response.function_call_arguments.done":
        response_ref = ref(event.get("response_id"))
        payload = t.ToolCall(
            call=t.ToolCallRef(
                session_id=response_ref.session_id,
                response_id=response_ref.response_id,
                call_id=_id(event.get("call_id"), "call_id"),
            ),
            item_id=_id(event.get("item_id"), "item_id"),
            name=_string(event.get("name"), "name"),
            arguments=_arguments(event.get("arguments")),
        )
    elif kind in (
        "response.output_item.added",
        "response.text.delta",
        "response.text.done",
        "response.audio.delta",
        "response.audio.done",
    ):
        item = _object(event.get("item"), "item") if kind == "response.output_item.added" else None
        if item is not None and _string(item.get("type"), "item.type") != "message":
            return None
        content_index = _integer(
            event.get("content_index", 0)
            if kind == "response.output_item.added"
            else event.get("content_index"),
            "content_index",
        )
        if content_index != 0:
            raise RimeStreamError("Unsupported Prism contract: content_index must be zero")
        output = t.OutputRef(
            response=ref(event.get("response_id")),
            item_id=_id(item.get("id") if item is not None else event.get("item_id"), "item_id"),
            output_index=_integer(event.get("output_index"), "output_index"),
            content_index=content_index,
        )
        if kind == "response.output_item.added":
            payload = t.MessageStarted(output=output)
        elif kind == "response.text.delta":
            payload = t.TextDelta(output=output, delta=_string(event.get("delta"), "delta"))
        elif kind == "response.text.done":
            payload = t.TextDone(output=output, text=_string(event.get("text"), "text"))
        elif kind == "response.audio.delta":
            payload = t.AudioDelta(output=output, audio=_audio(event.get("delta")))
        else:
            payload = t.AudioDone(output=output)
    elif kind == "error":
        payload = t.FaultEvent(error=_fault(event.get("error")))
    else:
        return None
    return ServerEvent(
        event_id=_string(event.get("event_id", ""), "event_id"),
        request_id=_optional_string(event.get("prism_request_event_id"), "prism_request_event_id"),
        payload=payload,
    )
