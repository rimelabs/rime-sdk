"""Shared validation and snapshots for connection and session tool settings."""

import copy
import json
from collections import deque
from collections.abc import Sequence

from .._errors import RimeInputError, RimeResourceLimitError
from . import _types as t
from ._protocol import _Tool
from ._types import JsonObject, PrismFiller, ToolDefinition


def speed_setting(value: float) -> float:
    """Validate the supplied speech rate without supplying a default."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0.85 <= value <= 1.3:
        raise RimeInputError("Speed must be a number from 0.85 to 1.3")
    return value


def turn_settings(value: t.TurnDetectionUpdate) -> dict[str, t.JsonValue]:
    """Snapshot supplied turn settings. Empty phrase lists clear the server setting."""
    if not isinstance(value, t.TurnDetectionUpdate):
        raise RimeInputError("turn_detection must be a TurnDetectionUpdate value")
    result: dict[str, t.JsonValue] = {}
    for name in ("interrupt_min_words", "interrupt_phrases", "acknowledgment_phrases"):
        setting = getattr(value, name)
        if setting is not None:
            result[name] = list(setting) if isinstance(setting, tuple) else setting
    if isinstance(value, t.TurnDetection):
        for name, wire in (
            ("eagerness", "eagerness"),
            ("end_of_turn_timeout_ms", "prism_end_of_turn_timeout_ms"),
            ("end_of_turn_min_silence_ms", "end_of_turn_min_silence_ms"),
        ):
            setting = getattr(value, name)
            if setting is not None:
                result[wire] = setting
    return result


def review_settings(value: t.ReviewSettings) -> t.JsonObject:
    """Encode the fixed review policy independently of the callback."""
    if not isinstance(value, t.ReviewSettings):
        raise RimeInputError("review must be a Review value")
    action = value.on_timeout
    result: t.JsonObject = {
        "timeout_ms": value.timeout_ms,
        "on_timeout": "stop"
        if isinstance(action, t.Stop)
        else "speak"
        if isinstance(action, t.Speak)
        else "say",
    }
    if isinstance(action, t.Say):
        result["timeout_line"] = action.text
    return result


def _filler_settings(filler: PrismFiller) -> JsonObject:
    if not isinstance(filler, PrismFiller):
        raise RimeInputError("prism_filler must be a PrismFiller value")
    result: JsonObject = {"type": filler.type}
    if filler.text is not None:
        result["text"] = filler.text
    if filler.instructions is not None:
        result["instructions"] = filler.instructions
    return result


def tool_settings(tools: Sequence[ToolDefinition]) -> list[_Tool]:
    result: list[_Tool] = []
    names: set[str] = set()
    for tool in tools:
        if not isinstance(tool, ToolDefinition):
            raise RimeInputError("tools must contain ToolDefinition values")
        if not isinstance(tool.name, str) or not tool.name.strip() or tool.name in names:
            raise RimeInputError("Tool names must be nonblank and unique")
        names.add(tool.name)
        if not isinstance(tool.parameters, dict) or not isinstance(tool.description, str):
            raise RimeInputError(
                "Tool parameters must be an object and description must be a string"
            )
        value: _Tool = {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            },
        }
        if tool.prism_filler is not None:
            value["prism_filler"] = _filler_settings(tool.prism_filler)
        result.append(value)
    try:
        result = copy.deepcopy(result)
        json.dumps(result, allow_nan=False)
    except (ValueError, TypeError):
        raise RimeInputError("Tool schemas must contain finite JSON values") from None
    return result


class InterruptionPolicies:
    """Retain bounded policy changes until a reply selects its settings revision."""

    def __init__(self) -> None:
        self._changes: deque[tuple[int, bool | None]] = deque(maxlen=128)
        self._current = True
        self._last_revision: int | None = None

    def record(self, revision: int | None, interrupt: bool) -> None:
        if not self._changes:
            self._changes.append((revision if revision is not None else 0, interrupt))
        elif revision is None:
            if interrupt != self._current and self._changes[-1][1] is not None:
                # The change happened after the last known acknowledgment, but
                # its exact revision cannot be assigned to a later reply.
                first_unknown = self._last_revision + 1 if self._last_revision is not None else 0
                self._changes.append((first_unknown, None))
        elif self._changes[-1][1] is None or interrupt != self._current:
            self._changes.append((revision, interrupt))
        if revision is not None:
            self._last_revision = revision
        self._current = interrupt

    def resolve(self, revision: int | None) -> bool:
        if revision is None or not self._changes:
            return self._current
        for first_revision, interrupt in reversed(self._changes):
            if first_revision <= revision:
                if interrupt is None:
                    raise RimeResourceLimitError("Reply interruption policy revision is unknown")
                return interrupt
        raise RimeResourceLimitError("Reply settings revision is no longer retained")
