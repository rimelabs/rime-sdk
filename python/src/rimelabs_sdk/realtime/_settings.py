"""Shared validation and snapshots for connection and session tool settings."""

import copy
import json
from collections import deque
from collections.abc import Sequence

from .._errors import RimeInputError, RimeResourceLimitError
from ._protocol import _Tool
from ._types import JsonObject, PrismFiller, ToolDefinition


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
        self._changes: deque[tuple[int, bool]] = deque(maxlen=128)
        self._current = True

    def record(self, revision: int | None, interrupt: bool) -> None:
        if revision is None:
            self._changes.clear()
        if not self._changes or interrupt != self._current:
            self._changes.append((revision if revision is not None else 0, interrupt))
        self._current = interrupt

    def resolve(self, revision: int | None) -> bool:
        if revision is None or not self._changes:
            return self._current
        for first_revision, interrupt in reversed(self._changes):
            if first_revision <= revision:
                return interrupt
        raise RimeResourceLimitError("Reply settings revision is no longer retained")
