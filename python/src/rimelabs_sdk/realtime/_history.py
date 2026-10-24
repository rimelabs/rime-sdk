"""Conversation item conversion. The server owns placement and deletion rules."""

from dataclasses import asdict

from .._errors import RimeInputError, RimeStreamError
from . import _types as t


def item_settings(item: t.HistoryItem) -> dict[str, object]:
    if not isinstance(item, (t.HistoryMessage, t.HistoryFunctionCall, t.HistoryFunctionResult)):
        raise RimeInputError("item must be a history message, call, or result")
    value = asdict(item)
    if item.id is None:
        value.pop("id")
    else:
        if not isinstance(item.id, str) or not 1 <= len(item.id) <= 32:
            raise RimeInputError("item.id must contain 1 to 32 characters")
    if isinstance(item, t.HistoryMessage):
        if item.role not in ("user", "assistant", "system"):
            raise RimeInputError("history role must be user, assistant, or system")
        value["type"] = "message"
        if not isinstance(item.text, str) or len(item.text) > 4000:
            raise RimeInputError("history text must contain at most 4000 characters")
    elif isinstance(item, t.HistoryFunctionCall):
        value["type"] = "function_call"
        if not item.call_id or not item.name:
            raise RimeInputError("past calls require a call_id and name")
    else:
        value["type"] = "function_call_output"
        if not item.call_id:
            raise RimeInputError("tool results require a call_id")
    for field in value.values():
        if not isinstance(field, str):
            raise RimeInputError("history item fields must be strings")
        try:
            field.encode("utf-8")
        except UnicodeEncodeError:
            raise RimeInputError("history item fields must be valid UTF-8") from None
    return value


def history_id(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise RimeInputError("history ID must be a nonempty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise RimeInputError("history ID must be valid UTF-8") from None
    return value


def listed_item(value: object) -> t.HistoryItem:
    if not isinstance(value, dict) or not isinstance(value.get("id"), str) or not value["id"]:
        raise RimeStreamError("Invalid history item id")

    def field(name: str) -> str:
        v = value.get(name)
        if not isinstance(v, str):
            raise RimeStreamError("Invalid history item " + name)
        if name in ("call_id", "name") and not v:
            raise RimeStreamError("Invalid history " + name)
        return v

    kind = value.get("type")
    if kind == "message":
        role = field("role")
        if role not in ("user", "assistant", "system"):
            raise RimeStreamError("Invalid history role")
        from typing import Literal, cast

        return t.HistoryMessage(
            id=value["id"],
            role=cast(Literal["user", "assistant", "system"], role),
            text=field("text"),
        )
    if kind == "function_call":
        return t.HistoryFunctionCall(
            id=value["id"],
            call_id=field("call_id"),
            name=field("name"),
            arguments=field("arguments"),
        )
    if kind == "function_call_output":
        return t.HistoryFunctionResult(
            id=value["id"], call_id=field("call_id"), output=field("output")
        )
    raise RimeStreamError("Invalid history item type")
