"""Host error conversion. SDK decisions live in the native core."""

import json

from . import _errors

INHERIT = object()


def encode_options(options):
    try:
        return json.dumps(options)
    except (TypeError, ValueError) as error:
        raise _errors.RimeInputError("Options must contain JSON-serializable values") from error


def translate(error):
    if isinstance(error, _errors.RimeError):
        return error
    try:
        detail = json.loads(str(error))
        cls = getattr(_errors, "Rime" + detail["kind"] + "Error")
        return cls(detail["message"], request_id=detail.get("request_id"))
    except (ValueError, KeyError, AttributeError, TypeError):
        return _errors.RimeStreamError("Native SDK operation failed")


def call(function, *args):
    try:
        return function(*args)
    except ValueError as error:
        raise translate(error) from None
