"""Shared endpoint and timeout validation."""

import math
import re

from ._errors import RimeInputError

INHERIT = object()


def endpoint_address(endpoint):
    error = "endpoint must be a hostname with an optional port (1-65535), without a scheme or path"
    if not isinstance(endpoint, str):
        raise RimeInputError(error)
    host, separator, port = endpoint.partition(":")
    if (
        not host
        or len(host) > 253
        or any(
            not re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
            for label in host.split(".")
        )
    ):
        raise RimeInputError(error)
    if separator and (not re.fullmatch(r"[0-9]{1,5}", port) or not 1 <= int(port) <= 65535):
        raise RimeInputError(error)
    host = host.lower()
    return f"{host}:{int(port) if separator else 443}", host


def timeout(value, inherited=None):
    if value is INHERIT:
        return inherited
    try:
        valid = value is None or (
            not isinstance(value, bool)
            and isinstance(value, (float, int))
            and math.isfinite(value)
            and value > 0
        )
    except OverflowError:
        valid = False
    if not valid:
        raise RimeInputError("timeout must be finite positive seconds or None")
    return value
