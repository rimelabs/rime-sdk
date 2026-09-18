"""Private policy. The Themis wire contract is an implementation assumption."""

import math
from dataclasses import dataclass

from ._errors import RimeInputError

INHERIT = object()


@dataclass(frozen=True)
class Policy:
    target: str = "coda.api.rime.ai:443"
    audience: str = "coda.api.rime.ai"
    exchange_url: str = "https://themis.api.rime.ai/v1/token"
    sentence_bytes: int = 65536
    source_chars: int = 1024
    output_bytes: int = 96000
    output_chunk_bytes: int = 9600
    receive_bytes: int = 4194304
    auth_timeout: float = 10
    connection_timeout: float = 10
    first_audio_timeout: float = 30
    progress_timeout: float = 60
    discovery_timeout: float = 10
    cleanup_timeout: float = 2


POLICY = Policy()


def timeout(value, inherited=None):
    if value is INHERIT:
        return inherited
    if value is not None and (
        isinstance(value, bool)
        or not isinstance(value, (float, int))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise RimeInputError("timeout must be finite positive seconds or None")
    return value


def nonempty(value, name):
    if not isinstance(value, str) or not value.strip():
        raise RimeInputError(f"{name} must be a non-empty string")
    return value
