"""Private policy. The Themis wire contract is an implementation assumption."""

import math
import re
from dataclasses import dataclass, replace

from ._errors import RimeInputError

INHERIT = object()
_CODA_HOSTNAME = "coda.api.rime.ai"


@dataclass(frozen=True)
class Policy:
    target: str = f"{_CODA_HOSTNAME}:443"
    audience: str = _CODA_HOSTNAME
    default_voice: str = "clementine"
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


def resolve(model: str, endpoint: str | None) -> Policy:
    if model == "coda":
        deployment = replace(POLICY)
    elif model == "mistv3":
        hostname = "mist.api.rime.ai"
        deployment = replace(
            POLICY, target=f"{hostname}:443", audience=hostname, default_voice="astra"
        )
    else:
        raise RimeInputError("model must be 'coda' or 'mistv3'")
    if endpoint is None:
        return deployment
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
    return replace(deployment, target=f"{host}:{int(port) if separator else 443}", audience=host)


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
