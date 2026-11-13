"""Private policy. The Themis wire contract is an implementation assumption."""

from dataclasses import dataclass, replace

from .._errors import RimeInputError
from .._validation import INHERIT, endpoint_address, timeout

__all__ = ["INHERIT", "POLICY", "Policy", "nonempty", "resolve", "timeout"]

_CODA_HOSTNAME = "coda.api.rime.ai"
_MIST_HOSTNAME = "mist.api.rime.ai"
HOSTED_TARGETS = frozenset(f"{hostname}:443" for hostname in (_CODA_HOSTNAME, _MIST_HOSTNAME))


@dataclass(frozen=True)
class Policy:
    target: str = f"{_CODA_HOSTNAME}:443"
    audience: str = _CODA_HOSTNAME
    default_voice: str = "clementine"
    exchange_url: str = "https://themis.api.rime.ai/v1/token"
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
    enable_keepalive: bool = False


POLICY = Policy()


def resolve(model: str, endpoint: str | None, enable_keepalive: bool = False) -> Policy:
    if not isinstance(enable_keepalive, bool):
        raise RimeInputError("enable_tts_keepalive must be a boolean")
    if model == "coda":
        deployment = replace(POLICY, enable_keepalive=enable_keepalive)
    elif model == "mistv3":
        deployment = replace(
            POLICY,
            target=f"{_MIST_HOSTNAME}:443",
            audience=_MIST_HOSTNAME,
            default_voice="astra",
            enable_keepalive=enable_keepalive,
        )
    else:
        raise RimeInputError("model must be 'coda' or 'mistv3'")
    if endpoint is None:
        return deployment
    target, audience = endpoint_address(endpoint)
    return replace(deployment, target=target, audience=audience)


def nonempty(value, name):
    if not isinstance(value, str) or not value.strip():
        raise RimeInputError(f"{name} must be a non-empty string")
    return value
