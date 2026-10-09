"""Private policy. The Themis wire contract is an implementation assumption."""

from dataclasses import dataclass, replace

from .._errors import RimeInputError
from .._validation import INHERIT, endpoint_address, timeout

__all__ = ["INHERIT", "POLICY", "Policy", "nonempty", "resolve", "timeout"]

_CODA_HOSTNAME = "coda.api.rime.ai"


@dataclass(frozen=True)
class Policy:
    target: str = f"{_CODA_HOSTNAME}:50051"
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
            POLICY, target=f"{hostname}:50051", audience=hostname, default_voice="astra"
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
