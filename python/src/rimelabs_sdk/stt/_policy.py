"""Independent transport and resource policy for speech recognition."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Policy:
    """Transport and memory bounds; timeout values are durations in seconds."""

    target: str = "stt.api.rime.ai:443"
    connection_timeout: float = 10
    acceptance_timeout: float = 10
    completion_timeout: float = 120
    cleanup_timeout: float = 2
    receive_bytes: int = 262144
    transcript_bytes: int = 65536
    queued_updates: int = 16


POLICY = Policy()
