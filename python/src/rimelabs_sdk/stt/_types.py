"""Public transcription options, replacement snapshots, and append fragments."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, TypeAlias


class TranscriptionOutputContract(str, Enum):
    """Whether updates replace provisional text or append permanent fragments."""

    REVISED_HYPOTHESES = "revised_hypotheses"
    APPEND_FRAGMENTS = "append_fragments"


@dataclass(frozen=True, kw_only=True)
class TranscriptionDelta:
    """A permanent fragment. Append text exactly, including server separators."""

    text: str
    kind: Literal["delta"] = field(default="delta", init=False)


class TranscriptionMode(str, Enum):
    """Formatting intent; it steers recognition without guaranteeing exact tokens."""

    WRITTEN = "written"
    """Prefer written forms, for example 'flight 247'."""
    VERBATIM = "verbatim"
    """Prefer spoken forms, including fillers and spelled-out numbers."""


@dataclass(frozen=True, kw_only=True)
class TranscriptionPartial:
    """A replacement snapshot; later updates can revise earlier words."""

    text: str
    """The complete current transcript, never an append-only delta."""
    kind: Literal["partial"] = field(default="partial", init=False)
    """Discriminator for a revisable result."""


@dataclass(frozen=True, kw_only=True)
class TranscriptionFinal:
    """The completed transcript after successful protocol and transport completion."""

    text: str
    """The complete final transcript; silence can produce an empty string."""
    language: str
    """Canonical BCP-47 language selected by the service, for example 'en'."""
    kind: Literal["final"] = field(default="final", init=False)
    """Discriminator for a successfully completed utterance."""


TranscriptionUpdate: TypeAlias = TranscriptionPartial | TranscriptionDelta | TranscriptionFinal


@dataclass(frozen=True, kw_only=True)
class Transcription:
    """Completed WAV transcription, including the service request identity."""

    text: str
    language: str
    request_id: str | None = None
