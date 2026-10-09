"""Public transcription options and replacement transcript snapshots."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, TypeAlias


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


TranscriptionUpdate: TypeAlias = TranscriptionPartial | TranscriptionFinal
