"""Request-scoped pronunciation overrides; linguistic validation belongs to the service."""

from collections.abc import Sequence
from dataclasses import dataclass

from .._errors import RimeInputError


@dataclass(frozen=True)
class PronunciationEntry:
    """A word or phrase and its space-separated X-SAMPA pronunciation."""

    spelling: str
    pronunciation: str


def snapshot(entries: Sequence[PronunciationEntry]) -> tuple[PronunciationEntry, ...]:
    if isinstance(entries, (str, bytes)) or not isinstance(entries, Sequence):
        raise RimeInputError("custom_lexicon must be a sequence of PronunciationEntry values")
    result = tuple(entries)
    for entry in result:
        if not isinstance(entry, PronunciationEntry):
            raise RimeInputError("custom_lexicon must contain PronunciationEntry values")
        if not isinstance(entry.spelling, str) or not isinstance(entry.pronunciation, str):
            raise RimeInputError("Lexicon spelling and pronunciation must be strings")
    return result
