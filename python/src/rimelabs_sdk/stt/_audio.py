"""Prepare one utterance as headerless PCM16 little-endian mono 16 kHz messages."""

from collections.abc import Iterator

from .._errors import RimeAudioFormatError, RimeInputError
from .._pcm import InputConverter, PCMFormat

# Bound conversion work and temporary allocations even for a large source chunk.
_SOURCE_BYTES = 16384
# Maximum decoded audio payload per STT message, excluding the protobuf envelope.
_MESSAGE_BYTES = 65536


class InputAudio:
    """Carry incomplete frames across source chunks; never pad or truncate audio.

    One instance belongs to one utterance. Fully consume each feed iterator before
    feeding the next chunk. Each yield is a complete wire payload; advancing the
    iterator converts only the next bounded portion of source audio.
    """

    def __init__(self, format: PCMFormat | None = None):
        format = PCMFormat() if format is None else format
        self._converter = InputConverter(format)
        self._frame_bytes = 2 * format.channels
        self._pending = b""
        self._finished = False

    def feed(self, data: bytes) -> Iterator[bytes]:
        if self._finished:
            raise RimeInputError("The audio input is finished")
        if not isinstance(data, bytes):
            raise RimeAudioFormatError("The audio source must yield PCM16 bytes")
        for offset in range(0, len(data), _SOURCE_BYTES):
            part = self._pending + data[offset : offset + _SOURCE_BYTES]
            complete = len(part) - len(part) % self._frame_bytes
            self._pending = part[complete:]
            if complete:
                converted = self._converter.process(part[:complete])
                for start in range(0, len(converted), _MESSAGE_BYTES):
                    yield converted[start : start + _MESSAGE_BYTES]

    def finish(self) -> None:
        """Check EOF before committing an utterance; a truncated frame is invalid."""
        self._finished = True
        if self._pending:
            raise RimeAudioFormatError("The audio source ended with an incomplete PCM16 frame")
