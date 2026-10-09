# SPDX-License-Identifier: Apache-2.0
"""PCM input values and stateful conversion to signed PCM16 little-endian mono 16 kHz."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from ._errors import RimeAudioFormatError


@dataclass(frozen=True, kw_only=True)
class PCMFormat:
    """Headerless signed PCM16 little-endian input, interleaved when stereo."""

    sample_rate: Literal[8000, 16000, 24000, 48000] = 16000
    """Input frames per second; conversion produces 16 kHz mono."""
    channels: Literal[1, 2] = 1
    """One sample per frame for mono, two interleaved samples for stereo."""
    encoding: Literal["pcm_s16le"] = "pcm_s16le"
    """Signed 16-bit little-endian samples without a file header."""

    def __post_init__(self):
        if (
            type(self.sample_rate) is not int
            or self.sample_rate not in (8000, 16000, 24000, 48000)
            or type(self.channels) is not int
            or self.channels not in (1, 2)
            or self.encoding != "pcm_s16le"
        ):
            raise RimeAudioFormatError("Use PCM16 at 8, 16, 24 or 48 kHz, with one or two channels")


class InputConverter:
    """Convert complete frames; each operation owns independent resampling state."""

    def __init__(self, format: PCMFormat):
        self.format = format
        self._state: Any = None

    def clone(self) -> InputConverter:
        """Snapshot immutable resampling state before an interruptible send."""
        copy = InputConverter(self.format)
        copy._state = self._state
        return copy

    def process(self, data: bytes) -> bytes:
        import audioop

        if self.format.channels == 2:
            data = audioop.tomono(data, 2, 0.5, 0.5)
        if self.format.sample_rate != 16000:
            data, self._state = audioop.ratecv(
                data, 2, 1, self.format.sample_rate, 16000, self._state
            )
        return data
