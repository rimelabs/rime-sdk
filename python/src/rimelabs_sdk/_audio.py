"""Raw PCM alignment and a shared streaming FIR / G.711 conversion path."""

import math
import struct
from collections import deque
from enum import Enum

from ._errors import RimeAudioFormatError


class AudioFormat(Enum):
    PCM_24000 = ("pcm_s16le", 24000, 1)
    MULAW_8000 = ("mulaw", 8000, 1)

    @property
    def encoding(self):
        return self.value[0]

    @property
    def sample_rate(self):
        return self.value[1]

    @property
    def channels(self):
        return self.value[2]


# 63-tap low-pass filter, 3.4 kHz cutoff, Hamming window. Same coefficients in Node.
_TAPS = 63
_raw = [
    (
        2 * 3400 / 24000
        if i == 31
        else math.sin(2 * math.pi * 3400 / 24000 * (i - 31)) / (math.pi * (i - 31))
    )
    * (0.54 - 0.46 * math.cos(2 * math.pi * i / 62))
    for i in range(_TAPS)
]
COEFFICIENTS = tuple(x / sum(_raw) for x in _raw)


def _mulaw(sample):
    sample = max(-32768, min(32767, sample))
    sign = 0x80 if sample < 0 else 0
    # G.711 uses one's complement for negative PCM before quantization.
    magnitude = min(~sample if sample < 0 else sample, 32635) + 132
    exponent = max(0, magnitude.bit_length() - 8)
    mantissa = (magnitude >> (exponent + 3)) & 15
    return ~(sign | (exponent << 4) | mantissa) & 255


class Converter:
    def __init__(self, profile):
        self._profile = profile
        self._tail = b""
        self._samples = deque([0.0] * _TAPS, maxlen=_TAPS)
        self._seen = 0
        self._emitted = 0
        self._input_samples = 0

    def _sample(self, sample):
        self._samples.appendleft(sample)
        position = self._seen - 31
        self._seen += 1
        if position >= 0 and position % 3 == 0:
            filtered = sum(a * b for a, b in zip(COEFFICIENTS, self._samples))
            self._emitted += 1
            return _mulaw(math.floor(filtered + 0.5))
        return None

    def process(self, data, *, final=False):
        data = self._tail + data
        size = len(data) // 2 * 2
        self._tail = data[size:]
        if final and self._tail:
            raise RimeAudioFormatError("Incomplete final PCM sample frame")
        if self._profile is AudioFormat.PCM_24000:
            return data[:size]
        result = bytearray()
        for (sample,) in struct.iter_unpack("<h", data[:size]):
            self._input_samples += 1
            value = self._sample(sample)
            if value is not None:
                result.append(value)
        if final:
            target = (self._input_samples + 2) // 3
            while self._emitted < target:
                value = self._sample(0)
                if value is not None:
                    result.append(value)
        return bytes(result)
