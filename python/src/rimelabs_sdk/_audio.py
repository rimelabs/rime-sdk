"""Public audio profiles and private native converter adapter."""

from enum import Enum

from . import _native
from ._bridge import call


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


class Converter:
    def __init__(self, profile):
        self._native = call(_native.Converter, profile.name)

    def process(self, data, *, final=False):
        return call(self._native.process, data, final)
