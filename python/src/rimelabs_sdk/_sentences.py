"""Private compatibility adapter for the shared detector tests."""

from . import _native
from ._bridge import call


class SentenceBuffer:
    def __init__(self, limit):
        self._native = _native.SentenceBuffer(limit)

    def feed(self, fragment, *, final=False):
        return iter(call(self._native.feed, fragment, final))

    @property
    def retained_bytes(self):
        return self._native.retained_bytes

    @property
    def scans(self):
        return self._native.scans
