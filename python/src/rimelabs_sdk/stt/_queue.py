"""Bounded, atomic transcript events with terminal failure waking both sides."""

import asyncio
from collections import deque

from ._types import TranscriptionUpdate


class TranscriptQueue:
    def __init__(self, limit: int):
        self._limit = limit
        self._items: deque[TranscriptionUpdate] = deque()
        self._changed = asyncio.Event()
        self._closed = False
        self._error: Exception | None = None

    async def put(self, event: TranscriptionUpdate) -> None:
        while len(self._items) >= self._limit and not self._closed:
            self._changed.clear()
            await self._changed.wait()
        if not self._closed:
            self._items.append(event)
            self._changed.set()

    async def get(self) -> TranscriptionUpdate:
        while not self._items and not self._closed:
            self._changed.clear()
            await self._changed.wait()
        if self._error:
            raise self._error
        if not self._items:
            raise StopAsyncIteration
        result = self._items.popleft()
        self._changed.set()
        return result

    def finish(self) -> None:
        self._closed = True
        self._changed.set()

    def fail(self, error: Exception) -> None:
        self._error = self._error or error
        self._items.clear()
        self.finish()
