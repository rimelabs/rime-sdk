"""Bounded audio output, including chunk sizing and producer completion.

finish() allows queued audio to drain. fail() discards it and keeps the first
error, even after finish(). Subsequent puts discard their input. The producer
awaits its last put before finish(). AudioStream owns the later point at which
the consumer observes successful completion.
"""

import asyncio
from collections import deque


class ByteQueue:
    def __init__(self, limit: int, chunk_size: int):
        if limit <= 0 or chunk_size <= 0:
            raise ValueError("Audio queue limits must be positive")
        self._limit = limit
        self._chunk_size = min(chunk_size, limit)
        self._size = 0
        self._items: deque[bytes] = deque()
        self._changed = asyncio.Event()
        self._closed = False
        self._error: Exception | None = None
        self._writers = 0

    @property
    def size(self) -> int:
        return self._size

    @property
    def has_pending_output(self) -> bool:
        """Output is queued or a producer has not finished submitting it."""
        return self._size > 0 or self._writers > 0

    async def put(self, data: bytes) -> None:
        self._writers += 1
        try:
            for offset in range(0, len(data), self._chunk_size):
                part = data[offset : offset + self._chunk_size]
                while not self._closed and self._size + len(part) > self._limit:
                    self._changed.clear()
                    await self._changed.wait()
                if self._closed:
                    return
                self._items.append(part)
                self._size += len(part)
                self._changed.set()
        finally:
            self._writers -= 1

    async def get(self) -> bytes:
        while not self._items and not self._closed:
            self._changed.clear()
            await self._changed.wait()
        if self._error is not None:
            raise self._error
        if self._items:
            item = self._items.popleft()
            self._size -= len(item)
            self._changed.set()
            return item
        raise StopAsyncIteration

    def finish(self) -> None:
        self._closed = True
        self._changed.set()

    def fail(self, error: Exception) -> None:
        if self._error is None:
            self._error = error
        self._items.clear()
        self._size = 0
        self.finish()
