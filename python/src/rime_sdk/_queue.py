import asyncio
from collections import deque


class ByteQueue:
    def __init__(self, limit):
        self.limit = limit
        self.size = 0
        self.items: deque[bytes] = deque()
        self.changed = asyncio.Event()
        self.done = False
        self.error = None

    async def put(self, data):
        while not self.done and self.size + len(data) > self.limit:
            self.changed.clear()
            await self.changed.wait()
        if self.done:
            return
        self.items.append(data)
        self.size += len(data)
        self.changed.set()

    async def get(self):
        while not self.items and not self.done:
            self.changed.clear()
            await self.changed.wait()
        if self.error:
            raise self.error
        if self.items:
            item = self.items.popleft()
            self.size -= len(item)
            self.changed.set()
            return item
        raise StopAsyncIteration

    def finish(self, error=None):
        self.done = True
        self.error = error
        if error:
            self.items.clear()
            self.size = 0
        self.changed.set()
