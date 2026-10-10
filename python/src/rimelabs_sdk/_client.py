"""Shared credentials and lifetime for synthesis, recognition, and realtime conversations."""

from __future__ import annotations

import asyncio
import os
from typing import Self

from ._auth import Credentials
from ._errors import RimeAuthenticationError, RimeInputError
from .realtime import Realtime
from .stt._client import STT
from .tts._client import _TTS, _Languages, _Voices


class Rime:
    """Async TTS, streaming recognition, voice discovery, and realtime conversations."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "coda",
        endpoint: str | None = None,
        timeout: float | None = None,
        stt_endpoint: str | None = None,
    ):
        key = os.getenv("RIME_API_KEY") if api_key is None else api_key
        if not isinstance(key, str) or not key.strip():
            raise RimeAuthenticationError("Provide api_key or set RIME_API_KEY")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pid = os.getpid()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self.tts = _TTS(self, model=model, endpoint=endpoint, timeout=timeout)
        self._credentials = Credentials(key, self.tts._policy)
        self.realtime = Realtime(self)
        self.stt = STT(self, stt_endpoint)
        self.voices = _Voices(self.tts)
        self.languages = _Languages(self.tts)

    def _check_open(self):
        if self._closed:
            raise RimeInputError("The Rime client is closed")

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._pid != os.getpid() or self._loop not in (None, loop):
            raise RimeInputError("A Rime client belongs to one process and event loop")
        self._loop = loop

    async def _shutdown(self):
        try:
            await self.realtime._close()
        finally:
            try:
                await self.stt._close()
            finally:
                try:
                    await self.tts._close()
                finally:
                    await self._credentials.close()

    async def close(self) -> None:
        self._check_loop()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._shutdown(), name="rime:close")
        await asyncio.shield(self._close_task)

    async def __aenter__(self) -> Self:
        self._check_loop()
        self._check_open()
        return self

    async def __aexit__(self, *_):
        await self.close()
