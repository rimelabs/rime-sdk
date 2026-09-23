"""Python public interface and event-loop ownership."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterable
from typing import Self

from . import _native
from ._audio import AudioFormat
from ._bridge import INHERIT, call, encode_options, translate
from ._errors import RimeAudioFormatError, RimeAuthenticationError, RimeInputError
from ._stream import _CONSTRUCTION_KEY, AudioStream

_native_factory = _native.NativeClient


class Rime:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "coda",
        endpoint: str | None = None,
        timeout: float | None = None,
    ):
        key = os.getenv("RIME_API_KEY") if api_key is None else api_key
        if not isinstance(key, str) or not key.strip():
            raise RimeAuthenticationError("Provide api_key or set RIME_API_KEY")
        if endpoint is not None and not isinstance(endpoint, str):
            raise RimeInputError("endpoint must be a hostname")
        self._native = call(
            _native_factory,
            encode_options(
                {"api_key": key, "model": model, "endpoint": endpoint, "timeout": timeout}
            ),
        )
        self._pid = os.getpid()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        self._streams: set[AudioStream] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.tts = _TTS(self)
        self.voices = _Voices(self)
        self.languages = _Languages(self)

    def _check_open(self):
        if self._closed:
            raise RimeInputError("The Rime client is closed")

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._pid != os.getpid() or self._loop not in (None, loop):
            raise RimeInputError("A Rime client belongs to one process and event loop")
        self._loop = loop

    async def _discover(self, voices, language, timeout):
        self._check_loop()
        self._check_open()
        if language is not None and (not isinstance(language, str) or not language.strip()):
            raise RimeInputError("language must be a non-empty string")
        if (
            timeout is not INHERIT
            and timeout is not None
            and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)))
        ):
            raise RimeInputError("timeout must be a number or None")
        try:
            return await self._native.discover(
                voices, language, None if timeout is INHERIT else timeout, timeout is INHERIT
            )
        except ValueError as error:
            raise translate(error) from None

    async def _shutdown(self):
        self._native.cancel()
        await asyncio.gather(*(stream.cancel() for stream in list(self._streams)))
        await self._native.close()

    async def close(self) -> None:
        self._check_loop()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._shutdown())
        await asyncio.shield(self._close_task)

    async def __aenter__(self) -> Self:
        self._check_loop()
        self._check_open()
        return self

    async def __aexit__(self, *_):
        await self.close()


class _TTS:
    def __init__(self, client):
        self._client = client

    def stream(
        self,
        text: str | AsyncIterable[str],
        *,
        voice: str | None = None,
        language: str = "en",
        audio_format: AudioFormat | None = None,
        timeout: float | None | object = INHERIT,
    ) -> AudioStream:
        self._client._check_open()
        if isinstance(text, str):
            if not text.strip():
                raise RimeInputError("Text must contain non-whitespace characters")
        elif not hasattr(text, "__aiter__"):
            raise RimeInputError("text must be a string or an async iterable of strings")
        profile = AudioFormat.PCM_24000 if audio_format is None else audio_format
        if not isinstance(profile, AudioFormat):
            raise RimeAudioFormatError("Select a named AudioFormat profile")
        options = {
            "voice": "clementine" if voice is None else voice,
            "language": language,
            "profile": profile.name,
            "timeout": None if timeout is INHERIT else timeout,
            "inherit_timeout": timeout is INHERIT,
        }
        native = call(self._client._native.stream, encode_options(options))
        stream = AudioStream(_CONSTRUCTION_KEY, self._client, native, text, profile)
        self._client._streams.add(stream)
        return stream


class _Voices:
    def __init__(self, client):
        self._client = client

    async def list(self, language: str | None = None, *, timeout=INHERIT) -> list[str]:
        return await self._client._discover(True, language, timeout)


class _Languages:
    def __init__(self, client):
        self._client = client

    async def list(self, *, timeout=INHERIT) -> list[str]:
        return await self._client._discover(False, None, timeout)
