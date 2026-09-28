"""Reusable async Rime client and public operation namespaces."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterable
from typing import Any, Self

import grpc

from . import _policy, _transport
from ._audio import AudioFormat
from ._auth import Credentials
from ._errors import (
    RimeAudioFormatError,
    RimeAuthenticationError,
    RimeCancelledError,
    RimeInputError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from ._stream import _CONSTRUCTION_KEY, AudioStream


class Rime:
    """Speech and discovery for model='coda' or model='mistv3'."""

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
        self._timeout = _policy.timeout(timeout)
        self._policy = _policy.resolve(model, endpoint)
        self._credentials = Credentials(key, self._policy)
        self._channel: grpc.aio.Channel | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pid = os.getpid()
        self._closed = False
        self._streams: set[AudioStream] = set()
        self._discovery_tasks: set[asyncio.Task[Any]] = set()
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

    async def _prepare(self):
        self._check_loop()
        self._check_open()
        metadata = await self._credentials.metadata()
        self._check_open()
        if self._channel is None:
            self._channel = _transport.make_channel(self._policy)
        try:
            async with asyncio.timeout(self._policy.connection_timeout):
                await self._channel.channel_ready()
        except TimeoutError:
            raise RimeTimeoutError("Connection establishment timed out") from None
        return self._channel, metadata

    async def _discover(self, kind, language, timeout):
        self._check_loop()
        self._check_open()
        budget = self._policy.discovery_timeout
        if timeout is not None:
            budget = min(budget, timeout)

        async def run():
            request_id = None
            loop = asyncio.get_running_loop()
            deadline = loop.time() + budget
            try:
                async with asyncio.timeout_at(deadline):
                    channel, metadata = await self._prepare()
                for attempt in range(3):
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError
                    request_id = None
                    try:
                        return await _transport.discover(
                            channel, metadata, kind, language, remaining
                        )
                    except RimeUnavailableError as error:
                        request_id = error.request_id
                        if attempt == 2:
                            raise
                        async with asyncio.timeout_at(deadline):
                            await asyncio.sleep(0.05 * 2**attempt)
            except TimeoutError:
                raise RimeTimeoutError(
                    "Discovery deadline expired", request_id=request_id
                ) from None

        task = asyncio.create_task(run(), name="rime:discovery")
        self._discovery_tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            if self._closed:
                raise RimeCancelledError("Client closed during discovery") from None
            raise
        finally:
            self._discovery_tasks.discard(task)

    async def _shutdown(self):
        await asyncio.gather(*(stream.cancel() for stream in list(self._streams)))
        for task in self._discovery_tasks:
            task.cancel()
        await asyncio.gather(*self._discovery_tasks, return_exceptions=True)
        await self._credentials.close()
        if self._channel:
            await self._channel.close()

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
        timeout: float | None | object = _policy.INHERIT,
    ) -> AudioStream:
        self._client._check_open()
        if isinstance(text, str):
            if not text.strip():
                raise RimeInputError("Text must contain non-whitespace characters")
        elif not hasattr(text, "__aiter__"):
            raise RimeInputError("text must be a string or an async iterable of strings")
        voice = (
            self._client._policy.default_voice
            if voice is None
            else _policy.nonempty(voice, "voice")
        )
        _policy.nonempty(language, "language")
        profile = AudioFormat.PCM_24000 if audio_format is None else audio_format
        if not isinstance(profile, AudioFormat):
            raise RimeAudioFormatError("Select a named AudioFormat profile")
        stream = AudioStream(
            _CONSTRUCTION_KEY,
            self._client,
            text,
            voice,
            language,
            profile,
            _policy.timeout(timeout, self._client._timeout),
        )
        self._client._streams.add(stream)
        return stream


class _Voices:
    def __init__(self, client):
        self._client = client

    async def list(self, language: str | None = None, *, timeout=_policy.INHERIT) -> list[str]:
        if language is not None:
            _policy.nonempty(language, "language")
        return await self._client._discover(
            "voices", language, _policy.timeout(timeout, self._client._timeout)
        )


class _Languages:
    def __init__(self, client):
        self._client = client

    async def list(self, *, timeout=_policy.INHERIT) -> list[str]:
        return await self._client._discover(
            "languages", None, _policy.timeout(timeout, self._client._timeout)
        )
