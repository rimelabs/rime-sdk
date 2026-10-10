"""TTS configuration, streaming and discovery, owned by one Rime client."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Any

import grpc

from .._errors import (
    RimeAudioFormatError,
    RimeCancelledError,
    RimeInputError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from . import _policy, _transport
from ._audio import AudioFormat
from ._stream import _CONSTRUCTION_KEY, AudioStream

if TYPE_CHECKING:
    from .._client import Rime


class _TTS:
    def __init__(self, client: Rime, *, model: str, endpoint: str | None, timeout: float | None):
        self._client = client
        self._timeout = _policy.timeout(timeout)
        self._policy = _policy.resolve(model, endpoint)
        self._channel: grpc.aio.Channel | None = None
        self._streams: set[AudioStream] = set()
        self._discovery_tasks: set[asyncio.Task[Any]] = set()

    def _check_open(self):
        self._client._check_open()

    def _check_loop(self):
        self._client._check_loop()

    def synthesize(
        self,
        text: str,
        *,
        voice: str | None = None,
        language: str = "en",
        audio_format: AudioFormat | None = None,
        timeout: float | None | object = _policy.INHERIT,
    ) -> AudioStream:
        """Synthesize complete text and return an audio stream. Do not await this call."""
        self._check_open()
        if not isinstance(text, str):
            raise RimeInputError("text must be a string; use stream() for an async text source")
        if not text.strip():
            raise RimeInputError("Text must contain non-whitespace characters")

        async def source():
            yield text

        return self.stream(
            source(), voice=voice, language=language, audio_format=audio_format, timeout=timeout
        )

    def stream(
        self,
        text: AsyncIterable[str],
        *,
        voice: str | None = None,
        language: str = "en",
        audio_format: AudioFormat | None = None,
        timeout: float | None | object = _policy.INHERIT,
    ) -> AudioStream:
        """Stream text chunks into synthesis and return an audio stream. Do not await this call."""
        self._check_open()
        if isinstance(text, str) or not hasattr(text, "__aiter__"):
            raise RimeInputError(
                "text must be an async iterable of strings; use synthesize() for a string"
            )
        voice = self._policy.default_voice if voice is None else _policy.nonempty(voice, "voice")
        _policy.nonempty(language, "language")
        profile = AudioFormat.PCM_24000 if audio_format is None else audio_format
        if not isinstance(profile, AudioFormat):
            raise RimeAudioFormatError("Select a named AudioFormat profile")
        stream = AudioStream(
            _CONSTRUCTION_KEY,
            self,
            text,
            voice,
            language,
            profile,
            _policy.timeout(timeout, self._timeout),
        )
        self._streams.add(stream)
        return stream

    async def _prepare(self):
        self._check_loop()
        self._check_open()
        metadata = await self._client._credentials.metadata()
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
            if self._client._closed:
                raise RimeCancelledError("Client closed during discovery") from None
            raise
        finally:
            self._discovery_tasks.discard(task)

    async def _close(self):
        await asyncio.gather(*(stream.cancel() for stream in list(self._streams)))
        for task in self._discovery_tasks:
            task.cancel()
        await asyncio.gather(*self._discovery_tasks, return_exceptions=True)
        if self._channel:
            await self._channel.close()


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
