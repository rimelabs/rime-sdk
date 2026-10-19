"""TTS configuration, streaming and discovery, owned by one Rime client."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import grpc

from .._errors import (
    RimeCancelledError,
    RimeError,
    RimeInputError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from . import _policy, _transport
from ._audio import AudioFormat, resolve_audio
from ._buffer import MAX_BUFFER_BYTES, finalize_wav
from ._lexicon import PronunciationEntry, snapshot
from ._stream import _CONSTRUCTION_KEY, AudioStream

if TYPE_CHECKING:
    from .._client import Rime


class _TTS:
    def __init__(
        self, client: Rime, *, model: str, endpoint: str | None, timeout: float | timedelta | None
    ):
        self._client = client
        self._model = model
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
        sample_rate: int | None = None,
        time_scale_factor: float | None = None,
        timestamps: bool = False,
        custom_lexicon: Sequence[PronunciationEntry] = (),
        timeout: float | timedelta | None | object = _policy.INHERIT,
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
            source(),
            voice=voice,
            language=language,
            audio_format=audio_format,
            sample_rate=sample_rate,
            time_scale_factor=time_scale_factor,
            timestamps=timestamps,
            custom_lexicon=custom_lexicon,
            timeout=timeout,
        )

    async def synthesize_to_buffer(
        self,
        text: str,
        *,
        voice: str | None = None,
        language: str = "en",
        audio_format: AudioFormat | None = None,
        sample_rate: int | None = None,
        time_scale_factor: float | None = None,
        timestamps: bool = False,
        custom_lexicon: Sequence[PronunciationEntry] = (),
        timeout: float | timedelta | None | object = _policy.INHERIT,
    ) -> bytes:
        """Await complete audio, finalizing WAV lengths. Never returns partial audio.

        The fixed output limit is 64 MiB (67,108,864 bytes). Exceeding it raises
        RimeResourceLimitError. Use streaming synthesis for larger outputs or timestamps.
        """
        stream = self.synthesize(
            text,
            voice=voice,
            language=language,
            audio_format=audio_format,
            sample_rate=sample_rate,
            time_scale_factor=time_scale_factor,
            timestamps=timestamps,
            custom_lexicon=custom_lexicon,
            timeout=timeout,
        )
        data = bytearray()
        try:
            async for chunk in stream:
                if len(chunk) > MAX_BUFFER_BYTES - len(data):
                    raise RimeResourceLimitError("Audio exceeds the 64 MiB buffer limit")
                data.extend(chunk)
            if stream.format is AudioFormat.WAV:
                finalize_wav(data, stream.sample_rate)
            return bytes(data)
        except RimeError as error:
            error.request_id = stream.request_id
            raise
        finally:
            await stream.cancel()

    def stream(
        self,
        text: AsyncIterable[str],
        *,
        voice: str | None = None,
        language: str = "en",
        audio_format: AudioFormat | None = None,
        sample_rate: int | None = None,
        time_scale_factor: float | None = None,
        timestamps: bool = False,
        custom_lexicon: Sequence[PronunciationEntry] = (),
        timeout: float | timedelta | None | object = _policy.INHERIT,
    ) -> AudioStream:
        """Stream text chunks into synthesis and return an audio stream. Do not await this call."""
        self._check_open()
        if isinstance(text, str) or not hasattr(text, "__aiter__"):
            raise RimeInputError(
                "text must be an async iterable of strings; use synthesize() for a string"
            )
        lexicon = snapshot(custom_lexicon)
        if not isinstance(timestamps, bool):
            raise RimeInputError("timestamps must be a boolean")
        if timestamps and self._model != "mistv3":
            raise RimeInputError("Word timestamps are supported only with model='mistv3'")
        voice = self._policy.default_voice if voice is None else _policy.nonempty(voice, "voice")
        _policy.nonempty(language, "language")
        profile, sample_rate = resolve_audio(audio_format, sample_rate, time_scale_factor)
        stream = AudioStream(
            _CONSTRUCTION_KEY,
            self,
            text,
            voice,
            language,
            profile,
            _policy.timeout(timeout, self._timeout),
            timestamps,
            lexicon,
            sample_rate,
            time_scale_factor,
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

    async def list(
        self,
        language: str | None = None,
        *,
        timeout: float | timedelta | None | object = _policy.INHERIT,
    ) -> list[str]:
        if language is not None:
            _policy.nonempty(language, "language")
        return await self._client._discover(
            "voices", language, _policy.timeout(timeout, self._client._timeout)
        )


class _Languages:
    def __init__(self, client):
        self._client = client

    async def list(
        self, *, timeout: float | timedelta | None | object = _policy.INHERIT
    ) -> list[str]:
        return await self._client._discover(
            "languages", None, _policy.timeout(timeout, self._client._timeout)
        )
