"""Speech recognition operations and their independent lazy gRPC connection."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Sequence
from typing import TYPE_CHECKING

import grpc

from .._errors import RimeAudioFormatError, RimeInputError, RimeTimeoutError
from .._pcm import PCMFormat
from .._validation import timeout as validate_timeout
from . import _policy, _transport
from ._stream import TranscriptStream
from ._types import TranscriptionMode

if TYPE_CHECKING:
    from .._client import Rime


class STT:
    def __init__(self, client: Rime, endpoint: str | None):
        self._client = client
        self._endpoint = endpoint
        self._channel: grpc.aio.Channel | None = None
        self._streams: set[TranscriptStream] = set()

    def stream(
        self,
        audio: AsyncIterable[bytes],
        *,
        language: str,
        mode: TranscriptionMode = TranscriptionMode.WRITTEN,
        context_terms: Sequence[str] = (),
        input_format: PCMFormat | None = None,
        timeout: float | None = None,
    ) -> TranscriptStream:
        """Transcribe one caller-ended utterance; source exhaustion commits input.

        Audio is headerless PCM16 little-endian, mono 16 kHz by default. Partial
        results replace prior text. Language and recognition terms pass unchanged
        to the service. Timeout is an optional overall deadline in seconds.
        """
        self._client._check_open()
        if not hasattr(audio, "__aiter__"):
            raise RimeInputError("audio must be an async iterable of bytes")
        if not isinstance(language, str):
            raise RimeInputError("language must be a string")
        if not isinstance(mode, TranscriptionMode):
            raise RimeInputError("mode must be a TranscriptionMode")
        if isinstance(context_terms, (str, bytes)) or not isinstance(context_terms, Sequence):
            raise RimeInputError("context_terms must be a sequence of strings")
        if any(not isinstance(term, str) for term in context_terms):
            raise RimeInputError("context_terms must contain strings")
        if input_format is not None and not isinstance(input_format, PCMFormat):
            raise RimeAudioFormatError("input_format must be a PCMFormat")
        operation = TranscriptStream(
            self,
            audio,
            language,
            mode,
            tuple(context_terms),
            input_format or PCMFormat(),
            validate_timeout(timeout),
            _policy.resolve(self._endpoint),
        )
        self._streams.add(operation)
        return operation

    async def _prepare(self, policy):
        self._client._check_loop()
        self._client._check_open()
        _transport.schema()
        metadata = (("authorization", self._client._credentials.authorization()),)
        if self._channel is None:
            self._channel = _transport.make_channel(policy)
        try:
            async with asyncio.timeout(policy.connection_timeout):
                await self._channel.channel_ready()
        except TimeoutError:
            raise RimeTimeoutError("STT connection establishment timed out") from None
        return self._channel, metadata

    async def _close(self):
        try:
            results = await asyncio.gather(
                *(operation.cancel() for operation in list(self._streams)), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            if self._channel:
                await self._channel.close()
