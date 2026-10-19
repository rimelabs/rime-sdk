"""Speech recognition operations and their independent lazy gRPC connection."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING

import grpc

from .._errors import (
    RimeAudioFormatError,
    RimeCancelledError,
    RimeInputError,
    RimeResourceLimitError,
    RimeTimeoutError,
)
from .._pcm import PCMFormat
from .._validation import timeout as validate_timeout
from . import _policy, _transport
from ._stream import TranscriptStream
from ._types import Transcription, TranscriptionMode

if TYPE_CHECKING:
    from .._client import Rime


class STT:
    def __init__(self, client: Rime):
        self._client = client
        self._channel: grpc.aio.Channel | None = None
        self._streams: set[TranscriptStream] = set()
        self._requests: set[asyncio.Task] = set()

    async def transcribe(
        self,
        audio: bytes,
        *,
        language: str,
        mode: TranscriptionMode = TranscriptionMode.WRITTEN,
        context_terms: Sequence[str] = (),
        timeout: float | None = None,
    ) -> Transcription:
        """Transcribe a complete WAV file (at most 16 MiB), without retries.

        PCM16 mono 16 kHz WAV is the portable input format. Audio bytes are sent
        unchanged; the service validates the file. TTS defaults do not apply.
        The optional overall timeout includes connecting and upload.
        """
        self._client._check_loop()
        self._client._check_open()
        if not isinstance(audio, bytes) or not audio:
            raise RimeAudioFormatError("audio must contain complete WAV bytes")
        if len(audio) > _transport.MAX_WAV_BYTES:
            raise RimeResourceLimitError("WAV input exceeds 16 MiB")
        _validate_options(language, mode, context_terms)
        budget = validate_timeout(timeout)
        terms = tuple(context_terms)
        policy = _policy.POLICY
        identity = None

        def capture_identity(value):
            nonlocal identity
            identity = value

        async def run():
            try:
                async with asyncio.timeout(budget):
                    channel, metadata = await self._prepare(policy)
                    return await _transport.transcribe(
                        channel, metadata, audio, language, mode, terms, policy, capture_identity
                    )
            except TimeoutError:
                raise RimeTimeoutError(
                    "Overall transcription deadline expired", request_id=identity
                ) from None

        task = asyncio.create_task(run(), name="rime:transcribe")
        self._requests.add(task)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if caller is not None and caller.cancelling():
                raise
            raise RimeCancelledError("Transcription cancelled", request_id=identity) from None
        finally:
            self._requests.discard(task)

    def stream(
        self,
        audio: AsyncIterable[bytes],
        *,
        language: str,
        mode: TranscriptionMode = TranscriptionMode.WRITTEN,
        context_terms: Sequence[str] = (),
        input_format: PCMFormat | None = None,
        timeout: float | timedelta | None = None,
    ) -> TranscriptStream:
        """Transcribe one caller-ended utterance; source exhaustion commits input.

        Audio is headerless PCM16 little-endian, mono 16 kHz by default. Partial
        results replace prior text. Language and recognition terms pass unchanged
        to the service. Timeout is an optional positive overall duration, given
        as seconds or a datetime.timedelta. None disables the overall deadline.
        """
        self._client._check_open()
        if not hasattr(audio, "__aiter__"):
            raise RimeInputError("audio must be an async iterable of bytes")
        _validate_options(language, mode, context_terms)
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
            _policy.POLICY,
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
            requests = list(self._requests)
            for task in requests:
                task.cancel()
            await asyncio.gather(*requests, return_exceptions=True)
            results = await asyncio.gather(
                *(operation.cancel() for operation in list(self._streams)), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            if self._channel:
                await self._channel.close()


def _validate_options(language, mode, context_terms):
    if not isinstance(language, str):
        raise RimeInputError("language must be a string")
    if not isinstance(mode, TranscriptionMode):
        raise RimeInputError("mode must be a TranscriptionMode")
    if isinstance(context_terms, (str, bytes)) or not isinstance(context_terms, Sequence):
        raise RimeInputError("context_terms must be a sequence of strings")
    if any(not isinstance(term, str) for term in context_terms):
        raise RimeInputError("context_terms must contain strings")
