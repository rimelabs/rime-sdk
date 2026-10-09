"""One raw-text operation, one RPC, one audio consumer."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from typing import Self

from .._errors import (
    RimeCancelledError,
    RimeError,
    RimeInputError,
    RimeStreamError,
    RimeTimeoutError,
)
from . import _transport
from ._audio import AudioFormat, Converter
from ._queue import ByteQueue
from ._sentences import SentenceBuffer
from ._timestamps import TimestampResult

_LOG = logging.getLogger(__name__)
_CONSTRUCTION_KEY = object()


class AudioStream:
    def __init__(
        self,
        key,
        client,
        source,
        voice,
        language,
        profile,
        timeout,
        timestamps=False,
        custom_lexicon=(),
        complete_text=False,
    ):
        if key is not _CONSTRUCTION_KEY:
            raise TypeError("AudioStream is returned by client.tts.stream()")
        self._client = client
        self._source = source
        self._voice = voice
        self._language = language
        self._custom_lexicon = custom_lexicon
        self._complete_text = complete_text
        self._format = profile
        self._timeout = timeout
        self._timestamps_requested = timestamps
        self._operation_id = uuid.uuid4().hex
        self._queue = ByteQueue(client._policy.output_bytes, client._policy.output_chunk_bytes)
        self._worker = None
        self._watcher = None
        self._call = None
        self._error = None
        self._finished = False
        self._reading = False
        self._input_done = False
        self._source_waiting = False
        self._submitted = False
        self._received = False
        self._progress_at = 0.0
        self._bytes_received = 0
        self._bytes_delivered = 0

    @property
    def format(self) -> AudioFormat:
        return self._format

    @property
    def request_id(self) -> str | None:
        return self._call.request_id if self._call is not None else None

    async def timestamps(self) -> TimestampResult:
        """Read requested timestamps after consuming all audio; never drains audio.

        Calling before iteration completes raises RimeInputError. Synthesis
        errors are raised here too; alignment failures are returned in status.
        """
        if not self._timestamps_requested:
            raise RimeInputError("Enable timestamps=True when creating the stream")
        if self._error:
            raise self._error
        if not self._finished:
            raise RimeInputError("Consume all audio before reading timestamps")
        assert self._call is not None
        return self._call.timestamp_result()

    def _start(self):
        self._client._check_loop()
        if self._error:
            raise self._error
        if self._finished:
            return
        if self._worker is None:
            self._client._check_open()
            self._started_at = time.monotonic()
            self._worker = asyncio.create_task(self._run(), name="rime:operation")
            self._watcher = asyncio.create_task(self._watch(), name="rime:deadline")

    def _fail(self, error):
        if self._error or self._finished:
            return
        error.request_id = self.request_id
        self._error = error
        self._queue.fail(error)
        if self._call:
            self._call.cancel()
        if self._worker and self._worker is not asyncio.current_task():
            self._worker.cancel()

    async def _watch(self):
        try:
            while not self._finished and not self._error:
                await asyncio.sleep(0.02)
                now = time.monotonic()
                if self._timeout is not None and now - self._started_at >= self._timeout:
                    self._fail(RimeTimeoutError("Overall synthesis deadline expired"))
                    return
                if (
                    self._worker
                    and not self._worker.done()
                    and self._submitted
                    and not self._source_waiting
                    and not self._queue.has_pending_output
                ):
                    limit = (
                        self._client._policy.progress_timeout
                        if self._received
                        else self._client._policy.first_audio_timeout
                    )
                    if now - self._progress_at >= limit:
                        self._fail(RimeTimeoutError("Synthesis output stopped making progress"))
                        return
                else:
                    # Consumer/source pauses suspend internal stall accounting.
                    self._progress_at = now
        except asyncio.CancelledError:
            pass

    async def _write_sentence(self, sentence):
        assert self._call is not None
        if not self._submitted:
            self._progress_at = time.monotonic()
        self._submitted = True
        await self._call.write(sentence)

    async def _produce(self):
        assert self._call is not None
        buffer = SentenceBuffer(self._client._policy.sentence_bytes)
        iterator = None
        meaningful = False
        try:
            if isinstance(self._source, str):

                async def one():
                    yield self._source

                iterator = one().__aiter__()
            else:
                try:
                    iterator = self._source.__aiter__()
                except Exception as error:
                    raise RimeInputError("The text source failed") from error
            while True:
                self._source_waiting = True
                try:
                    chunk = await anext(iterator)
                except StopAsyncIteration:
                    break
                except Exception as error:
                    raise RimeInputError("The text source failed") from error
                finally:
                    self._source_waiting = False
                if not isinstance(chunk, str):
                    raise RimeInputError("The text source must yield strings")
                if chunk.strip():
                    meaningful = True
                step = self._client._policy.source_chars
                for offset in range(0, len(chunk), step):
                    for sentence in buffer.feed(chunk[offset : offset + step]):
                        await self._write_sentence(sentence)
            if not meaningful:
                raise RimeInputError("The text source contained no meaningful text")
            for sentence in buffer.feed("", final=True):
                await self._write_sentence(sentence)
            await self._call.finish_input()
            self._input_done = True
        finally:
            if iterator is not None and hasattr(iterator, "aclose"):
                try:
                    async with asyncio.timeout(self._client._policy.cleanup_timeout):
                        await iterator.aclose()
                except (TimeoutError, RuntimeError):
                    _LOG.debug(
                        "Source cleanup could not complete",
                        extra={"operation_id": self._operation_id},
                    )

    async def _read(self):
        assert self._call is not None
        converter = Converter(self._format)
        async for data in self._call.audio():
            self._progress_at = time.monotonic()
            if data:
                self._received = True
                self._bytes_received += len(data)
                await self._queue.put(converter.process(data))
        if not self._input_done:
            raise RimeStreamError("The service completed before input finished")
        await self._queue.put(converter.process(b"", final=True))

    async def _run(self):
        tasks = []
        try:
            channel, metadata = await self._client._prepare()
            self._call = _transport.SynthesisCall(channel, metadata, self._complete_text)
            await self._call.start(
                self._voice,
                self._language,
                self._timestamps_requested,
                self._custom_lexicon,
                self._source if self._complete_text else None,
            )
            if self._complete_text:
                self._input_done = self._submitted = True
                self._progress_at = time.monotonic()
            else:
                tasks.append(asyncio.create_task(self._produce(), name="rime:input"))
            tasks.append(asyncio.create_task(self._read(), name="rime:audio"))
            # The worker owns child cancellation. Unlike gather(), wait() does
            # not cancel children when the worker is cancelled.
            # FIRST_EXCEPTION does not wake for a cancelled child task.
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            self._queue.finish()
        except RimeError as error:
            self._fail(error)
        except asyncio.CancelledError:
            if not self._error:
                self._fail(RimeCancelledError("Synthesis cancelled"))
        except Exception as error:  # noqa: BLE001 - terminal boundary wakes all readers
            failure = RimeStreamError("Synthesis failed")
            failure.__cause__ = error
            self._fail(failure)
        finally:
            for task in tasks:
                if not task.done() and not task.cancelling():
                    task.cancel()
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=self._client._policy.cleanup_timeout)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            if self._call and not self._call.done():
                self._call.cancel()
            self._source = None
            if self._error:
                self._client._streams.discard(self)
            _LOG.debug(
                "Synthesis stopped",
                extra={
                    "operation_id": self._operation_id,
                    "request_id": self.request_id,
                    "bytes_received": self._bytes_received,
                    "bytes_delivered": self._bytes_delivered,
                },
            )

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        if self._reading:
            raise RimeInputError("AudioStream permits only one concurrent reader")
        self._reading = True
        try:
            self._start()
            try:
                data = await self._queue.get()
            except StopAsyncIteration:
                self._finished = True
                await self._cleanup()
                raise
            self._bytes_delivered += len(data)
            return data
        except asyncio.CancelledError:
            await self.cancel()
            raise
        finally:
            self._reading = False

    async def _cleanup(self):
        if self._watcher:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)
        if self._worker:
            await asyncio.gather(self._worker, return_exceptions=True)
        self._client._streams.discard(self)

    async def cancel(self) -> None:
        if not self._finished and not self._error:
            self._fail(RimeCancelledError("Synthesis cancelled"))
        await self._cleanup()
        self._source = None

    async def __aenter__(self) -> Self:
        self._start()
        return self

    async def __aexit__(self, *_):
        await self.cancel()
