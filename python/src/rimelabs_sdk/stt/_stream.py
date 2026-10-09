"""One utterance with concurrent input, replacing transcripts, and owned cleanup."""

from __future__ import annotations

import asyncio
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
from ._audio import InputAudio
from ._protocol import TranscriptState
from ._queue import TranscriptQueue
from ._types import TranscriptionFinal, TranscriptionUpdate


class TranscriptStream:
    """Lazy, single-consumer transcript stream returned by ``client.stt.stream``.

    Enter the async context or start iteration to connect. Context exit and
    ``cancel`` cancel unfinished work; exhausting the input source requests a
    final transcript. Breaking iteration alone requires ``cancel`` or a context.
    """

    def __init__(self, owner, source, language, mode, terms, format, timeout, policy):
        self._owner = owner
        self._source = source
        self._language = language
        self._mode = mode
        self._terms = terms
        self._format = format
        self._timeout = timeout
        self._policy = policy
        self._queue = TranscriptQueue(policy.queued_updates)
        self._accepted = asyncio.Event()
        self._worker: asyncio.Task | None = None
        self._call: _transport.TranscriptionCall | None = None
        self._failure: RimeError | None = None
        self._timers: list[asyncio.TimerHandle] = []
        self._admission_timer: asyncio.TimerHandle | None = None
        self._finished = False
        self._reading = False
        self._input_done = False

    @property
    def request_id(self) -> str | None:
        """Server request identity, available after response headers arrive."""
        return self._call.request_id if self._call else None

    def _deadline(self, seconds, message):
        timer = asyncio.get_running_loop().call_later(
            seconds, lambda: self._fail(RimeTimeoutError(message))
        )
        self._timers.append(timer)
        return timer

    def _start(self):
        self._owner._client._check_loop()
        if self._failure:
            raise self._failure
        if self._finished or self._worker is not None:
            return
        self._owner._client._check_open()
        if self._timeout is not None:
            self._deadline(self._timeout, "Overall transcription deadline expired")
        self._worker = asyncio.create_task(self._run(), name="rime:transcription")

    def _fail(self, error: RimeError):
        if self._failure or self._finished:
            return
        error.request_id = error.request_id or self.request_id
        self._failure = error
        self._queue.fail(error)
        for timer in self._timers:
            timer.cancel()
        if self._call:
            self._call.cancel()
        if self._worker and self._worker is not asyncio.current_task():
            self._worker.cancel()

    async def _produce(self):
        assert self._call is not None
        await self._accepted.wait()
        audio = InputAudio(self._format)
        iterator = None
        try:
            try:
                iterator = self._source.__aiter__()
            except Exception as error:
                raise RimeInputError("The audio source failed") from error
            while True:
                try:
                    data = await anext(iterator)
                except StopAsyncIteration:
                    break
                except Exception as error:
                    raise RimeInputError("The audio source failed") from error
                for part in audio.feed(data):
                    await self._call.write(part)
            audio.finish()
            self._input_done = True
            self._deadline(self._policy.completion_timeout, "Transcription completion timed out")
            await self._call.finish_input()
        finally:
            if iterator is not None and hasattr(iterator, "aclose"):

                async def close_source():
                    await iterator.aclose()

                close = asyncio.create_task(close_source())
                try:
                    await asyncio.wait({close}, timeout=self._policy.cleanup_timeout)
                finally:
                    if not close.done():
                        close.cancel()
                        close.add_done_callback(
                            lambda task: None if task.cancelled() else task.exception()
                        )
                    elif not close.cancelled():
                        close.exception()

    async def _read(self):
        assert self._call is not None and self._admission_timer is not None
        state = TranscriptState(self._call.proto, self._policy.transcript_bytes)
        async for message in self._call.responses():
            update = state.accept(message, input_done=self._input_done)
            if state.language is not None and not self._accepted.is_set():
                self._accepted.set()
                self._admission_timer.cancel()
            if update is not None:
                await self._queue.put(update)
        return state.finish()

    async def _run(self):
        tasks = []
        try:
            channel, metadata = await self._owner._prepare(self._policy)
            self._call = _transport.TranscriptionCall(channel, metadata)
            self._admission_timer = self._deadline(
                self._policy.acceptance_timeout, "Transcription acceptance timed out"
            )
            await self._call.start(self._language, self._mode, self._terms)
            producer = asyncio.create_task(self._produce(), name="rime:stt-input")
            reader = asyncio.create_task(self._read(), name="rime:stt-results")
            tasks = [producer, reader]
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            await self._queue.put(reader.result())
            self._queue.finish()
        except RimeError as error:
            self._fail(error)
        except asyncio.CancelledError:
            self._fail(RimeCancelledError("Transcription cancelled"))
        except Exception as error:  # noqa: BLE001 - terminal boundary releases both directions
            failure = RimeStreamError("Transcription failed")
            failure.__cause__ = error
            self._fail(failure)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                done, pending = await asyncio.wait(tasks, timeout=self._policy.cleanup_timeout)
                for task in done:
                    if not task.cancelled():
                        task.exception()
                for task in pending:
                    task.cancel()
                    task.add_done_callback(
                        lambda task: None if task.cancelled() else task.exception()
                    )
            if self._call:
                self._call.cancel()
            self._source = None
            if self._failure:
                self._owner._streams.discard(self)

    def __aiter__(self) -> AsyncIterator[TranscriptionUpdate]:
        return self

    async def __anext__(self) -> TranscriptionUpdate:
        if self._reading:
            raise RimeInputError("TranscriptStream permits only one concurrent reader")
        self._reading = True
        try:
            self._start()
            if self._finished:
                raise StopAsyncIteration
            update = await self._queue.get()
            if self._failure:
                raise self._failure
            if isinstance(update, TranscriptionFinal):
                await self._cleanup()
                if self._failure:
                    raise self._failure
                self._finished = True
            return update
        except asyncio.CancelledError:
            await self.cancel()
            raise
        finally:
            self._reading = False

    async def _cleanup(self):
        if self._worker:
            await asyncio.shield(asyncio.gather(self._worker, return_exceptions=True))
        for timer in self._timers:
            timer.cancel()
        self._owner._streams.discard(self)

    async def cancel(self) -> None:
        """Cancel unfinished work and release owned tasks; safe to call repeatedly."""
        self._owner._client._check_loop()
        self._fail(RimeCancelledError("Transcription cancelled"))
        await self._cleanup()
        self._source = None

    async def __aenter__(self) -> Self:
        self._start()
        return self

    async def __aexit__(self, *_):
        await self.cancel()
