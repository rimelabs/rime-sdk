"""Python iterator scheduling; native code owns stream policy."""

from __future__ import annotations

import asyncio
import json
import weakref
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Self

from ._audio import AudioFormat
from ._bridge import call, translate
from ._errors import RimeError, RimeInputError

_CONSTRUCTION_KEY = object()


def _consume(task):
    if not task.cancelled():
        task.exception()


def _source_failed(owner, error=None):
    stream = owner()
    if stream is not None:
        if error is None:
            stream._native.cancel()
        else:
            stream._cause = error
            stream._native.fail_source()


async def _read_source(source, requests, replies, closing, owner):
    # This task owns the iterator throughout its lifetime, including aclose().
    # Only a weak reference reaches the stream, so cancellation can detach it safely.
    iterator = None
    try:
        if isinstance(source, str):

            async def one():
                yield source

            iterator = one().__aiter__()
        else:
            iterator = source.__aiter__()
        while True:
            await requests.get()
            try:
                item = await anext(iterator)
            except StopAsyncIteration:
                replies.put_nowait(("end", None))
                break
            replies.put_nowait(("item", item))
            del item
    except asyncio.CancelledError:
        _source_failed(owner)
    except Exception as error:  # noqa: BLE001 - preserve arbitrary host source exceptions
        _source_failed(owner, error)
    finally:
        closing.set()
        if iterator is not None and hasattr(iterator, "aclose"):
            # Cleanup must not replace the terminal result.
            with suppress(Exception):
                await iterator.aclose()


async def _bounded_cleanup(awaitable, timeout):
    task = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait([task], timeout=timeout)
    if done:
        _consume(task)
    else:
        task.cancel()
        task.add_done_callback(_consume)
        await asyncio.sleep(0)


class AudioStream:
    def __init__(self, key, client, native, source, profile):
        if key is not _CONSTRUCTION_KEY:
            raise RimeInputError("AudioStream is returned by client.tts.stream()")
        self._client, self._native, self._source, self._format = client, native, source, profile
        self._pump_task = None
        self._stop_task = None
        self._cause = None
        self._finished = False
        self._reading = False

    @property
    def format(self) -> AudioFormat:
        return self._format

    @property
    def request_id(self) -> str | None:
        return self._native.request_id

    @property
    def _input_done(self):
        return self._native.input_done

    def _start(self):
        self._client._check_loop()
        call(self._native.start)
        if self._pump_task is None:
            self._pump_task = asyncio.create_task(self._pump())
            self._stop_task = asyncio.create_task(self._cleanup_when_stopped())

    async def _cleanup_when_stopped(self):
        await self._native.wait_stopped()
        await self._cleanup()

    async def _pump(self):
        pending = None
        requests: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
        replies: asyncio.Queue[tuple[str, object]] = asyncio.Queue(maxsize=1)
        closing = asyncio.Event()
        source_task = asyncio.create_task(
            _read_source(self._source, requests, replies, closing, weakref.ref(self))
        )
        stopped = asyncio.ensure_future(self._native.wait_stopped())
        try:
            item, offset = "", 0
            while (request := await self._native.input_request()) is not None:
                if request == 0:
                    requests.put_nowait(None)
                    pending = asyncio.create_task(replies.get())
                    done, _ = await asyncio.wait(
                        [pending, stopped], return_when=asyncio.FIRST_COMPLETED
                    )
                    if stopped in done:
                        break
                    kind, value = pending.result()
                    pending = None
                    if kind == "end":
                        call(self._native.input_reply, '{"kind":"end"}')
                        break
                    if not isinstance(value, str):
                        raise RimeInputError("The text source must yield strings")
                    item = value
                    del value
                    offset = 0
                    reply: dict[str, object] = {"kind": "item"}
                else:
                    part = item[offset : offset + self._native.source_chars]
                    offset += len(part)
                    reply = {"kind": "text", "text": part, "last": offset == len(item)}
                    if reply["last"]:
                        item = ""
                call(self._native.input_reply, json.dumps(reply, ensure_ascii=False))
        except asyncio.CancelledError:
            self._native.cancel()
            return
        except Exception as error:  # noqa: BLE001 - preserve arbitrary host source exceptions
            self._cause = error
            self._native.fail_source()
        finally:
            stopped.cancel()
            if pending is not None:
                pending.cancel()
                pending.add_done_callback(_consume)
            if not closing.is_set():
                source_task.cancel()
            await _bounded_cleanup(source_task, self._native.cleanup_timeout)
            self._source = None

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        # Validation must not enter cleanup on a different event loop.
        self._client._check_loop()
        if self._finished:
            raise StopAsyncIteration
        if self._reading:
            raise RimeInputError("AudioStream permits only one concurrent reader")
        self._reading = True
        try:
            self._start()
            data, ticket = await self._native.read()
            if data is None:
                await self._cleanup()
            call(self._native.accept_read, ticket)
            if data is None:
                self._finished = True
                raise StopAsyncIteration
            return data
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except (ValueError, RimeError) as error:
            await self._cleanup()
            raise translate(error) from self._cause
        finally:
            self._reading = False

    async def _cleanup(self):
        if self._pump_task:
            try:
                await asyncio.shield(self._pump_task)
            except asyncio.CancelledError:
                if not self._pump_task.done():
                    raise
        self._client._streams.discard(self)
        self._source = None

    async def cancel(self) -> None:
        self._native.cancel()
        await self._cleanup()

    async def __aenter__(self) -> Self:
        self._start()
        return self

    async def __aexit__(self, *_):
        await self.cancel()
