import asyncio

import pytest

from rimelabs_sdk.tts._queue import ByteQueue


@pytest.mark.parametrize("limit,chunk_size", [(6, 2), (5, 3), (3, 8)])
async def test_large_input_is_bounded_and_split_in_order(limit, chunk_size):
    queue = ByteQueue(limit, chunk_size)
    data = bytes(range(23))

    async def produce():
        await queue.put(data)
        queue.finish()

    writer = asyncio.create_task(produce())
    chunks = []
    try:
        async with asyncio.timeout(1):
            while True:
                try:
                    chunks.append(await queue.get())
                except StopAsyncIteration:
                    break
                assert queue.size <= limit
            await writer
    finally:
        queue.finish()
        await writer
    assert b"".join(chunks) == data
    assert all(0 < len(part) <= min(limit, chunk_size) for part in chunks)
    assert not queue.has_pending_output


async def test_pending_output_includes_a_writer_waiting_to_resume():
    queue = ByteQueue(4, 4)
    writer = asyncio.create_task(queue.put(b"abcdefgh"))
    await asyncio.sleep(0)
    try:
        assert queue.size == 4
        assert not writer.done()
        assert await queue.get() == b"abcd"
        assert queue.size == 0
        # The producer still owns output while its wake-up is pending.
        assert queue.has_pending_output
        await asyncio.wait_for(writer, 1)
        assert await queue.get() == b"efgh"
        assert not queue.has_pending_output
    finally:
        queue.finish()
        await writer


async def test_empty_input_does_not_yield_an_empty_chunk():
    queue = ByteQueue(4, 2)
    await queue.put(b"")
    assert queue.size == 0
    assert not queue.has_pending_output
    reader = asyncio.create_task(queue.get())
    await asyncio.sleep(0)
    assert not reader.done()
    queue.finish()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(reader, 1)


async def test_finish_drains_and_ignores_further_output():
    queue = ByteQueue(4, 2)
    await queue.put(b"abcd")
    queue.finish()
    queue.finish()
    await queue.put(b"ignored")
    assert await queue.get() == b"ab"
    assert await queue.get() == b"cd"
    with pytest.raises(StopAsyncIteration):
        await queue.get()


@pytest.mark.parametrize("finish_first", [False, True])
async def test_failure_discards_output_and_cannot_be_overwritten(finish_first):
    queue = ByteQueue(4, 2)
    await queue.put(b"abcd")
    if finish_first:
        queue.finish()
    error = RuntimeError("output failed")
    queue.fail(error)
    queue.finish()
    queue.fail(RuntimeError("later failure"))
    await queue.put(b"ignored")
    assert queue.size == 0
    assert not queue.has_pending_output
    for _ in range(2):
        with pytest.raises(RuntimeError) as caught:
            await queue.get()
        assert caught.value is error


@pytest.mark.parametrize("fail", [False, True])
async def test_termination_wakes_blocked_writer(fail):
    queue = ByteQueue(4, 4)
    writer = asyncio.create_task(queue.put(b"abcdefgh"))
    await asyncio.sleep(0)
    assert not writer.done()
    if fail:
        queue.fail(RuntimeError("cancelled"))
    else:
        queue.finish()
    await asyncio.wait_for(writer, 1)
    if fail:
        assert queue.size == 0
        with pytest.raises(RuntimeError, match="cancelled"):
            await queue.get()
    else:
        assert await queue.get() == b"abcd"
        with pytest.raises(StopAsyncIteration):
            await queue.get()
    assert not queue.has_pending_output


async def test_failure_wakes_blocked_reader():
    queue = ByteQueue(4, 2)
    reader = asyncio.create_task(queue.get())
    await asyncio.sleep(0)
    error = RuntimeError("cancelled")
    queue.fail(error)
    with pytest.raises(RuntimeError) as caught:
        await asyncio.wait_for(reader, 1)
    assert caught.value is error


async def test_cancelled_writer_releases_pending_output_state():
    queue = ByteQueue(4, 4)
    writer = asyncio.create_task(queue.put(b"abcdefgh"))
    await asyncio.sleep(0)
    writer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writer
    assert await queue.get() == b"abcd"
    assert not queue.has_pending_output


@pytest.mark.parametrize("limit,chunk_size", [(0, 1), (1, 0), (-1, 1), (1, -1)])
def test_invalid_limits_fail_at_construction(limit, chunk_size):
    with pytest.raises(ValueError):
        ByteQueue(limit, chunk_size)
