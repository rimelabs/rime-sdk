import asyncio
import logging
import time
from contextlib import asynccontextmanager
from itertools import pairwise

import grpc
import pytest
from fake_service import FakeService

from rimelabs_sdk import Rime, RimeInputError, RimeTimeoutError
from rimelabs_sdk.stt import _policy as stt_policy
from rimelabs_sdk.stt import _transport as stt_transport
from rimelabs_sdk.tts import _transport

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "options,enabled",
    [
        ({}, False),
        ({"model": "mistv3"}, False),
        ({"enable_tts_keepalive": True}, True),
        ({"model": "mistv3", "enable_tts_keepalive": True}, True),
        ({"endpoint": "coda.api.rime.ai:443", "enable_tts_keepalive": True}, True),
        ({"endpoint": "mist.api.rime.ai", "enable_tts_keepalive": True}, True),
        ({"endpoint": "CODA.API.RIME.AI:00443", "enable_tts_keepalive": True}, True),
        ({"model": "mistv3", "endpoint": "coda.api.rime.ai", "enable_tts_keepalive": True}, True),
        ({"endpoint": "private.example:443", "enable_tts_keepalive": True}, False),
        ({"endpoint": "coda.api.rime.ai.example", "enable_tts_keepalive": True}, False),
        ({"endpoint": "mist.api.rime.ai:8443", "enable_tts_keepalive": True}, False),
        ({"endpoint": "coda.api.rime.ai:443"}, False),
        ({"endpoint": "mist.api.rime.ai:443"}, False),
    ],
)
async def test_keepalive_scope(monkeypatch, options, enabled):
    captured = []

    def channel(target, credentials, **kwargs):
        captured.append(dict(kwargs["options"]))
        return grpc.aio.insecure_channel("127.0.0.1:1", **kwargs)

    monkeypatch.setattr(grpc.aio, "secure_channel", channel)
    async with Rime(api_key="test-key", **options) as client:
        assert client.tts._channel is None
        transport = _transport.make_channel(client.tts._policy)
        await transport.close()
        recognition = stt_transport.make_channel(stt_policy.POLICY)
        await recognition.close()
    expected = {
        "grpc.keepalive_time_ms": 60000,
        "grpc.keepalive_timeout_ms": 20000,
        "grpc.http2.ping_timeout_ms": 20000,
        "grpc.keepalive_permit_without_calls": 1,
        "grpc.http2.max_pings_without_data": 0,
    }
    keys = expected.keys() & captured[0].keys()
    assert {key: captured[0][key] for key in keys} == (expected if enabled else {})
    assert not expected.keys() & captured[1].keys()


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
def test_keepalive_requires_boolean(value):
    with pytest.raises(RimeInputError, match="enable_tts_keepalive must be a boolean"):
        Rime(api_key="test-key", enable_tts_keepalive=value)


class ConnectionProbe:
    def __init__(self):
        self.stalled = False
        self.pings = asyncio.Queue()
        self.closed = asyncio.Event()
        self.connections = 0
        self.tasks = set()

    @asynccontextmanager
    async def route(self, target):
        async def accept(reader, writer):
            task = asyncio.current_task()
            self.tasks.add(task)
            self.connections += 1
            upstream_reader, upstream_writer = await asyncio.open_connection(*target.rsplit(":", 1))

            async def outgoing():
                upstream_writer.write(await reader.readexactly(24))
                while True:
                    header = await reader.readexactly(9)
                    payload = await reader.readexactly(int.from_bytes(header[:3]))
                    if header[3] == 6 and not header[4] & 1:
                        self.pings.put_nowait(time.monotonic())
                    if not self.stalled:
                        upstream_writer.write(header + payload)
                        await upstream_writer.drain()

            async def incoming():
                while data := await upstream_reader.read(65536):
                    if not self.stalled:
                        writer.write(data)
                        await writer.drain()

            pumps = [asyncio.create_task(outgoing()), asyncio.create_task(incoming())]
            try:
                await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for pump in pumps:
                    pump.cancel()
                await asyncio.gather(*pumps, return_exceptions=True)
                writer.close()
                upstream_writer.close()
                async with asyncio.timeout(3):
                    await asyncio.gather(writer.wait_closed(), upstream_writer.wait_closed())
                self.closed.set()
                self.tasks.remove(task)

        server = await asyncio.start_server(accept, "127.0.0.1", 0)
        try:
            yield f"127.0.0.1:{server.sockets[0].getsockname()[1]}"
        finally:
            server.close()
            tasks = list(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await server.wait_closed()


async def test_keepalive_wire_and_recovery(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=__name__)
    server_options = (
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.min_ping_interval_without_data_ms", 0),
        ("grpc.http2.max_ping_strikes", 0),
    )
    healthy, stalled = ConnectionProbe(), ConnectionProbe()
    async with (
        FakeService(server_options=server_options) as first,
        FakeService(server_options=server_options) as second,
        healthy.route(first.target) as first_target,
        stalled.route(second.target) as second_target,
    ):
        routes = {"coda.api.rime.ai:443": first_target, "mist.api.rime.ai:443": second_target}

        def channel(target, credentials, **kwargs):
            # Disable bandwidth-estimation pings so only keepalive is observed.
            kwargs["options"] = [*kwargs["options"], ("grpc.http2.bdp_probe", 0)]
            return grpc.aio.insecure_channel(routes[target], **kwargs)

        monkeypatch.setattr(grpc.aio, "secure_channel", channel)
        async with (
            Rime(api_key="test-key", enable_tts_keepalive=True) as coda,
            Rime(api_key="test-key", model="mistv3", enable_tts_keepalive=True) as mist,
        ):
            assert await coda.languages.list() == ["en", "de"]
            assert await mist.languages.list() == ["en", "de"]
            started = time.monotonic()
            stalled.stalled = True
            with pytest.raises(RimeTimeoutError):
                await mist.languages.list(timeout=0.15)

            async def recover():
                ping = await asyncio.wait_for(stalled.pings.get(), 75)
                logger.info("Stalled transport ping after %.3fs", ping - started)
                assert 55 <= ping - started <= 75
                await asyncio.wait_for(stalled.closed.wait(), 30)
                logger.info("Stalled transport closed after %.3fs", time.monotonic() - ping)
                assert time.monotonic() - ping >= 18
                assert second.discovery_calls == 1
                stalled.stalled = False
                assert await mist.languages.list() == ["en", "de"]
                assert stalled.connections == 2

            async def preserve():
                pings = []
                for count in range(3):
                    ping = await asyncio.wait_for(healthy.pings.get(), 75)
                    pings.append(ping)
                    logger.info("Healthy idle ping %d after %.3fs", count + 1, ping - started)
                assert 55 <= pings[0] - started <= 75
                assert all(55 <= right - left <= 75 for left, right in pairwise(pings)), [
                    ping - started for ping in pings
                ]
                assert await coda.languages.list() == ["en", "de"]
                assert healthy.connections == 1

            async with asyncio.timeout(240), asyncio.TaskGroup() as tasks:
                tasks.create_task(preserve())
                tasks.create_task(recover())
        await asyncio.wait_for(healthy.closed.wait(), 3)
