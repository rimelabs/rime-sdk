"""HTTP refusals through the public realtime connection interface."""

import asyncio
from datetime import UTC

import pytest

from rimelabs_sdk import (
    Rime,
    RimeAuthenticationError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from rimelabs_sdk.realtime import RealtimeTimeouts


@pytest.mark.parametrize(
    "status,body,error_type,detail",
    [
        (401, b"Authentication failed", RimeAuthenticationError, "Authentication failed"),
        (403, b"Forbidden", RimeAuthenticationError, "Forbidden"),
        (429, b"Currently at websocket limit", RimeResourceLimitError, "websocket limit"),
        (429, b"", RimeResourceLimitError, "connection or rate limit reached"),
        (503, b"conversation capacity reached", RimeUnavailableError, "capacity reached"),
        (503, b"engine is draining", RimeUnavailableError, "engine is draining"),
        (
            503,
            b'{"error":{"message":"graph is not ready"}}',
            RimeUnavailableError,
            "graph is not ready",
        ),
        (503, b"<html>proxy error</html>", RimeUnavailableError, None),
        (503, b'{"error":', RimeUnavailableError, None),
        (503, b'{"error":null}', RimeUnavailableError, None),
        (429, b"[" * 1023, RimeResourceLimitError, None),
        (503, b"\xff", RimeUnavailableError, None),
        (503, b"bad\x00message", RimeUnavailableError, None),
        (429, b"x" * 2048, RimeResourceLimitError, None),
    ],
)
async def test_http_refusal_preserves_category_and_diagnostics(status, body, error_type, detail):
    closed = asyncio.Queue()
    attempts = 0

    async def refuse(reader, writer):
        nonlocal attempts
        attempts += 1
        failure = None
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                f"HTTP/1.1 {status} Refused\r\nContent-Length: {len(body)}\r\n"
                "x-request-id: server-request\r\n\r\n".encode()
                + body
            )
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == b""
        except (AssertionError, TimeoutError, OSError, asyncio.IncompleteReadError) as error:
            failure = error
        finally:
            writer.close()
            await writer.wait_closed()
            closed.put_nowait(failure)

    async with await asyncio.start_server(refuse, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with Rime(api_key="test") as client:
            for _ in range(2):
                with pytest.raises(error_type) as caught:
                    async with client.realtime.connect(
                        endpoint=f"ws://127.0.0.1:{port}/v1/realtime"
                    ):
                        pytest.fail("refused connection was accepted")
                error = caught.value
                assert f"HTTP {status}" in str(error)
                assert error.request_id == "server-request"
                assert error.status_code == status
                assert error.close_code is None
                assert error.retry_after_s is None
                if detail:
                    assert detail in str(error)
                else:
                    assert ": " not in str(error)
                assert await asyncio.wait_for(closed.get(), 2) is None
        assert attempts == 2


@pytest.mark.parametrize(
    "headers,expected",
    [
        ([("Retry-After", "3")], 3.0),
        ([("Retry-After", "0")], 0.0),
        ([("Retry-After", "-1")], None),
        ([("Retry-After", "1.5")], None),
        ([("Retry-After", "nan")], None),
        ([("Retry-After", "inf")], None),
        ([("Retry-After", "bad date")], None),
        ([("Retry-After", "9" * 400)], None),
        ([("Retry-After", "1"), ("Retry-After", "2")], None),
        ([("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT")], 0.0),
        ([("Retry-After", "Sunday, 06-Nov-94 08:49:37 GMT")], 0.0),
        ([("Retry-After", "Sun Nov  6 08:49:37 1994")], 0.0),
        ([("Retry-After", "Tue, 01 Jan 2030 00:00:00 +0000")], None),
        ([("Retry-After", "Tue, 01 Jan 2030 00:00:00")], None),
        ([("Retry-After", "Tue, 01 Jan 2030 00:00:00 UTC")], None),
        ([("Retry-After", "Tue, 1 Jan 2030 00:00:00 GMT")], None),
        ([("Retry-After", "tue, 01 Jan 2030 00:00:00 GMT")], None),
        ([("Retry-After", "Tue, 01 Jan 2030 00:00:00 GMT trailing")], None),
        ([("Retry-After", "Tue, 31 Feb 2030 00:00:00 GMT")], None),
        ([("Retry-After", "Tue, 01 Jan 2030 25:00:00 GMT")], None),
        ([("Retry-After", "Sat, 01 Jan 0000 00:00:00 GMT")], None),
        ([("Retry-After", "Sat Jan  1 00:00:00 0000")], None),
        ([("Retry-After", "Sat, 29 Feb 0000 00:00:00 GMT")], None),
    ],
)
async def test_http_refusal_exposes_retry_after(headers, expected):
    from websockets.asyncio.server import serve

    async def refuse(connection, request):
        response = connection.respond(503, "busy")
        for name, value in headers:
            response.headers[name] = value
        return response

    async def unexpected(socket):
        pytest.fail("refused connection upgraded")

    async with (
        serve(unexpected, "127.0.0.1", 0, process_request=refuse) as server,
        Rime(api_key="test") as client,
    ):
        with pytest.raises(RimeUnavailableError) as caught:
            async with client.realtime.connect(
                endpoint=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1/realtime"
            ):
                pytest.fail("refused connection accepted")
        assert caught.value.status_code == 503
        assert caught.value.retry_after_s == expected


@pytest.mark.parametrize("date_format", ["imf", "rfc850", "asctime"])
async def test_http_refusal_retry_after_date(date_format):
    from datetime import datetime, timedelta
    from email.utils import format_datetime

    from websockets.asyncio.server import serve

    async def refuse(connection, request):
        response = connection.respond(503, "busy")
        future = datetime.now(UTC) + timedelta(seconds=30)
        imf = format_datetime(future, usegmt=True)
        if date_format == "rfc850":
            weekdays = (
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            )
            header = (
                f"{weekdays[future.weekday()]}, {imf[5:7]}-{imf[8:11]}-{imf[14:16]} "
                f"{imf[17:25]} GMT"
            )
        elif date_format == "asctime":
            header = f"{imf[:3]} {imf[8:11]} {future.day:2} {imf[17:25]} {future.year}"
        else:
            header = imf
        response.headers["Retry-After"] = header
        return response

    async def unexpected(socket):
        pytest.fail("refused connection upgraded")

    async with (
        serve(unexpected, "127.0.0.1", 0, process_request=refuse) as server,
        Rime(api_key="test") as client,
    ):
        with pytest.raises(RimeUnavailableError) as caught:
            async with client.realtime.connect(
                endpoint=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1/realtime"
            ):
                pytest.fail("refused connection accepted")
        assert 28 < caught.value.retry_after_s <= 30


@pytest.mark.parametrize(
    "now,header,expected_date",
    [
        ("2026-01-01", "Tuesday, 01-Jan-75 00:00:00 GMT", "2075-01-01"),
        ("2026-01-01", "Wednesday, 01-Jan-76 00:00:00 GMT", "2076-01-01"),
        ("2026-01-01", "Friday, 02-Jan-76 00:00:00 GMT", "1976-01-02"),
        ("2090-01-01", "Friday, 01-Jan-00 00:00:00 GMT", "2100-01-01"),
        ("2090-01-01", "Tuesday, 29-Feb-00 00:00:00 GMT", None),
    ],
)
async def test_http_refusal_retry_after_obsolete_year(monkeypatch, now, header, expected_date):
    from datetime import datetime
    from types import SimpleNamespace

    from websockets.asyncio.server import serve

    from rimelabs_sdk.realtime import _client

    timestamp = datetime.fromisoformat(now).replace(tzinfo=UTC).timestamp()
    monkeypatch.setattr(_client, "time", SimpleNamespace(time=lambda: timestamp))

    async def refuse(connection, request):
        response = connection.respond(503, "busy")
        response.headers["Retry-After"] = header
        return response

    async def unexpected(socket):
        pytest.fail("refused connection upgraded")

    async with (
        serve(unexpected, "127.0.0.1", 0, process_request=refuse) as server,
        Rime(api_key="test") as client,
    ):
        with pytest.raises(RimeUnavailableError) as caught:
            async with client.realtime.connect(
                endpoint=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1/realtime"
            ):
                pytest.fail("refused connection accepted")
        expected = (
            max(
                0, datetime.fromisoformat(expected_date).replace(tzinfo=UTC).timestamp() - timestamp
            )
            if expected_date is not None
            else None
        )
        assert caught.value.retry_after_s == expected


@pytest.mark.parametrize("code", [1000, 1001, 1008, 1013, None])
async def test_close_before_initialization_preserves_received_code(code):
    from websockets.asyncio.server import serve

    from rimelabs_sdk import RimeStreamError

    async def close(socket):
        if code is None:
            socket.transport.abort()
        else:
            await socket.close(code=code)

    async with serve(close, "127.0.0.1", 0) as server, Rime(api_key="test") as client:
        with pytest.raises(RimeStreamError) as caught:
            async with client.realtime.connect(
                endpoint=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1/realtime"
            ):
                pytest.fail("closed session initialized")
        assert caught.value.close_code == code
        assert caught.value.status_code is None


@pytest.mark.parametrize(
    "header,expected",
    [
        (b"", None),
        (b"x-request-id: \r\n", None),
        (b"x-request-id: server-request\r\n", "server-request"),
    ],
)
async def test_http_refusal_normalizes_missing_request_id(header, expected):
    async def refuse(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 429 Refused\r\nContent-Length: 0\r\n"
                + header
                + b"Connection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(refuse, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with Rime(api_key="test") as client:
            with pytest.raises(RimeResourceLimitError) as caught:
                async with client.realtime.connect(endpoint=f"ws://127.0.0.1:{port}/v1/realtime"):
                    pytest.fail("refused connection was accepted")
            assert caught.value.request_id == expected


async def test_unfinished_http_body_obeys_connect_timeout_and_closes():
    closed = asyncio.Event()

    async def refuse(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 429 Refused\r\nContent-Length: 100\r\n\r\n")
            await writer.drain()
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            closed.set()

    async with await asyncio.start_server(refuse, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with Rime(api_key="test") as client:
            with pytest.raises(RimeTimeoutError):
                async with client.realtime.connect(
                    endpoint=f"ws://127.0.0.1:{port}/v1/realtime",
                    timeouts=RealtimeTimeouts(connect_s=0.1),
                ):
                    pytest.fail("unfinished handshake was accepted")
            await asyncio.wait_for(closed.wait(), 2)
