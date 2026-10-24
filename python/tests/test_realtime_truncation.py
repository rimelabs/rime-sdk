"""Explicit history policy preserves older-server request compatibility."""

import asyncio

import pytest
from test_realtime import Peer, session

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client

CASES = [
    ("auto", "auto"),
    ("disabled", "disabled"),
    (r.RetentionRatio(0.9), {"type": "retention_ratio", "retention_ratio": 0.9}),
    (r.RetentionRatio(1), {"type": "retention_ratio", "retention_ratio": 1}),
]


@pytest.mark.parametrize("setting,wire", [(None, None), *CASES])
async def test_connect_omits_or_sends_explicit_truncation(monkeypatch, setting, wire):
    peer = Peer()

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(truncation=setting) as current,
    ):
        if setting is None:
            assert "truncation" not in peer.settings
        else:
            assert peer.settings["truncation"] == wire
        assert current.info.truncation is None


@pytest.mark.parametrize("value", [False, {}, "unknown", 0.9, float("nan"), float("inf")])
async def test_invalid_connect_setting_never_opens_transport(monkeypatch, value):
    opened = False

    async def connect(*args, **kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("invalid settings opened the transport")

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError):
            async with client.realtime.connect(truncation=value):
                pytest.fail("invalid truncation was accepted")
    assert not opened


@pytest.mark.parametrize("setting,wire", CASES)
async def test_update_returns_acknowledged_truncation(monkeypatch, setting, wire):
    async with session(monkeypatch) as (current, peer, _):
        pending = asyncio.create_task(current.update(truncation=setting))
        q = await peer.next("session.update")
        assert q["session"] == {"truncation": wire}
        assert current.info.truncation is None
        peer.emit(
            "session.updated",
            prism_request_event_id=q["event_id"],
            session={"id": "session-1", "truncation": wire},
        )
        assert (await pending).truncation == setting
        assert current.info.truncation == setting


@pytest.mark.parametrize("value", [0, -0.1, 1.1, float("nan"), float("inf"), True, None, "0.9"])
def test_invalid_retention_ratio(value):
    with pytest.raises(RimeInputError):
        r.RetentionRatio(value)


@pytest.mark.parametrize("value", [False, {}, "unknown", 0.9])
async def test_invalid_setting_never_sends(monkeypatch, value):
    async with session(monkeypatch) as (current, peer, _):
        with pytest.raises(RimeInputError):
            await current.update(truncation=value)
        assert peer.sent.empty()
