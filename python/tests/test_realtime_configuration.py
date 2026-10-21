"""Connection defaults and turn timing through the public Prism interface."""

import json

import pytest
from test_realtime import _VALIDATORS, Peer

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


@pytest.mark.parametrize("options", [{}, {"endpoint": None}])
async def test_public_endpoint_and_server_defaults(monkeypatch, options):
    peer = Peer()

    async def connect(endpoint, **kwargs):
        assert endpoint == "wss://prism.api.rime.ai/v1/realtime"
        assert kwargs["additional_headers"] == {"Authorization": "Bearer test"}
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client, client.realtime.connect(**options) as session:
        assert peer.settings["turn_detection"] == {"interrupt_response": True}
        assert "voice" not in peer.settings
        assert session.info.turn_detection == r.TurnDetectionInfo()
    assert peer.closed


@pytest.mark.parametrize(
    ("options", "wire"),
    [
        (r.TurnDetection(), {}),
        *[
            (r.TurnDetection(eagerness=value), {"eagerness": value})
            for value in ("low", "medium", "high", "auto")
        ],
        *[
            (r.TurnDetection(end_of_turn_timeout_ms=value), {"prism_end_of_turn_timeout_ms": value})
            for value in (1000, 10000)
        ],
        (
            r.TurnDetection(eagerness="low", end_of_turn_timeout_ms=2000),
            {"eagerness": "low", "prism_end_of_turn_timeout_ms": 2000},
        ),
    ],
)
async def test_turn_overrides_and_effective_server_settings(monkeypatch, options, wire):
    class ConfiguredPeer(Peer):
        async def send(self, raw):
            event = json.loads(raw)
            if event["type"] != "session.update":
                return await super().send(raw)
            _VALIDATORS[event["type"]].validate(event)
            self.settings = event["session"]
            self.emit(
                "session.updated",
                prism_request_event_id=event["event_id"],
                session={
                    "id": "session-1",
                    "voice": "test",
                    "prism_effective_interrupt_response": False,
                    "turn_detection": {
                        "interrupt_response": False,
                        "eagerness": "medium",
                        "prism_end_of_turn_timeout_ms": 3000,
                    },
                },
            )

    peer = ConfiguredPeer()

    async def connect(endpoint, **kwargs):
        assert endpoint == "ws://localhost/v1/realtime"
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(
            endpoint="ws://localhost/v1/realtime",
            interrupt_on_speech=False,
            turn_detection=options,
        ) as session,
    ):
        assert peer.settings["turn_detection"] == {"interrupt_response": False, **wire}
        assert not session.info.interrupt_on_speech
        assert session.info.turn_detection == r.TurnDetectionInfo(
            eagerness="medium", end_of_turn_timeout_ms=3000
        )


@pytest.mark.parametrize("value", ["future", "", 1, True, [], {}])
def test_invalid_turn_eagerness(value):
    with pytest.raises(RimeInputError, match="eagerness"):
        r.TurnDetection(eagerness=value)


@pytest.mark.parametrize(
    "value", [999, 10001, 1000.5, 1000.0, True, "2000", float("nan"), float("inf")]
)
def test_invalid_end_of_turn_timeout(value):
    with pytest.raises(RimeInputError, match="timeout"):
        r.TurnDetection(end_of_turn_timeout_ms=value)


async def test_invalid_turn_configuration_does_not_open_connection(monkeypatch):
    async def connect(*args, **kwargs):
        pytest.fail("Invalid settings must fail before opening a socket")

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="TurnDetection"):
            async with client.realtime.connect(turn_detection={"eagerness": "low"}):
                pass


@pytest.mark.parametrize("value", ["future", None])
async def test_unknown_or_missing_effective_turn_eagerness(monkeypatch, value):
    peer = Peer()

    async def send(raw):
        event = json.loads(raw)
        peer.emit(
            "session.updated",
            prism_request_event_id=event["event_id"],
            session={
                "id": "session-1",
                "turn_detection": {} if value is None else {"eagerness": value},
            },
        )

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(peer, "send", send)
    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client, client.realtime.connect() as session:
        assert session.info.turn_detection.eagerness == (None if value is None else "unknown")
        assert session.info.turn_detection.end_of_turn_timeout_ms is None
