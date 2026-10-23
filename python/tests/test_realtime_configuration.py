"""Connection defaults and turn timing through the public Prism interface."""

import json

import pytest
from test_realtime import _VALIDATORS, Peer

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


@pytest.mark.parametrize(
    "filler",
    [
        None,
        {"type": "none"},
        {"type": "text", "text": " One moment. "},
        {"type": "auto"},
        {"type": "auto", "instructions": " Say you are checking. "},
        {"type": "text", "text": "x"},
        {"type": "text", "text": "😀" * 500},
        {"type": "auto", "instructions": "😀" * 500},
        *[
            {"type": mode, field: value}
            for mode, field in (("text", "text"), ("auto", "instructions"))
            for value in ("\ufeff", "\u001c", "\u001d", "\u001e", "\u001f", "\u0085Hi\u0085")
        ],
    ],
)
async def test_tool_filler_serializes_at_tool_level(monkeypatch, filler):
    peer = Peer()

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    tool = r.ToolDefinition(
        name="press_digits",
        parameters={"type": "object"},
        prism_filler=None if filler is None else r.PrismFiller(**filler),
    )
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(tools=[tool, r.ToolDefinition(name="other", parameters={})]),
    ):
        expected = {
            "type": "function",
            "function": {
                "name": "press_digits",
                "description": "",
                "parameters": {"type": "object"},
            },
        }
        if filler is not None:
            expected["prism_filler"] = filler
        assert peer.settings["tools"][0] == expected
        assert "prism_filler" not in peer.settings["tools"][1]


@pytest.mark.parametrize(
    "filler",
    [
        {"type": "future"},
        {"type": None},
        {"type": []},
        {"type": "text"},
        {"type": "none", "text": "Hi"},
        {"type": "none", "instructions": "Hi"},
        {"type": "auto", "text": "Hi"},
        {"type": "text", "text": "Hi", "instructions": "Hi"},
        *[{"type": "text", "text": value} for value in ("", " \n\t", "\u0085", 1, [], "😀" * 501)],
        *[
            {"type": "auto", "instructions": value}
            for value in ("", " \n\t", "\u0085", 1, [], "😀" * 501)
        ],
    ],
)
def test_invalid_tool_filler(filler):
    with pytest.raises(RimeInputError, match="filler"):
        r.PrismFiller(**filler)


@pytest.mark.parametrize("mode,field", [("text", "text"), ("auto", "instructions")])
@pytest.mark.parametrize(
    "value",
    [chr(0xD800), chr(0xDFFF), "Hi" + chr(0xD800), chr(0xDFFF) + "Hi", chr(0xD800) + chr(0xDC00)],
)
async def test_invalid_unicode_filler_does_not_open_connection(monkeypatch, mode, field, value):
    async def connect(*args, **kwargs):
        pytest.fail("Invalid Unicode must fail before opening a socket")

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="UTF-8"):
            filler = r.PrismFiller(type=mode, **{field: value})
            async with client.realtime.connect(
                tools=[r.ToolDefinition(name="lookup", parameters={}, prism_filler=filler)]
            ):
                pass


@pytest.mark.parametrize("filler", [{"type": "none"}, "none", False, []])
async def test_invalid_tool_filler_value_does_not_open_connection(monkeypatch, filler):
    async def connect(*args, **kwargs):
        pytest.fail("Invalid filler settings must fail before opening a socket")

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="PrismFiller"):
            async with client.realtime.connect(
                tools=[r.ToolDefinition(name="lookup", parameters={}, prism_filler=filler)]
            ):
                pass


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


@pytest.mark.parametrize(("request_id", "used"), [("client-7", "client-7"), (None, "server-made")])
async def test_request_id_on_the_upgrade_and_the_one_prism_used(monkeypatch, request_id, used):
    peer = Peer()

    async def send(raw):
        event = json.loads(raw)
        peer.emit(
            "session.updated",
            prism_request_event_id=event["event_id"],
            session={"id": "session-1", "request_id": used},
        )

    async def connect(endpoint, **kwargs):
        expected = {"Authorization": "Bearer test"}
        if request_id is not None:
            expected["x-request-id"] = request_id
        assert kwargs["additional_headers"] == expected
        return peer

    monkeypatch.setattr(peer, "send", send)
    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(request_id=request_id) as session,
    ):
        assert session.info.request_id == used


@pytest.mark.parametrize("value", [123, "", "  ", "line\nbreak", "é"])
async def test_invalid_request_id_does_not_open_connection(monkeypatch, value):
    async def connect(*args, **kwargs):
        pytest.fail("Invalid settings must fail before opening a socket")

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="request_id"):
            async with client.realtime.connect(request_id=value):
                pass


class _LabelPeer(Peer):
    """Echoes the labels an update names, as Prism does."""

    echo = True
    refuse = False

    async def send(self, raw):
        event = json.loads(raw)
        if event["type"] != "session.update":
            return await super().send(raw)
        _VALIDATORS[event["type"]].validate(event)
        self.settings = event["session"]
        if self.refuse:
            error = {"code": "invalid_session", "scope": "event"}
            self.emit("error", error={**error, "owner": {"event_id": event["event_id"]}})
            return
        session = {"id": "session-1", "voice": "test"}
        if self.echo and "prism_response_metadata" in event["session"]:
            session["prism_response_metadata"] = event["session"]["prism_response_metadata"]
        self.emit("session.updated", prism_request_event_id=event["event_id"], session=session)


async def test_response_metadata_is_set_changed_and_shown(monkeypatch):
    peer = _LabelPeer()

    async def connect(endpoint, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    first = {"scenario": "refund", "step": "1"}
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(response_metadata=first) as session,
    ):
        assert peer.settings["prism_response_metadata"] == first
        assert session.info.response_metadata == first
        second = {"scenario": "refund", "step": "2"}
        await session.set_response_metadata(second)
        assert peer.settings == {"prism_response_metadata": second}
        assert session.info.response_metadata == second
        # A server that does not confirm the labels leaves none shown.
        peer.echo = False
        await session.set_response_metadata({"step": "3"})
        assert session.info.response_metadata is None


@pytest.mark.parametrize("value", [[], "step", {"step": 3}])
async def test_invalid_response_metadata_is_not_sent(monkeypatch, value):
    peer = _LabelPeer()

    async def connect(endpoint, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client:
        with pytest.raises(RimeInputError, match="response_metadata"):
            async with client.realtime.connect(response_metadata=value):
                pass
        async with client.realtime.connect() as session:
            settings = peer.settings
            with pytest.raises(RimeInputError, match="response_metadata"):
                await session.set_response_metadata(value)
            assert peer.settings is settings


async def test_a_refused_label_change_raises_invalid_session(monkeypatch):
    peer = _LabelPeer()

    async def connect(endpoint, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with (
        Rime(api_key="test") as client,
        client.realtime.connect(response_metadata={"step": "1"}) as session,
    ):
        peer.refuse = True
        with pytest.raises(r.RimeRealtimeError) as refusal:
            await session.set_response_metadata({"step": "2"})
        assert refusal.value.fault.code == "invalid_session"
        assert session.info.response_metadata == {"step": "1"}
