"""Context terms and a custom lexicon through the public Prism session."""

import json
from contextlib import asynccontextmanager

import pytest
from test_realtime import _VALIDATORS, Peer

from rimelabs_sdk import PronunciationEntry, Rime, RimeInputError
from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client

KUBECTL = PronunciationEntry(spelling="kubectl", pronunciation='" k u b . k @ n . % t r\\ oU l')


class _ConfirmingPeer(Peer):
    """Confirms each update with the context terms and custom lexicon the last update named,
    as Prism does, or refuses every update with ``refusal``."""

    def __init__(self):
        super().__init__()
        self.updates = []
        self.view = {"id": "session-1", "voice": "test", "context_terms": [], "custom_lexicon": []}
        self.refusal = None

    async def send(self, raw):
        event = json.loads(raw)
        if event["type"] != "session.update":
            return await super().send(raw)
        _VALIDATORS[event["type"]].validate(event)
        self.updates.append(event["session"])
        if self.refusal is not None:
            owner = {"event_id": event["event_id"]}
            self.emit("error", error={"code": self.refusal, "scope": "event", "owner": owner})
            return
        for name in ("context_terms", "custom_lexicon"):
            if name in event["session"]:
                self.view[name] = event["session"][name]
        self.emit("session.updated", prism_request_event_id=event["event_id"], session=self.view)


@asynccontextmanager
async def opened(monkeypatch, peer, **options):
    async def connect(endpoint, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    async with Rime(api_key="test") as client, client.realtime.connect(**options) as session:
        yield session


async def test_context_terms_and_lexicon_are_sent_and_read_back(monkeypatch):
    peer = _ConfirmingPeer()
    async with opened(
        monkeypatch, peer, context_terms=["Skyvale", "Doctor Chen"], custom_lexicon=[KUBECTL]
    ) as session:
        assert peer.updates[0]["context_terms"] == ["Skyvale", "Doctor Chen"]
        assert peer.updates[0]["custom_lexicon"] == [
            {"spelling": "kubectl", "pronunciation": '" k u b . k @ n . % t r\\ oU l'}
        ]
        assert session.info.context_terms == ("Skyvale", "Doctor Chen")
        assert session.info.custom_lexicon == (KUBECTL,)

        info = await session.update(context_terms=["Velkora"])
        assert peer.updates[1] == {"context_terms": ["Velkora"]}
        assert info.context_terms == ("Velkora",)
        assert info.custom_lexicon == (KUBECTL,)

        info = await session.update(custom_lexicon=[])
        assert peer.updates[2] == {"custom_lexicon": []}
        assert info.custom_lexicon == ()
        assert session.info == info


async def test_omitted_settings_stay_out_of_the_request(monkeypatch):
    peer = Peer()
    async with opened(monkeypatch, peer) as session:
        assert "context_terms" not in peer.settings
        assert "custom_lexicon" not in peer.settings
        # A Prism release without these fields does not publish them.
        assert session.info.context_terms is None
        assert session.info.custom_lexicon is None


@pytest.mark.parametrize("code", ["invalid_client_event", "invalid_session", "turn_failed"])
async def test_prism_refusal_reaches_the_caller_unchanged(monkeypatch, code):
    peer = _ConfirmingPeer()
    async with opened(monkeypatch, peer, context_terms=["Skyvale"]) as session:
        peer.refusal = code
        with pytest.raises(r.RimeRealtimeError) as refusal:
            await session.update(context_terms=["Velkora"], custom_lexicon=[KUBECTL])
        assert refusal.value.fault.code == code
        assert refusal.value.fault.scope == "event"
        assert session.info.context_terms == ("Skyvale",)
        assert session.info.custom_lexicon == ()


@pytest.mark.parametrize(
    "settings",
    [
        {"context_terms": "Skyvale"},
        {"context_terms": ["Skyvale", 1]},
        {"context_terms": ["\ud800"]},
        {"custom_lexicon": [{"spelling": "kubectl", "pronunciation": "k"}]},
        {"custom_lexicon": [PronunciationEntry(spelling="\ud800", pronunciation="k")]},
    ],
)
async def test_malformed_values_fail_before_sending(monkeypatch, settings):
    peer = _ConfirmingPeer()
    async with opened(monkeypatch, peer) as session:
        with pytest.raises(RimeInputError):
            await session.update(**settings)
        assert len(peer.updates) == 1
