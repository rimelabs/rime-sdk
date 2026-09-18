import asyncio
import time

import grpc
import pytest
from fake_service import FakeService

from rimelabs_sdk import Rime, RimeInputError, _auth, _policy, _transport


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "https://coda.api.rime.ai",
        "host/path",
        "user@host",
        "host?query",
        "host#tag",
        " host",
        "host\n",
        "host:",
        "host:0",
        "host:65536",
        "host:-1",
        "host:1:2",
        "host:abc",
        "-host",
        "host..name",
        "a" * 64,
        42,
    ],
)
def test_invalid_endpoint(endpoint):
    with pytest.raises(RimeInputError, match="endpoint"):
        Rime(api_key="test-key", endpoint=endpoint)


def test_custom_endpoint_does_not_enable_unknown_model():
    with pytest.raises(RimeInputError, match="model"):
        Rime(api_key="test-key", model="mist", endpoint="mist.api.rime.ai")


@pytest.mark.parametrize("themis", [False, True])
@pytest.mark.parametrize("endpoint", ["Customer.Example", "Customer.Example:8443"])
async def test_clients_route_speech_and_discovery_independently(monkeypatch, themis, endpoint):
    original = _policy.POLICY
    audiences = []
    targets = []
    async with FakeService() as standard, FakeService() as custom:
        custom.payload = b"\x02\x00" * 2400
        port = 8443 if ":" in endpoint else 443
        routes = {
            "coda.api.rime.ai:443": standard.target,
            f"customer.example:{port}": custom.target,
        }

        def channel(policy):
            targets.append(policy.target)
            return grpc.aio.insecure_channel(routes[policy.target])

        async def exchange(key, policy):
            assert policy.exchange_url == original.exchange_url
            audiences.append(policy.audience)
            return _auth.Token(key, time.time() + 3600, policy.audience)

        monkeypatch.setattr(_transport, "make_channel", channel)
        monkeypatch.setattr(_auth, "exchange_key", exchange)
        if themis:
            monkeypatch.setattr(_auth.Credentials, "metadata", _auth.Credentials._themis_metadata)

        async def use(client):
            async with client.tts.stream("Hello.") as audio:
                result = b"".join([chunk async for chunk in audio])
            assert await client.languages.list() == ["en", "de"]
            assert await client.voices.list() == ["test-speaker"]
            return result

        async with (
            Rime(api_key="standard-key") as first,
            Rime(api_key="custom-key", endpoint=endpoint) as second,
        ):
            assert await asyncio.gather(use(first), use(second)) == [
                standard.payload,
                custom.payload,
            ]
        assert sorted(targets) == sorted(routes)
        assert standard.metadata[0]["authorization"] == "Bearer standard-key"
        assert custom.metadata[0]["authorization"] == "Bearer custom-key"
        assert standard.discovery_calls == custom.discovery_calls == 1
        assert sorted(audiences) == (["coda.api.rime.ai", "customer.example"] if themis else [])
        assert _policy.POLICY == original


async def test_custom_hostname_defaults_to_port_443_and_tls(monkeypatch):
    targets = []
    secure_channel = grpc.aio.secure_channel

    def capture(target, credentials, **kwargs):
        targets.append(target)
        return secure_channel(target, credentials, **kwargs)

    monkeypatch.setattr(grpc.aio, "secure_channel", capture)
    async with Rime(api_key="test-key", endpoint="Customer.Example") as client:
        channel = _transport.make_channel(client._policy)
        await channel.close()
    assert targets == ["customer.example:443"]
