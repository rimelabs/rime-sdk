import asyncio
import json

import pytest
from fake_service import FakeService

from rimelabs_sdk import Rime, RimeInputError, _client, _native


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


@pytest.mark.parametrize("endpoint", ["Customer.Example", "Customer.Example:8443"])
async def test_clients_route_speech_and_discovery_independently(monkeypatch, endpoint):
    async with FakeService() as standard, FakeService() as custom:
        custom.payload = b"\x02\x00" * 2400

        def factory(config):
            target = custom.target if json.loads(config)["endpoint"] else standard.target
            return _native.NativeClient.testing(config, target, "{}")

        monkeypatch.setattr(_client, "_native_factory", factory)

        async def use(client):
            result = b"".join([chunk async for chunk in client.tts.stream("Hello.")])
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
        assert standard.metadata[0]["authorization"] == "Bearer standard-key"
        assert custom.metadata[0]["authorization"] == "Bearer custom-key"
