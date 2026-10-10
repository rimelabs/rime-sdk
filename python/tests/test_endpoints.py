import asyncio
import time

import grpc
import pytest
from fake_service import FakeService

from rimelabs_sdk import Rime, RimeInputError, _auth
from rimelabs_sdk.tts import _policy, _transport


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


@pytest.mark.parametrize("model", ["mist", "mistv2", "mistv4", "arcana", "unknown", "", 42, []])
@pytest.mark.parametrize("endpoint", [None, "mist.api.rime.ai"])
def test_unsupported_model(model, endpoint):
    with pytest.raises(RimeInputError, match="model"):
        Rime(api_key="test-key", model=model, endpoint=endpoint)


@pytest.mark.parametrize("themis", [False, True])
@pytest.mark.parametrize("model,default_voice", [("coda", "clementine"), ("mistv3", "astra")])
@pytest.mark.parametrize("endpoint", ["Customer.Example", "Customer.Example:8443"])
async def test_clients_route_speech_and_discovery_independently(
    monkeypatch, themis, model, default_voice, endpoint
):
    original = _policy.POLICY
    audiences = []
    targets = []
    async with FakeService() as standard, FakeService() as mist, FakeService() as custom:
        mist.payload = b"\x02\x00" * 2400
        custom.payload = b"\x03\x00" * 2400
        mist.supported_speakers = ["astra"]
        custom.supported_speakers = ["customer-voice"]
        port = 8443 if ":" in endpoint else 443
        routes = {
            "coda.api.rime.ai:443": standard.target,
            "mist.api.rime.ai:443": mist.target,
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

        async def use(client, service, voice):
            async with client.tts.synthesize("Hello.") as audio:
                result = b"".join([chunk async for chunk in audio])
            assert service.calls[0][0].header.speaker == voice
            assert service.calls[0][0].header.audio_parameters.sampling_rate == 24000
            assert await client.languages.list() == ["en", "de"]
            assert await client.voices.list() == service.supported_speakers
            release = asyncio.Event()

            async def source():
                yield "First sentence. The next sentence "
                await release.wait()
                yield "is here."

            async with client.tts.stream(source(), voice="explicit-voice") as audio:
                first = await asyncio.wait_for(anext(audio), 2)
                release.set()
                assert first + b"".join([chunk async for chunk in audio]) == service.payload * 2
            assert service.calls[1][0].header.speaker == "explicit-voice"
            return result

        async with (
            Rime(api_key="standard-key") as first,
            Rime(api_key="mist-key", model="mistv3") as second,
            Rime(api_key="custom-key", model=model, endpoint=endpoint) as third,
        ):
            assert await asyncio.gather(
                use(first, standard, "clementine"),
                use(second, mist, "astra"),
                use(third, custom, default_voice),
            ) == [
                standard.payload,
                mist.payload,
                custom.payload,
            ]
        assert sorted(targets) == sorted(routes)
        assert standard.metadata[0]["authorization"] == "Bearer standard-key"
        assert mist.metadata[0]["authorization"] == "Bearer mist-key"
        assert custom.metadata[0]["authorization"] == "Bearer custom-key"
        assert standard.discovery_calls == mist.discovery_calls == custom.discovery_calls == 1
        assert sorted(audiences) == (
            ["coda.api.rime.ai", "customer.example", "mist.api.rime.ai"] if themis else []
        )
        assert _policy.POLICY == original


async def test_custom_hostname_defaults_to_port_443_and_tls(monkeypatch):
    targets = []
    secure_channel = grpc.aio.secure_channel

    def capture(target, credentials, **kwargs):
        targets.append(target)
        return secure_channel(target, credentials, **kwargs)

    monkeypatch.setattr(grpc.aio, "secure_channel", capture)
    async with Rime(api_key="test-key", endpoint="Customer.Example") as client:
        channel = _transport.make_channel(client.tts._policy)
        await channel.close()
    assert targets == ["customer.example:443"]
