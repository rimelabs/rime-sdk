import time
from dataclasses import replace

import grpc
import pytest
from fake_service import FakeService

from rime_sdk import Rime, _auth, _policy, _transport


@pytest.fixture
async def setup(monkeypatch):
    async with FakeService() as service:
        policy = replace(
            _policy.POLICY, target=service.target, first_audio_timeout=0.2, progress_timeout=0.2
        )
        monkeypatch.setattr(_policy, "POLICY", policy)
        monkeypatch.setattr(
            _transport, "make_channel", lambda p: grpc.aio.insecure_channel(p.target)
        )

        async def exchange(key, policy):
            return _auth.Token("test-token", time.time() + 3600, policy.audience)

        monkeypatch.setattr(_auth, "exchange_key", exchange)
        async with Rime(api_key="test-key") as client:
            yield service, client
