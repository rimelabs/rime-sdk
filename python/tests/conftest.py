import json

import pytest
from fake_service import FakeService

from rimelabs_sdk import Rime, _client, _native


@pytest.fixture
async def setup(monkeypatch):
    async with FakeService() as service:
        monkeypatch.setattr(
            _client,
            "_native_factory",
            lambda config: _native.NativeClient.testing(
                config,
                service.target,
                json.dumps(
                    {"first_audio_timeout": 0.2, "progress_timeout": 0.2, "cleanup_timeout": 0.1}
                ),
            ),
        )
        async with Rime(api_key="test-key") as client:
            yield service, client
