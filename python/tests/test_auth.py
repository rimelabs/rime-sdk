import json

import httpx
import pytest

from rime_sdk import (
    RimeAuthenticationError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeUnavailableError,
    _auth,
    _policy,
)


@pytest.mark.parametrize(
    "status,body,error",
    [
        (200, {"access_token": "token", "expires_in": 60, "audience": "coda.api.rime.ai"}, None),
        (401, {"detail": "secret"}, RimeAuthenticationError),
        (403, {}, RimePermissionError),
        (429, {"detail": "secret"}, RimeResourceLimitError),
        (500, {"detail": "secret"}, RimeUnavailableError),
        (502, {"detail": "secret"}, RimeUnavailableError),
        (503, {"detail": "secret"}, RimeUnavailableError),
        (504, {"detail": "secret"}, RimeUnavailableError),
        (
            200,
            {"access_token": "token", "expires_in": 0, "audience": "coda.api.rime.ai"},
            RimeAuthenticationError,
        ),
        (
            200,
            {"access_token": "token", "expires_in": 60, "audience": "wrong"},
            RimeAuthenticationError,
        ),
        (
            200,
            {"access_token": "token", "expires_in": True, "audience": "coda.api.rime.ai"},
            RimeAuthenticationError,
        ),
    ],
)
async def test_private_exchange_contract(monkeypatch, status, body, error):
    original = httpx.AsyncClient

    def handler(request):
        assert request.headers["authorization"] == "Api-Key local-test-secret"
        assert json.loads(request.content) == {"audience": "coda.api.rime.ai"}
        return httpx.Response(status, json=body)

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
    )
    if error:
        with pytest.raises(error) as caught:
            await _auth.exchange_key("local-test-secret", _policy.POLICY)
        assert "secret" not in str(caught.value)
    else:
        token = await _auth.exchange_key("local-test-secret", _policy.POLICY)
        assert token.value == "token"


async def test_exchange_response_limit(monkeypatch):
    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 65537)),
            **kwargs,
        ),
    )
    with pytest.raises(RimeAuthenticationError):
        await _auth.exchange_key("secret", _policy.POLICY)
