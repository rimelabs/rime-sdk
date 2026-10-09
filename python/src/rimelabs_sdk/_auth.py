"""Private, assumed Themis exchange; see docs/authentication.md."""

import asyncio
import math
import time
from dataclasses import dataclass, field

import httpx

from ._errors import (
    RimeAuthenticationError,
    RimeError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
)


@dataclass(frozen=True)
class Token:
    value: str = field(repr=False)
    expires_at: float
    audience: str


async def exchange_key(key, policy):
    try:
        async with httpx.AsyncClient(timeout=policy.auth_timeout, follow_redirects=False) as client:  # noqa: SIM117
            async with client.stream(
                "POST",
                policy.exchange_url,
                headers={"Authorization": "Api-Key " + key},
                json={"audience": policy.audience},
            ) as response:
                if response.status_code == 403:
                    raise RimePermissionError("Credential exchange denied permission")
                if response.status_code == 429:
                    raise RimeResourceLimitError("Credential exchange rate limit exceeded")
                if 500 <= response.status_code < 600:
                    raise RimeUnavailableError("Credential exchange service unavailable")
                if response.status_code != 200:
                    raise RimeAuthenticationError("Credential exchange rejected the API key")
                data = bytearray()
                async for part in response.aiter_bytes():
                    data.extend(part)
                    if len(data) > 65536:
                        raise RimeAuthenticationError("Credential response exceeds the size limit")
                import json

                body = json.loads(data)
        value = body["access_token"]
        lifetime = body["expires_in"]
        audience = body["audience"]
        if (
            not isinstance(value, str)
            or not value
            or any(not 33 <= ord(c) <= 126 for c in value)
            or isinstance(lifetime, bool)
            or not isinstance(lifetime, (int, float))
            or not math.isfinite(lifetime)
            or lifetime <= 0
            or audience != policy.audience
        ):
            raise RimeAuthenticationError("Credential exchange returned an invalid token")
        return Token(value, time.time() + lifetime, audience)
    except RimeError:
        raise
    except httpx.TimeoutException:
        raise RimeTimeoutError("Credential exchange timed out") from None
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        # HTTP exceptions can contain the authorization header. Do not chain them.
        raise RimeAuthenticationError("Credential exchange failed") from None


class Credentials:
    def __init__(self, key, policy):
        self._key = key
        self._policy = policy
        self._token: Token | None = None
        self._refresh: asyncio.Task[Token] | None = None
        self._refresh_at = 0.0
        self._closed = False

    async def _fetch(self):
        try:
            async with asyncio.timeout(self._policy.auth_timeout):
                token = await exchange_key(self._key, self._policy)
        except TimeoutError:
            raise RimeTimeoutError("Credential acquisition timed out") from None
        if (
            not isinstance(token, Token)
            or not token.value
            or token.expires_at <= time.time()
            or token.audience != self._policy.audience
        ):
            raise RimeAuthenticationError("Credential exchange returned an invalid token")
        self._token = token
        self._refresh_at = token.expires_at - min(30, (token.expires_at - time.time()) / 10)
        return token

    def authorization(self) -> str:
        """Direct service authentication independent of TTS exchange policy."""
        if self._closed:
            raise RimeAuthenticationError("Credentials are closed")
        return "Bearer " + self._key

    def realtime_headers(self) -> dict[str, str]:
        """Direct API-key authentication, independent of TTS token policy."""
        if self._closed:
            raise RimeAuthenticationError("Credentials are closed")
        return {"Authorization": self.authorization()}

    async def metadata(self):
        # TEMPORARY until Themis is ready: replace this body with the call below.
        # return await self._themis_metadata()
        if self._closed:
            raise RimeAuthenticationError("Credentials are closed")
        return (("authorization", self.authorization()),)

    async def _themis_metadata(self):
        if self._closed:
            raise RimeAuthenticationError("Credentials are closed")
        token = self._token
        if token is None or time.time() >= self._refresh_at:
            if self._refresh is None or self._refresh.done():
                self._refresh = asyncio.create_task(self._fetch(), name="rime:refresh")
                self._refresh.add_done_callback(lambda t: None if t.cancelled() else t.exception())
            token = await asyncio.shield(self._refresh)
        assert token is not None
        return (("authorization", "Bearer " + token.value),)

    async def close(self):
        self._closed = True
        if self._refresh:
            self._refresh.cancel()
            await asyncio.gather(self._refresh, return_exceptions=True)
        self._refresh = None
        self._token = None
        self._refresh_at = 0.0
        self._key = ""
