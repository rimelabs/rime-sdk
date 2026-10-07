"""Connection setup for Prism. No TTS routing or credential exchange is used."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake, InvalidStatus, InvalidURI

from .._errors import (
    RimeAuthenticationError,
    RimeInputError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from ._protocol import SessionSettings
from ._session import RealtimeSession
from ._types import RealtimeTimeouts, ToolDefinition

if TYPE_CHECKING:
    from .._client import Rime


_DEFAULT_TIMEOUTS = RealtimeTimeouts()


class Realtime:
    """Create conversations through ``Rime.realtime.connect``."""

    def __init__(self, client: Rime):
        self._client = client
        self._sessions: set[RealtimeSession] = set()
        self._opening: set[asyncio.Task] = set()

    @asynccontextmanager
    async def connect(
        self,
        *,
        endpoint: str,
        model: Literal["prism"] = "prism",
        voice: str | None = None,
        instructions: str | None = None,
        tools: Sequence[ToolDefinition] = (),
        interrupt_on_speech: bool = True,
        timeouts: RealtimeTimeouts = _DEFAULT_TIMEOUTS,
    ) -> AsyncIterator[RealtimeSession]:
        """Open and initialize a session. Supply the full /v1/realtime WebSocket URL.

        Credentials use Authorization: Bearer with the Rime API key. The endpoint
        must support this handshake. Use ws:// only for a local development server.
        Settings are fixed for the lifetime of the session.
        """
        self._client._check_loop()
        self._client._check_open()
        url = urlsplit(endpoint)
        if (
            url.scheme not in ("ws", "wss")
            or not url.hostname
            or url.username
            or url.password
            or url.fragment
            or url.path.rstrip("/") != "/v1/realtime"
        ):
            raise RimeInputError(
                "endpoint must be a full ws:// or wss:// URL ending in /v1/realtime"
            )
        if url.scheme == "ws" and url.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise RimeInputError("Use wss:// outside localhost to protect credentials")
        if model != "prism":
            raise RimeInputError("The realtime model must be prism")
        settings: SessionSettings = {
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm16",
            "turn_detection": {"interrupt_response": interrupt_on_speech},
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    },
                }
                for tool in tools
            ],
        }
        for name, value in (("voice", voice), ("instructions", instructions)):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise RimeInputError(f"{name} must be nonblank")
                if name == "voice":
                    settings["voice"] = value
                else:
                    settings["instructions"] = value
        if any(not tool.name.strip() for tool in tools) or len(
            {tool.name for tool in tools}
        ) != len(tools):
            raise RimeInputError("Tool names must be nonblank and unique")
        try:
            # Copy mutable schemas before an await, so callers cannot change initialization.
            settings = copy.deepcopy(settings)
            json.dumps(settings, allow_nan=False)
        except (ValueError, TypeError):
            raise RimeInputError("Tool schemas must contain finite JSON values") from None

        async def open_session() -> RealtimeSession:
            session = None
            try:
                try:
                    socket = await connect(
                        endpoint,
                        additional_headers=self._client._credentials.realtime_headers(),
                        open_timeout=timeouts.connect_s,
                        close_timeout=2,
                        max_size=1024 * 1024,
                        max_queue=16,
                        proxy=None,
                    )
                except InvalidStatus as error:
                    if error.response.status_code in (401, 403):
                        raise RimeAuthenticationError(
                            "Realtime endpoint rejected credentials"
                        ) from None
                    raise RimeUnavailableError(
                        "Realtime endpoint rejected the connection"
                    ) from None
                except TimeoutError:
                    raise RimeTimeoutError("Realtime connection timed out") from None
                except OSError:
                    raise RimeUnavailableError("Realtime endpoint is unavailable") from None
                except (InvalidHandshake, InvalidURI, ValueError):
                    raise RimeUnavailableError("Realtime handshake failed") from None
                session = RealtimeSession(socket, timeouts)
                self._sessions.add(session)
                await session._initialize(settings)
                self._client._check_open()
            except BaseException:
                if session is not None:
                    try:
                        await session.close()
                    finally:
                        self._sessions.discard(session)
                raise
            return session

        opening = asyncio.create_task(open_session(), name="rime:realtime-connect")
        self._opening.add(opening)
        try:
            try:
                session = await opening
            finally:
                self._opening.discard(opening)
            yield session
        finally:
            # Cancellation can prevent delivery of a successful opening result.
            if opening.done() and not opening.cancelled() and opening.exception() is None:
                session = opening.result()
                try:
                    await session.close()
                finally:
                    self._sessions.discard(session)

    async def _close(self) -> None:
        opening = list(self._opening)
        for task in opening:
            task.cancel()
        await asyncio.gather(*opening, return_exceptions=True)
        await asyncio.gather(*(s.close() for s in list(self._sessions)))
