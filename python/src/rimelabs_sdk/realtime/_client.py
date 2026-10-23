"""Connection setup for Prism. No TTS routing or credential exchange is used."""

from __future__ import annotations

import asyncio
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
from ._settings import tool_settings
from ._types import RealtimeTimeouts, ToolDefinition, TurnDetection

if TYPE_CHECKING:
    from .._client import Rime


_DEFAULT_TIMEOUTS = RealtimeTimeouts()
_DEFAULT_ENDPOINT = "wss://prism.api.rime.ai/v1/realtime"


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
        endpoint: str | None = None,
        model: Literal["prism"] = "prism",
        voice: str | None = None,
        instructions: str | None = None,
        tools: Sequence[ToolDefinition] = (),
        interrupt_on_speech: bool = True,
        turn_detection: TurnDetection | None = None,
        timeouts: RealtimeTimeouts = _DEFAULT_TIMEOUTS,
    ) -> AsyncIterator[RealtimeSession]:
        """Open and initialize a session on the public Prism endpoint.

        Override endpoint with a full /v1/realtime WebSocket URL for another deployment.
        Omitted turn timing settings use the server defaults.

        Credentials use Authorization: Bearer with the Rime API key. The endpoint
        must support this handshake. Use ws:// only for a local development server.
        Use session.update to change instructions, tools, voice, or interruption.
        """
        self._client._check_loop()
        self._client._check_open()
        endpoint = _DEFAULT_ENDPOINT if endpoint is None else endpoint
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
            "tools": tool_settings(tools),
        }
        if turn_detection is not None:
            if not isinstance(turn_detection, TurnDetection):
                raise RimeInputError("turn_detection must be a TurnDetection value")
            if turn_detection.eagerness is not None:
                settings["turn_detection"]["eagerness"] = turn_detection.eagerness
            if turn_detection.end_of_turn_timeout_ms is not None:
                settings["turn_detection"]["prism_end_of_turn_timeout_ms"] = (
                    turn_detection.end_of_turn_timeout_ms
                )
        for name, value in (("voice", voice), ("instructions", instructions)):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise RimeInputError(f"{name} must be nonblank")
                if name == "voice":
                    settings["voice"] = value
                else:
                    settings["instructions"] = value

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
                    await self._close_session(session)
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
                await self._close_session(session)

    async def _close_session(self, session: RealtimeSession) -> None:
        try:
            await session.close()
        finally:
            # close() shields shutdown, which can outlive its cancelled caller.
            shutdown = session._close_task
            if shutdown is not None and not shutdown.done():

                def finished(task: asyncio.Task[None]) -> None:
                    self._sessions.discard(session)
                    if not task.cancelled():
                        task.exception()

                shutdown.add_done_callback(finished)
            else:
                self._sessions.discard(session)

    async def _close(self) -> None:
        opening = list(self._opening)
        for task in opening:
            task.cancel()
        await asyncio.gather(*opening, return_exceptions=True)
        await asyncio.gather(*(s.close() for s in list(self._sessions)))
