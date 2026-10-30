"""Connection setup for Prism. No TTS routing or credential exchange is used."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake, InvalidStatus, InvalidURI

from .._errors import (
    RimeAuthenticationError,
    RimeError,
    RimeInputError,
    RimeResourceLimitError,
    RimeTimeoutError,
    RimeUnavailableError,
)
from ._protocol import SessionSettings, truncation_settings
from ._protocol import response_metadata as _response_metadata
from ._session import RealtimeSession
from ._settings import tool_settings
from ._types import Labels, RealtimeTimeouts, ToolDefinition, Truncation, TurnDetection

if TYPE_CHECKING:
    from .._client import Rime


_DEFAULT_TIMEOUTS = RealtimeTimeouts()
_DEFAULT_ENDPOINT = "wss://prism.api.rime.ai/v1/realtime"
_HTTP_DATE_PATTERNS = (
    re.compile(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} [A-Z][a-z]{2} [0-9]{4} "
        r"[0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    ),
    re.compile(
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
        r"[0-9]{2}-[A-Z][a-z]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT"
    ),
    re.compile(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) [A-Z][a-z]{2} (?:[0-9]{2}| [0-9]) "
        r"[0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}"
    ),
)


def _retry_after(values: list[str]) -> float | None:
    if len(values) != 1 or len(values[0]) > 256:
        return None
    value = values[0].strip()
    try:
        if value.isascii() and value.isdecimal():
            seconds = float(value)
        else:
            # Email date parsing alone accepts non-HTTP forms and timezone offsets.
            form = next(
                (i for i, pattern in enumerate(_HTTP_DATE_PATTERNS) if pattern.fullmatch(value)),
                None,
            )
            if form is None:
                return None
            date = parsedate_to_datetime(value).replace(tzinfo=UTC)
            now = time.time()
            if form == 1:
                # RFC 850 years more than 50 years ahead denote the previous century.
                current = datetime.fromtimestamp(now, UTC)
                cutoff = (current.year + 50, *current.timetuple()[1:6])
                year = cutoff[0] // 100 * 100 + int(value.split(" ")[1][-2:])
                if (year, *date.timetuple()[1:6]) > cutoff:
                    year -= 100
                date = date.replace(year=year)
            else:
                # The email parser also applies its year pivot to four-digit years.
                date = date.replace(year=int(value[12:16] if form == 0 else value[-4:]))
            seconds = max(0.0, date.timestamp() - now)
        return seconds if math.isfinite(seconds) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _handshake_error(error: InvalidStatus) -> RimeError:
    response = error.response
    status = response.status_code
    error_type: type[RimeError] = RimeUnavailableError
    reason = "Realtime endpoint rejected the connection"
    if status in (401, 403):
        error_type = RimeAuthenticationError
        reason = "Realtime endpoint rejected credentials"
    elif status == 429:
        error_type = RimeResourceLimitError
        reason = "Realtime connection or rate limit reached"
    elif status == 503:
        reason = "Realtime endpoint is unavailable"
    message = f"{reason} (HTTP {status})"
    # websockets bounds the HTTP body read; open_timeout bounds its duration.
    body = response.body
    if body and len(body) < 1024:
        try:
            detail = body.decode("utf-8").strip()
            if detail.startswith(("{", "[")):
                payload = json.loads(detail)
                detail = payload.get("error", {}).get("message")
            if (
                isinstance(detail, str)
                and not detail.lstrip().startswith("<")
                and all(ord(c) >= 32 or c in "\r\n\t" for c in detail)
            ):
                detail = " ".join(detail.split())
                if detail:
                    message += f": {detail}"
        except (UnicodeError, ValueError, AttributeError, RecursionError):
            pass
    request_ids = response.headers.get_all("x-request-id")
    request_id = request_ids[0] if len(request_ids) == 1 and request_ids[0] else None
    return error_type(
        message,
        request_id=request_id,
        status_code=status,
        retry_after_s=_retry_after(response.headers.get_all("retry-after")),
    )


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
        request_id: str | None = None,
        response_metadata: Labels | None = None,
        truncation: Truncation | None = None,
    ) -> AsyncIterator[RealtimeSession]:
        """Open and initialize a session on the public Prism endpoint.

        Override endpoint with a full /v1/realtime WebSocket URL for another deployment.
        Omitted turn timing settings use the server defaults.

        Credentials use Authorization: Bearer with the Rime API key. The endpoint
        must support this handshake. Use ws:// only for a local development server.
        Use session.update to change instructions, tools, voice, or interruption.

        request_id is sent as the upgrade's x-request-id. It must be nonblank printable ASCII, or
        connect raises RimeInputError. Prism replaces IDs longer than 128 characters.
        A gateway can replace shorter IDs too. info.request_id reports the ID Prism actually used.
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
        if truncation is not None:
            settings["truncation"] = truncation_settings(truncation)
        if turn_detection is not None:
            if not isinstance(turn_detection, TurnDetection):
                raise RimeInputError("turn_detection must be a TurnDetection value")
            if turn_detection.eagerness is not None:
                settings["turn_detection"]["eagerness"] = turn_detection.eagerness
            if turn_detection.end_of_turn_timeout_ms is not None:
                settings["turn_detection"]["prism_end_of_turn_timeout_ms"] = (
                    turn_detection.end_of_turn_timeout_ms
                )
        if response_metadata is not None:
            settings["prism_response_metadata"] = _response_metadata(response_metadata)
        if request_id is not None and (
            not isinstance(request_id, str)
            or not request_id.strip()
            or not all(" " <= character <= "~" for character in request_id)
        ):
            raise RimeInputError("request_id must be nonblank printable ASCII")
        for name, value in (("voice", voice), ("instructions", instructions)):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise RimeInputError(f"{name} must be nonblank")
                if name == "voice":
                    settings["voice"] = value
                else:
                    settings["instructions"] = value

        headers = self._client._credentials.realtime_headers()
        if request_id is not None:
            headers = {**headers, "x-request-id": request_id}

        async def open_session() -> RealtimeSession:
            session = None
            try:
                try:
                    socket = await connect(
                        endpoint,
                        additional_headers=headers,
                        open_timeout=timeouts.connect_s,
                        close_timeout=2,
                        max_size=1024 * 1024,
                        max_queue=16,
                        proxy=None,
                    )
                except InvalidStatus as error:
                    raise _handshake_error(error) from None
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
