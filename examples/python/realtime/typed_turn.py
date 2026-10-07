"""Run: uv run --project python examples/python/realtime/typed_turn.py"""

import asyncio
import os
from pathlib import Path

from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import (
    AudioDelta,
    FaultEvent,
    RealtimeSession,
    ResponseEnded,
    RimeRealtimeError,
    TextDelta,
)


async def save_reply(session: RealtimeSession) -> None:
    # Use a small local file to keep the example free of extra dependencies.
    with Path("reply.pcm").open("wb") as output:  # noqa: ASYNC230
        async for event in session.events:
            payload = event.payload
            if isinstance(payload, TextDelta):
                print(payload.delta, end="", flush=True)
            elif isinstance(payload, AudioDelta):
                output.write(payload.audio.data)
            elif isinstance(payload, FaultEvent):
                raise RimeRealtimeError(payload.error)
            elif isinstance(payload, ResponseEnded):
                if payload.status != "completed":
                    raise RuntimeError(f"Response {payload.status}: {payload.reason}")
                print("\nSaved reply.pcm: mono 24 kHz signed little-endian PCM16.")
                return
    raise RuntimeError("Session closed before the response ended")


async def main() -> None:
    # Bound the whole example, including generation after response creation.
    async with (
        asyncio.timeout(60),
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.getenv("PRISM_VOICE"),
            instructions="Give short, clear answers.",
        ) as session,
        asyncio.TaskGroup() as tasks,
    ):
        tasks.create_task(save_reply(session))
        await session.send_text("Hello! What can you help me with?")


if __name__ == "__main__":
    asyncio.run(main())
