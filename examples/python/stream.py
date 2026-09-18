"""Run: uv run --project python examples/python/stream.py"""

import asyncio
from pathlib import Path

from rime_sdk import Rime


async def text():
    yield "Hello. "
    yield "This text arrives in separate chunks. "
    yield "The SDK detects sentence boundaries."


async def main():
    async with Rime() as client:
        async with client.tts.stream(text()) as audio:
            with Path("speech.pcm").open("wb") as output:
                async for chunk in audio:
                    output.write(chunk)
            print(f"Wrote mono 24 kHz signed 16-bit PCM; request_id={audio.request_id}")


if __name__ == "__main__":
    asyncio.run(main())
