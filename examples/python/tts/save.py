"""Save speech as a WAV file. No audio hardware is required."""

import asyncio
import wave
from pathlib import Path

from rimelabs_sdk import Rime


async def main(output=Path("speech.wav")):
    async with (
        Rime(timeout=60) as client,
        client.tts.stream(
            "Your appointment is confirmed for tomorrow at ten in the morning."
        ) as audio,
    ):
        with wave.open(str(output), "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(24000)
            async for chunk in audio:
                wav.writeframesraw(chunk)
        print(f"Saved {output}; request_id={audio.request_id}")


if __name__ == "__main__":
    asyncio.run(main())
