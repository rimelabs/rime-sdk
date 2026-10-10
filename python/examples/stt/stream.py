"""Transcribe one headerless PCM16 mono 16 kHz file through the public SDK."""

import argparse
import asyncio
from pathlib import Path

from rimelabs_sdk import Rime, TranscriptionMode


async def chunks(path: Path):
    with path.open("rb") as audio:
        while data := await asyncio.to_thread(audio.read, 3200):
            yield data


async def main(path: Path, language: str, mode: TranscriptionMode = TranscriptionMode.WRITTEN):
    async with (
        Rime() as client,
        client.stt.stream(chunks(path), language=language, mode=mode, timeout=120) as stream,
    ):
        async for update in stream:
            print(f"{update.kind}: {update.text}")
        print(f"request_id: {stream.request_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audio", type=Path, help="Raw PCM16 little-endian mono 16 kHz file")
    parser.add_argument(
        "--language", required=True, help="Spoken BCP-47 language, for example en or es"
    )
    parser.add_argument(
        "--mode", choices=[mode.value for mode in TranscriptionMode], default="written"
    )
    args = parser.parse_args()
    asyncio.run(main(args.audio, args.language, TranscriptionMode(args.mode)))
