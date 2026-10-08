"""Send recorded speech at capture speed and save the spoken reply as WAV."""

import argparse
import asyncio
import os
import wave
from pathlib import Path

from rimelabs_sdk import Rime
from rimelabs_sdk import realtime as r

FIXTURE = Path(__file__).resolve().parents[2] / "audio/france.wav"


async def main(source=FIXTURE, output=Path("reply.wav")):
    with wave.open(str(source), "rb") as wav:
        if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (
            1,
            2,
            16000,
        ):
            raise ValueError("Input must be a 16 kHz mono PCM16 WAV file")
        data = wav.readframes(wav.getnframes())
    if not data:
        raise ValueError("The input recording is empty")
    async with (
        asyncio.timeout(60),
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
            voice=os.getenv("PRISM_VOICE"),
            instructions="Give short, clear answers.",
        ) as session,
    ):

        async def send():
            # Continue with silence so the server can detect the end of speech.
            offset = 0
            while True:
                await session.send_audio(
                    r.AudioChunk(data=data[offset : offset + 1280].ljust(1280, b"\0"))
                )
                offset += 1280
                await asyncio.sleep(0.04)

        async with asyncio.TaskGroup() as tasks:
            sending = tasks.create_task(send())
            with wave.open(str(output), "wb") as wav:
                wav.setnchannels(1)
                wav.setsampwidth(2)
                wav.setframerate(24000)
                async for event in session.events:
                    payload = event.payload
                    if isinstance(payload, r.AudioDelta):
                        wav.writeframesraw(payload.audio.data)
                    elif isinstance(payload, r.TranscriptFinal):
                        print(f"You: {payload.text}")
                    elif isinstance(payload, r.TextDone):
                        print(f"Prism: {payload.text}")
                    elif isinstance(payload, r.FaultEvent):
                        raise r.RimeRealtimeError(payload.error)
                    elif isinstance(payload, r.ResponseEnded):
                        if payload.status != "completed":
                            raise RuntimeError(f"Response {payload.status}: {payload.reason}")
                        sending.cancel()
                        break
                else:
                    raise ConnectionError("Session closed before the reply ended")
    # Saving a file is not playback. Do not send a playback receipt here.
    print(f"Saved {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=FIXTURE)
    parser.add_argument("--output", type=Path, default=Path("reply.wav"))
    args = parser.parse_args()
    asyncio.run(main(args.input, args.output))
