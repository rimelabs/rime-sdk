# /// script
# requires-python = ">=3.11"
# dependencies = ["rimelabs-sdk==0.1.0a19"]
# ///
"""Check your setup without a microphone: send a recorded question to Prism, print the
reply text, and save the reply audio to reply.wav.

Run: RIME_API_KEY=... uv run check.py
"""

import asyncio
import os
import sys
import wave

from rimelabs_sdk import Rime
from rimelabs_sdk.realtime import (
    AudioChunk,
    AudioDelta,
    FaultEvent,
    ResponseEnded,
    ResponseStarted,
    RimeRealtimeError,
    SpeechStarted,
    SpeechStopped,
    TextDelta,
)

ENDPOINT = os.getenv("PRISM_URL", "wss://prism.api.rime.ai/v1/realtime")
INSTRUCTIONS = (
    "You answer the phone for Juniper Cafe. It's open from 8 a.m. to 4 p.m. "
    "every day. Answer in one short sentence."
)
FRAME = 1280  # 40 ms of 16 kHz mono PCM16


async def send_call_audio(session, question):
    # Send the question at speaking pace, then silence, as a live call would.
    offset = 0
    while True:
        frame = question[offset : offset + FRAME].ljust(FRAME, b"\0")
        await session.send_audio(AudioChunk(data=frame))
        offset += FRAME
        await asyncio.sleep(0.04)


async def main(path):
    with wave.open(path, "rb") as recording:
        if (recording.getframerate(), recording.getnchannels(), recording.getsampwidth()) != (
            16000,
            1,
            2,
        ):
            raise SystemExit(f"{path} must be 16 kHz mono 16-bit PCM WAV")
        question = recording.readframes(recording.getnframes())
    async with (
        asyncio.timeout(60),
        Rime() as client,
        client.realtime.connect(
            endpoint=ENDPOINT, voice="astra", instructions=INSTRUCTIONS
        ) as session,
        asyncio.TaskGroup() as tasks,
    ):
        sending = tasks.create_task(send_call_audio(session, question))
        with wave.open("reply.wav", "wb") as reply:
            reply.setnchannels(1)
            reply.setsampwidth(2)
            reply.setframerate(24000)  # Prism replies in 24 kHz mono PCM16
            async for event in session.events:
                payload = event.payload
                if isinstance(payload, SpeechStarted):
                    print("Caller started speaking")
                elif isinstance(payload, SpeechStopped):
                    print("Caller stopped speaking")
                elif isinstance(payload, ResponseStarted):
                    print("Prism: ", end="")
                elif isinstance(payload, TextDelta):
                    print(payload.delta, end="", flush=True)
                elif isinstance(payload, AudioDelta):
                    reply.writeframes(payload.audio.data)
                elif isinstance(payload, FaultEvent):
                    raise RimeRealtimeError(payload.error)
                elif isinstance(payload, ResponseEnded):
                    print()
                    if payload.status != "completed":
                        raise RuntimeError(f"Reply {payload.status}: {payload.reason}")
                    break
            else:
                raise ConnectionError("The session closed before the reply ended")
            seconds = reply.getnframes() / 24000
        sending.cancel()
    print(f"Saved {seconds:.1f} seconds of audio to reply.wav")


asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "question.wav"))
