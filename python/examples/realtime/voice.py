"""Talk to Prism through local audio devices. Use headphones; Ctrl-C exits."""

import argparse
import asyncio
import os

from audio_devices import Microphone, Speaker, sounddevice

from rimelabs_sdk import Rime
from rimelabs_sdk import realtime as r


async def converse(session, microphone, speaker, execute_tool=None):
    """The application displays events and owns any manual tool work."""
    from realtime.manual_tools import ManualTools

    async with asyncio.TaskGroup() as tasks:
        tools = ManualTools(session, tasks, execute_tool)

        def on_event(event):
            payload = event.payload
            if isinstance(payload, r.TranscriptFinal):
                print(f"You: {payload.text}", flush=True)
            elif isinstance(payload, r.TextDone) and payload.text:
                print(f"Prism: {payload.text}", flush=True)
            elif isinstance(payload, (r.FaultEvent, r.TranscriptFailed)):
                print(f"Prism: {payload.error.code}: {payload.error.message}")
            elif isinstance(payload, r.ResponseEnded) and payload.status in ("failed", "unknown"):
                raise RuntimeError(f"Prism response {payload.status}: {payload.reason}")
            tools.on_event(event)

        try:
            print("Ready. Speak into your microphone. Ctrl-C exits.", flush=True)
            await r.run_conversation(
                session, source=microphone, playback=speaker, on_event=on_event
            )
        finally:
            tools.cancel()


async def main(args, *, tools=(), execute_tool=None):
    async with (
        Rime() as client,
        client.realtime.connect(
            endpoint=os.getenv("PRISM_URL"),
            voice=os.getenv("PRISM_VOICE"),
            instructions=(
                "Give short answers in English. Use lookup_order for order questions."
                if tools
                else "Give short answers in English."
            ),
            tools=tools,
        ) as session,
    ):
        with (
            Speaker(args.output_device) as speaker,
            Microphone(args.input_device) as microphone,
        ):
            await converse(session, microphone, speaker, execute_tool)


def cli(*, tools=(), execute_tool=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-devices", action="store_true")
    parser.add_argument("--input-device", type=int)
    parser.add_argument("--output-device", type=int)
    args = parser.parse_args()
    if args.list_devices:
        print(sounddevice().query_devices())
        return
    try:
        asyncio.run(main(args, tools=tools, execute_tool=execute_tool))
    except KeyboardInterrupt:
        print("Voice session closed.")


if __name__ == "__main__":
    cli()
