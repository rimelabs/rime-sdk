"""Talk to Prism through local audio devices. Use headphones; Ctrl-C exits."""

import argparse
import asyncio
import os
from dataclasses import dataclass, field

from audio_devices import Microphone, Playback, Speaker, sounddevice

from rimelabs_sdk import Rime, RimeInputError
from rimelabs_sdk import realtime as r


@dataclass
class Reply:
    output: r.OutputRef | None = None
    playback: Playback | None = None
    interrupted: bool = False
    calls: list[r.ToolCall] = field(default_factory=list)


async def converse(session, microphone, speaker, execute_tool=None):
    """One event reader; input, playback receipts and tools run alongside it."""
    replies: dict[str, Reply] = {}

    async def report(value):
        try:
            await session.report_playback(value)
        except r.RimeRealtimeError as error:
            if error.fault.scope != "event" or error.fault.code != "truncate_not_current":
                raise
            print("A newer turn already owns playback; the old report was refused.")

    async def finish(event):
        reply = replies[event.response.response_id]
        played = 0
        if reply.playback:
            speaker.end(reply.playback)
            played = await reply.playback.done
        if not reply.interrupted:
            await report(r.PlaybackFinished(response=event.response, played_ms=played * 1000))
        if event.status in ("failed", "unknown"):
            raise RuntimeError(f"Prism response {event.status}: {event.reason}")
        if reply.calls and event.status == "completed":
            for call in reply.calls:
                if execute_tool is None:
                    raise RuntimeError("The example has no tool handler")
                result = await execute_tool(call)
                await session.submit_tool_result(call.call, result)
            # New speech supersedes this round. Record results, but do not speak over it.
            if not reply.interrupted:
                try:
                    await session.continue_reply(event.response)
                except RimeInputError:
                    if not reply.interrupted:
                        raise
                except r.RimeRealtimeError as error:
                    if (
                        error.fault.scope != "event"
                        or error.fault.code != "tool_continuation_unavailable"
                    ):
                        raise
        replies.pop(event.response.response_id, None)

    async def send_microphone():
        while True:
            await session.send_audio(r.AudioChunk(data=await microphone.read()))

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(send_microphone())
        print("Ready. Speak into your microphone. Ctrl-C exits.", flush=True)
        async for event in session.events:
            payload = event.payload
            if isinstance(payload, r.ResponseStarted):
                replies[payload.response.response_id] = Reply()
            elif isinstance(payload, r.MessageStarted):
                reply = replies[payload.output.response.response_id]
                reply.output = payload.output
                if reply.interrupted:
                    tasks.create_task(
                        report(r.PlaybackInterrupted(output=payload.output, audio_end_ms=0))
                    )
            elif isinstance(payload, r.TranscriptFinal):
                print(f"You: {payload.text}", flush=True)
            elif isinstance(payload, r.TextDone):
                if payload.text:
                    print(f"Prism: {payload.text}", flush=True)
            elif isinstance(payload, r.AudioDelta):
                reply = replies[payload.output.response.response_id]
                reply.output = payload.output
                if not reply.interrupted:
                    if reply.playback is None:
                        reply.playback = speaker.begin()
                    speaker.write(reply.playback, payload.audio.data)
            elif isinstance(payload, r.ToolCall):
                replies[payload.call.response_id].calls.append(payload)
            elif isinstance(payload, r.SpeechStarted) and session.info.interrupt_on_speech:
                speaker.interrupt()
                for reply in replies.values():
                    if not reply.interrupted:
                        reply.interrupted = True
                        if reply.output:
                            played = reply.playback.done.result() if reply.playback else 0
                            tasks.create_task(
                                report(
                                    r.PlaybackInterrupted(
                                        output=reply.output,
                                        audio_end_ms=int(played * 1000),
                                    )
                                )
                            )
            elif isinstance(payload, r.ResponseEnded):
                # Cancellation is normal when the caller interrupts; keep the session open.
                tasks.create_task(finish(payload))
            elif isinstance(payload, (r.FaultEvent, r.TranscriptFailed)):
                print(f"Prism: {payload.error.code}: {payload.error.message}")
                if payload.error.scope == "session":
                    raise r.RimeRealtimeError(payload.error)
        raise ConnectionError("Prism closed the event stream")


async def main(args, *, tools=(), execute_tool=None):
    async with (
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ["PRISM_URL"],
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
