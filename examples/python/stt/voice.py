"""Press Enter to speak, then Enter to hear the recognized text through Rime TTS."""

import argparse
import asyncio
import contextlib
import shutil
import signal
import sys
import tempfile
from pathlib import Path

from rimelabs_sdk import Rime, RimeError, TranscriptionMode


def pcm_options(rate):
    """Headerless signed little-endian PCM16, mono, at the given frame rate."""
    return ["-t", "raw", "-e", "signed-integer", "-b", "16", "-L", "-r", str(rate), "-c", "1"]


async def close_process(process):
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 2)
        except TimeoutError:
            pass
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()


async def capture(input_path=None, lines=None):
    """Read a file or capture after STT acceptance; EOF commits only clean input."""
    process = await asyncio.create_subprocess_exec(
        "sox",
        "-q",
        "--buffer",
        "1280",
        str(Path(input_path).resolve()) if input_path else "-d",
        *pcm_options(16000),
        "-",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
    )
    stopping = False

    async def stop_on_enter():
        nonlocal stopping
        print("Listening. Press Enter to finish.", flush=True)
        await lines.readline()
        stopping = True
        with contextlib.suppress(ProcessLookupError):
            process.send_signal(signal.SIGINT)
        try:
            await asyncio.wait_for(process.wait(), 2)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise RuntimeError("Microphone did not stop cleanly") from None

    stop = asyncio.create_task(stop_on_enter()) if lines is not None else None
    try:
        while data := await process.stdout.read(1280):
            yield data
        code = await process.wait()
        if stop and stop.done():
            await stop
        if code != 0 and not (stopping and code == -signal.SIGINT):
            raise RuntimeError(f"SoX capture failed (exit {code}); check its error above")
        if lines is not None and not stopping:
            raise RuntimeError("Microphone stopped before the turn was finished")
    finally:
        if stop:
            stop.cancel()
            await asyncio.gather(stop, return_exceptions=True)
        await close_process(process)


async def synthesize(client, text, language, voice, output_path=None):
    process = await asyncio.create_subprocess_exec(
        "sox",
        "-q",
        *pcm_options(24000),
        "-",
        str(Path(output_path).resolve()) if output_path else "-d",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
    )
    try:
        async with client.tts.stream(text, language=language, voice=voice, timeout=60) as audio:
            async for chunk in audio:
                process.stdin.write(chunk)
                await process.stdin.drain()
        process.stdin.close()
        await process.stdin.wait_closed()
        if code := await asyncio.wait_for(process.wait(), 65):
            raise RuntimeError(f"SoX playback failed (exit {code}); check its error above")
        return audio.request_id
    finally:
        await close_process(process)


async def speak(client, text, language, voice, output_path=None):
    # A completed WAV lets the macOS player manage Bluetooth device format changes.
    if sys.platform == "darwin" and output_path is None:
        with tempfile.TemporaryDirectory(prefix="rime-voice-") as directory:
            reply = str(Path(directory) / "reply.wav")
            request_id = await synthesize(client, text, language, voice, reply)
            process = await asyncio.create_subprocess_exec(
                "afplay", reply, stdin=asyncio.subprocess.DEVNULL
            )
            try:
                if code := await asyncio.wait_for(process.wait(), 65):
                    raise RuntimeError(f"Audio playback failed (exit {code})")
            finally:
                await close_process(process)
    else:
        request_id = await synthesize(client, text, language, voice, output_path)
    print(f"TTS request: {request_id}")


async def turn(client, options, lines=None):
    source = capture(options.input, lines)
    transcript = None
    try:
        async with client.stt.stream(
            source,
            language=options.language,
            mode=TranscriptionMode(options.mode),
            context_terms=options.term,
            timeout=120,
        ) as stream:
            async for update in stream:
                print(f"{update.kind}: {update.text}", flush=True)
                if update.kind == "final":
                    transcript = update.text
        print(f"STT request: {stream.request_id}")
    finally:
        await source.aclose()
    if transcript is None:
        raise RuntimeError("Transcription ended without a final result")
    if not transcript.strip():
        print("No speech recognized.")
        return
    # The response step echoes the final transcript so recognition stays visible.
    await asyncio.wait_for(
        speak(client, transcript, options.language, options.voice, options.output), 125
    )


async def main(options):
    if not shutil.which("sox"):
        raise RuntimeError("Install SoX first: brew install sox (macOS)")
    transport = None
    lines = None
    if not options.input:
        lines = asyncio.StreamReader()
        transport, _ = await asyncio.get_running_loop().connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(lines), sys.stdin
        )
    try:
        async with Rime() as client:
            if options.input:
                await turn(client, options)
                return
            print("Voice echo: microphone → STT → TTS. Ctrl+C stops everything.")
            while True:
                print("\nPress Enter to talk, or type q then Enter to quit.", flush=True)
                line = await lines.readline()
                if not line or line.strip().lower() == b"q":
                    return
                await turn(client, options, lines)
    finally:
        if transport:
            transport.close()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--language", required=True, help="Spoken language, e.g. en or es")
    parser.add_argument("--mode", choices=["written", "verbatim"], default="written")
    parser.add_argument("--term", action="append", default=[], help="Recognition hint; repeatable")
    parser.add_argument("--voice", help="TTS voice; defaults to the SDK's Coda voice")
    parser.add_argument("--input", help="Transcribe one audio file instead of the microphone")
    parser.add_argument("--output", help="Save the spoken reply to a WAV file instead of playing")
    options = parser.parse_args()
    if options.output and not options.input:
        parser.error("--output requires --input")
    return options


if __name__ == "__main__":
    try:
        asyncio.run(main(arguments()))
    except KeyboardInterrupt:
        print("\nStopped.")
    except (RimeError, OSError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        if error.__cause__:
            print(f"Cause: {error.__cause__}", file=sys.stderr)
        sys.exit(1)
