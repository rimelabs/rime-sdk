"""Stream text chunks to Coda and play audio on a persistent WebSocket."""

import argparse
import asyncio
import base64
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
import wave
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

URL = "wss://api.rime.ai/coda/ws"
RATE = 24000


def redact(message):
    for name in ("RIME_API_KEY", "OPENAI_API_KEY"):
        if key := os.environ.get(name):
            message = message.replace(key, "[redacted]")
    return message


async def receive(connection):
    raw = await connection.recv()
    if not isinstance(raw, str):
        raise TypeError("Expected a JSON text frame")
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise TypeError("Expected a JSON event object")
    if "error" in event:
        raise RuntimeError(f"Coda error: {redact(json.dumps(event['error']))}")
    return event


async def stop_player(player):
    if player is not None and player.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            player.terminate()
        try:
            await asyncio.wait_for(player.wait(), 2)
        except TimeoutError:
            player.kill()
            await player.wait()


async def synthesize(connection, chunks, options, destination):
    """Send input and consume audio concurrently; end only finishes this context."""
    context_id = uuid.uuid4().hex
    started_at = time.monotonic()
    input_ended = options.complete_text
    complete_text = "".join([chunk async for chunk in chunks]) if options.complete_text else ""
    player = None
    received = 0
    first_audio = None
    partial = destination.with_suffix(".partial.wav")

    async def send_text():
        nonlocal input_ended
        if options.complete_text:
            return
        async for chunk in chunks:
            if not chunk.strip():
                continue
            # Keep word boundaries between independently normalized messages.
            if not chunk[-1].isspace():
                chunk += " "
            message = json.dumps({"contextId": context_id, "text": chunk})
            if len(message.encode()) > 64 * 1024:
                raise ValueError("Text chunk exceeds the 64 KiB message limit; split it up")
            await connection.send(message)
            print(f"\n[text sent] {chunk.rstrip()}", flush=True)
        input_ended = True
        await connection.send(json.dumps({"contextId": context_id, "end": {}}))

    async def receive_audio(output):
        nonlocal player, received, first_audio
        remainder = b""
        while True:
            event = await receive(connection)
            if event.get("contextId") != context_id:
                raise RuntimeError("Received an unexpected context ID")
            if "started" in event:
                print(f"\nRequest: {event['started'].get('requestId')}", flush=True)
            elif "audio" in event:
                audio = base64.b64decode(event["audio"], validate=True)
                if audio and first_audio is None:
                    first_audio = time.monotonic() - started_at
                    print(f"\nFirst audio: {first_audio * 1000:.0f} ms", flush=True)
                received += len(audio)
                audio = remainder + audio
                remainder = audio[len(audio) // 2 * 2 :]
                audio = audio[: len(audio) // 2 * 2]
                if not audio:
                    continue
                output.writeframesraw(audio)
                if not options.no_playback:
                    if player is None:
                        player = await asyncio.create_subprocess_exec(
                            "sox",
                            "-q",
                            "--buffer",
                            "2048",
                            "-t",
                            "raw",
                            "-e",
                            "signed-integer",
                            "-b",
                            "16",
                            "-L",
                            "-r",
                            str(RATE),
                            "-c",
                            "1",
                            "-",
                            "-d",
                            stdin=asyncio.subprocess.PIPE,
                        )
                    try:
                        player.stdin.write(audio)
                        await player.stdin.drain()
                    except (BrokenPipeError, ConnectionResetError) as exc:
                        raise RuntimeError(
                            "SoX playback stopped; check the audio device and its error above"
                        ) from exc
            elif "done" in event:
                if not input_ended:
                    raise RuntimeError("Coda ended the context before text input finished")
                if remainder or not received:
                    raise RuntimeError("Coda returned incomplete or empty PCM audio")
                return
            elif "cancelled" in event:
                raise RuntimeError("Coda cancelled synthesis")

    start = {
        "speaker": options.voice,
        "language": options.language,
        "text": complete_text,
        "audioParameters": {"audioFormat": "audio/pcm", "samplingRate": RATE},
    }
    if not options.complete_text:
        start["codaParameters"] = {"textLookaheadTokens": options.lookahead_tokens}
    message = json.dumps({"contextId": context_id, "start": start})
    if len(message.encode()) > 64 * 1024:
        raise ValueError("Start request exceeds the 64 KiB message limit")
    await connection.send(message)
    try:
        with wave.open(str(partial), "wb") as output:
            output.setparams((1, 2, RATE, 0, "NONE", "not compressed"))
            async with asyncio.TaskGroup() as tasks:
                receiver = tasks.create_task(receive_audio(output))
                await send_text()
                # Human input may pause indefinitely; bound completion after /end.
                await asyncio.wait_for(receiver, 60)
        partial.replace(destination)
        if player is not None:
            player.stdin.close()
            await player.stdin.wait_closed()
            code = await asyncio.wait_for(player.wait(), received / (RATE * 2) + 10)
            if code:
                raise RuntimeError(f"SoX playback failed (exit {code})")
        print(
            f"\nDone: {received / (RATE * 2):.2f}s audio; saved {destination}",
            flush=True,
        )
    finally:
        await stop_player(player)
        partial.unlink(missing_ok=True)


async def supplied_chunks(texts, delay):
    for index, text in enumerate(texts):
        if index:
            await asyncio.sleep(delay)
        yield text


async def read_line(reader, prompt):
    print(prompt, end="", flush=True)
    line = await reader.readline()
    return line.decode().rstrip("\r\n") if line else None


async def main(options):
    key = os.environ.get("RIME_API_KEY")
    if not key:
        raise RuntimeError("Set RIME_API_KEY in your environment")
    if not options.no_playback and not shutil.which("sox"):
        raise RuntimeError("Install SoX (brew install sox), or pass --no-playback")
    track = "canary" if options.canary else "stable"
    directory = options.output_dir or Path(tempfile.mkdtemp(prefix=f"rime-coda-{track}-"))
    directory.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {key}", "x-rime-track": track}
    transport = None
    try:
        async with connect(
            options.url,
            additional_headers=headers,
            subprotocols=["rime.v1.json"],
            open_timeout=15,
            close_timeout=3,
        ) as connection:
            if connection.subprotocol != "rime.v1.json":
                raise RuntimeError("Server did not negotiate rime.v1.json")
            ready = (await asyncio.wait_for(receive(connection), 15)).get("ready", {})
            if ready.get("protocol") != 1:
                raise RuntimeError("Expected Coda WebSocket protocol v1 readiness")
            if options.language not in ready.get("languages", []):
                raise ValueError(f"Unsupported language. Available: {ready.get('languages')}")
            print(f"Connected: {options.url} | track={track} | voice={options.voice}")
            print(
                "Input: complete text in start"
                if options.complete_text
                else f"Input: streaming text | lookahead={options.lookahead_tokens} tokens"
            )
            if options.text:
                await synthesize(
                    connection,
                    supplied_chunks(options.text, options.chunk_delay),
                    options,
                    directory / "turn-001.wav",
                )
                return
            reader = asyncio.StreamReader()
            transport, _ = await asyncio.get_running_loop().connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(reader),
                sys.stdin,
            )
            if options.auto_end:
                print("Enter a complete turn per line. Enter finishes its input; /quit exits.")
            else:
                print("Enter a sentence/clause per line. /end finishes a turn; /quit exits.")
                print("Coda can pause mid-sentence while awaiting more text or /end.")
                print("Use --auto-end to finish input automatically after each entered line.")
            print("The WebSocket stays open between turns. Ctrl+C stops everything.")
            quitting = False

            async def manual_chunks(first):
                nonlocal quitting
                yield first
                if options.auto_end:
                    return
                while True:
                    line = await read_line(reader, "chunk> ")
                    if line is None or line.strip() == "/quit":
                        quitting = True
                        return
                    if line.strip() == "/end":
                        return
                    yield line

            number = 0
            while not quitting:
                first = await read_line(reader, "text> ")
                if first is None or first.strip() == "/quit":
                    break
                if not first.strip() or first.strip() == "/end":
                    continue
                number += 1
                await synthesize(
                    connection,
                    manual_chunks(first),
                    options,
                    directory / f"turn-{number:03d}.wav",
                )
    finally:
        if transport is not None:
            transport.close()


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--canary",
        action="store_true",
        help="Select x-rime-track: canary (default: stable)",
    )
    parser.add_argument("--voice", default="lyra")
    parser.add_argument("--language", default="en")
    parser.add_argument("--url", default=URL)
    parser.add_argument(
        "--auto-end",
        action="store_true",
        help="Finish each interactive turn after one line, keeping the WebSocket open",
    )
    parser.add_argument(
        "--text",
        action="append",
        help="Send a text chunk; repeat for multiple chunks, then exit",
    )
    parser.add_argument(
        "--complete-text",
        action="store_true",
        help="Send one --text value in start.text to compare full-text synthesis",
    )
    parser.add_argument(
        "--lookahead-tokens",
        type=int,
        default=0,
        help="Leading text tokens to prefill for streaming input (default: 0)",
    )
    parser.add_argument(
        "--chunk-delay",
        type=float,
        default=0.5,
        help="Seconds between --text chunks (default: 0.5)",
    )
    parser.add_argument(
        "--no-playback", action="store_true", help="Save audio without opening speakers"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Save WAV files here (default: a new temporary directory)",
    )
    result = parser.parse_args(argv)
    if result.chunk_delay < 0:
        parser.error("--chunk-delay must be non-negative")
    if result.text and not any(text.strip() for text in result.text):
        parser.error("--text must include non-whitespace text")
    if result.complete_text and (not result.text or len(result.text) != 1):
        parser.error("--complete-text requires exactly one --text value")
    if result.lookahead_tokens < 0:
        parser.error("--lookahead-tokens must be non-negative")
    if result.complete_text and result.lookahead_tokens:
        parser.error("--lookahead-tokens only applies to streaming input")
    return result


def error_messages(error):
    if isinstance(error, BaseExceptionGroup):
        return "; ".join(error_messages(child) for child in error.exceptions)
    return redact(str(error) or type(error).__name__)


if __name__ == "__main__":
    try:
        asyncio.run(main(arguments()))
    except KeyboardInterrupt:
        print("\nStopped speech and playback.")
    except (
        OSError,
        TypeError,
        ValueError,
        RuntimeError,
        TimeoutError,
        WebSocketException,
        ExceptionGroup,
    ) as exc:
        print(f"Error: {error_messages(exc)}", file=sys.stderr)
        sys.exit(1)
