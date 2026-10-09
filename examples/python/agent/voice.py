"""Local Rime STT → OpenAI → Rime TTS agent and SDK feature checks."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import wave
from dataclasses import asdict
from pathlib import Path

import httpx

from rimelabs_sdk import PronunciationEntry, Rime, RimeError, TranscriptionMode

# Reuse the terminal example's lazy, cancellable SoX microphone capture.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stt.voice import capture, close_process

INSTRUCTIONS = (
    "You are a concise voice assistant. Reply in one or two short sentences, without markdown. "
    "When asked to repeat text, repeat it exactly. Reply in the user's language."
)


def redact(message):
    for name in ("RIME_API_KEY", "OPENAI_API_KEY"):
        if key := os.environ.get(name):
            message = message.replace(key, "[redacted]")
    return message


def load_lexicon(path):
    if not path:
        return []
    entries = json.loads(Path(path).read_text())
    if not isinstance(entries, list) or any(
        not isinstance(entry, dict)
        or set(entry) != {"spelling", "pronunciation"}
        or not all(isinstance(value, str) for value in entry.values())
        for entry in entries
    ):
        raise ValueError(
            'Lexicon must be a JSON array of {"spelling": string, "pronunciation": string}'
        )
    return [PronunciationEntry(**entry) for entry in entries]


async def respond(http, model, instructions, history, text):
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError(
            "Set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY"
        )
    response = await http.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "model": model,
            "instructions": instructions,
            "input": [*history[-20:], {"role": "user", "content": text}],
            "max_output_tokens": 512,
            "store": False,
        },
    )
    request_id = response.headers.get("x-request-id")
    if response.is_error:
        # Provider error bodies can echo a rejected API key.
        try:
            message = response.json().get("error", {}).get("message", response.reason_phrase)
        except ValueError:
            message = response.reason_phrase
        raise RuntimeError(
            redact(f"OpenAI HTTP {response.status_code}: {message}; request_id={request_id}")
        )
    body = response.json()
    if body.get("status") != "completed":
        raise RuntimeError(
            f"OpenAI response did not complete: {body.get('status')}; request_id={request_id}"
        )
    reply = "".join(
        part["text"]
        for item in body.get("output", [])
        if item.get("type") == "message"
        for part in item.get("content", [])
        if part.get("type") == "output_text"
    ).strip()
    if not reply:
        raise RuntimeError(f"OpenAI returned no speakable text; request_id={request_id}")
    return reply, request_id


async def playback(path):
    command = ("afplay", str(path)) if sys.platform == "darwin" else ("sox", "-q", str(path), "-d")
    process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.DEVNULL)
    try:
        if code := await asyncio.wait_for(process.wait(), 65):
            raise RuntimeError(f"Playback failed (exit {code})")
    finally:
        await close_process(process)


async def turn(client, http, options, history, number, *, lines=None, say=None, ask=None):
    stem = Path(options.output_dir) / f"turn-{number:03d}"
    record = {
        "model": options.model,
        "language": options.language,
        "voice": options.voice,
        "llm_model": options.llm_model,
        "stage": "configuration",
        "status": "pending",
    }
    partial = stem.with_suffix(".partial.wav")
    try:
        lexicon = load_lexicon(options.lexicon)
        record["custom_lexicon"] = [asdict(entry) for entry in lexicon]
        record["timestamps_requested"] = options.timestamps
        record["complete_text"] = options.complete_text
        transcript = ask
        if say is None:
            if not os.environ.get("OPENAI_API_KEY"):
                raise RuntimeError(
                    "Set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY"
                )
            if transcript is None:
                record["stage"] = "stt"
                source = capture(options.input, lines)
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
                    record["stt_request_id"] = stream.request_id
                    print(f"STT request: {stream.request_id}")
                finally:
                    await source.aclose()
                if transcript is None:
                    raise RuntimeError("Transcription ended without a final result")
            record["transcript"] = transcript
            if not transcript.strip():
                record["status"] = "silence"
                print("No speech recognized.")
                return True
            record["stage"] = "llm"
            reply, record["llm_request_id"] = await respond(
                http, options.llm_model, options.instructions, history, transcript
            )
            print(f"OpenAI request: {record['llm_request_id']}")
        else:
            reply = say
        record["reply"] = reply
        print(f"Assistant: {reply}")
        record["stage"] = "tts"
        with wave.open(str(partial), "wb") as output:
            output.setparams((1, 2, 24000, 0, "NONE", "not compressed"))
            async with client.tts.stream(
                reply,
                language=options.language,
                voice=options.voice,
                timestamps=options.timestamps,
                custom_lexicon=lexicon,
                complete_text=options.complete_text,
                timeout=60,
            ) as audio:
                async for chunk in audio:
                    output.writeframesraw(chunk)
                record["tts_request_id"] = audio.request_id
                print(f"TTS request: {audio.request_id}")
        destination = stem.with_suffix(".wav")
        partial.replace(destination)
        record["audio_file"] = str(destination)
        # Keep completed audio even if the timestamp result itself is malformed.
        if options.timestamps:
            record["stage"] = "timestamps"
            result = await audio.timestamps()
            record["timestamps"] = asdict(result)
            print(f"Timestamp status: {result.status.code} {result.status.message}")
            for word in result.spans:
                print(f"  {word.start:8.3f}–{word.end:8.3f}  {word.text}")
        if not options.no_playback:
            record["stage"] = "playback"
            await playback(destination)
        if say is None:
            history.extend(
                [
                    {"role": "user", "content": transcript},
                    {"role": "assistant", "content": reply},
                ]
            )
            del history[:-20]
        record["stage"], record["status"] = "done", "ok"
        return True
    except (
        RimeError,
        OSError,
        RuntimeError,
        ValueError,
        httpx.HTTPError,
        TimeoutError,
    ) as error:
        record["status"] = "error"
        record["error"] = {
            "type": type(error).__name__,
            "message": redact(str(error)),
            "request_id": getattr(error, "request_id", None),
        }
        print(f"{record['stage']} error: {record['error']}", file=sys.stderr, flush=True)
        return False
    except asyncio.CancelledError:
        record["status"] = "cancelled"
        raise
    finally:
        partial.unlink(missing_ok=True)
        stem.with_suffix(".json").write_text(
            json.dumps(record, indent=2, ensure_ascii=False) + "\n"
        )
        print(f"Report: {stem.with_suffix('.json')}", flush=True)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=["coda", "mistv3"], default="coda")
    parser.add_argument("--language", default="en")
    parser.add_argument("--voice")
    parser.add_argument("--timestamps", action="store_true")
    parser.add_argument(
        "--complete-text",
        action="store_true",
        help="Send complete text with Synthesize; audio still streams",
    )
    parser.add_argument("--lexicon", help="JSON entries; reloaded before every turn")
    parser.add_argument("--llm-model", default=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"))
    parser.add_argument("--instructions", default=INSTRUCTIONS)
    parser.add_argument("--mode", choices=["written", "verbatim"], default="written")
    parser.add_argument("--term", action="append", default=[])
    parser.add_argument("--endpoint", help="Optional Rime TTS host:port")
    parser.add_argument("--stt-endpoint", help="Optional Rime STT host:port")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--input", help="One recorded-audio conversation turn")
    source.add_argument("--say", help="One direct TTS test; bypasses STT and OpenAI")
    parser.add_argument(
        "--no-playback", action="store_true", help="Save audio without opening speakers"
    )
    parser.add_argument(
        "--output-dir",
        help="Parent for a new run directory; default is OS temp directory",
    )
    return parser.parse_args(argv)


async def main(options):
    if options.output_dir:
        Path(options.output_dir).mkdir(parents=True, exist_ok=True)
    options.output_dir = tempfile.mkdtemp(prefix="rime-agent-python-", dir=options.output_dir)
    print(f"Artifacts: {options.output_dir}")
    transport = None
    history = []
    try:
        async with (
            Rime(
                model=options.model,
                endpoint=options.endpoint,
                stt_endpoint=options.stt_endpoint,
            ) as client,
            httpx.AsyncClient(timeout=30) as http,
        ):
            if options.input or options.say is not None:
                return await turn(client, http, options, history, 1, say=options.say)
            lines = asyncio.StreamReader()
            transport, _ = await asyncio.get_running_loop().connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(lines), sys.stdin
            )
            print(
                "Enter to talk; /say TEXT tests TTS; /ask TEXT talks to the LLM; /reset; /quit. Ctrl+C exits."
            )
            number = 0
            while True:
                print("\nReady > ", end="", flush=True)
                raw = await lines.readline()
                if not raw:
                    return True
                command = raw.decode().strip()
                if command in ("q", "/quit"):
                    return True
                if command == "/reset":
                    history.clear()
                    print("Conversation cleared.")
                    continue
                say = command[5:].strip() if command.startswith("/say ") else None
                ask = command[5:].strip() if command.startswith("/ask ") else None
                if command and say is None and ask is None:
                    print("Use Enter, /say TEXT, /ask TEXT, /reset, or /quit.")
                    continue
                number += 1
                await turn(
                    client,
                    http,
                    options,
                    history,
                    number,
                    lines=lines if not command else None,
                    say=say,
                    ask=ask,
                )
    finally:
        if transport:
            transport.close()


if __name__ == "__main__":
    try:
        sys.exit(0 if asyncio.run(main(arguments())) else 1)
    except KeyboardInterrupt:
        print("\nStopped.")
    except (RimeError, OSError, RuntimeError, ValueError) as error:
        print(redact(f"Error: {error}"), file=sys.stderr)
        sys.exit(1)
