"""Exercise duplex streaming and turn reuse against a real local WebSocket peer."""

import asyncio
import base64
import importlib
import json
import os
import sys
import wave
from pathlib import Path
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


@pytest.fixture
def example(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[2]))
    return importlib.import_module("examples.python.tts.coda_ws")


async def test_audio_plays_before_next_text_and_connection_survives_turns(
    example, monkeypatch, tmp_path
):
    played = asyncio.Event()
    samples = []
    contexts = []
    peers = []

    class Player:
        def __init__(self):
            self.stdin = self
            self.returncode = None

        def write(self, data):
            samples.append(data)
            played.set()

        async def drain(self):
            pass

        def close(self):
            pass

        async def wait_closed(self):
            pass

        async def wait(self):
            self.returncode = 0
            return 0

    async def spawn(*args, **kwargs):
        return Player()

    monkeypatch.setattr(example.asyncio, "create_subprocess_exec", spawn)

    async def peer(socket):
        peers.append(socket)
        for _ in range(2):
            start = json.loads(await socket.recv())
            assert start["start"]["text"] == ""
            context_id = start["contextId"]
            contexts.append(context_id)

            async def send(context_id=context_id, **event):
                await socket.send(json.dumps({"contextId": context_id, **event}))

            first = json.loads(await socket.recv())
            assert first == {"contextId": context_id, "text": "First sentence. "}
            await send(started={"requestId": "test-request"})
            # A PCM sample may cross transport chunk boundaries.
            for chunk in (b"\x01", b"\x00\x02\x00"):
                await send(audio=base64.b64encode(chunk).decode())
            second = json.loads(await socket.recv())
            assert second == {"contextId": context_id, "text": "Second sentence. "}
            assert json.loads(await socket.recv()) == {"contextId": context_id, "end": {}}
            await send(audio=base64.b64encode(b"\x03\x00").decode())
            await send(done={})

    async def chunks():
        yield "First sentence."
        # Waiting here deadlocks any implementation that buffers the full input.
        await asyncio.wait_for(played.wait(), 2)
        yield "Second sentence."

    options = SimpleNamespace(
        voice="lyra", language="en", no_playback=False, complete_text=False, lookahead_tokens=0
    )
    async with serve(peer, "127.0.0.1", 0, subprotocols=["rime.v1.json"]) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        async with connect(url, subprotocols=["rime.v1.json"]) as connection:
            for index in range(2):
                played.clear()
                path = tmp_path / f"turn-{index}.wav"
                await asyncio.wait_for(example.synthesize(connection, chunks(), options, path), 5)
                with wave.open(str(path)) as audio:
                    assert audio.getparams()[:3] == (1, 2, 24000)
                    assert audio.readframes(3) == b"\x01\x00\x02\x00\x03\x00"
    assert len(peers) == 1
    assert len(set(contexts)) == 2
    assert b"".join(samples) == b"\x01\x00\x02\x00\x03\x00" * 2


async def test_incomplete_audio_never_saved_as_success(example, monkeypatch, tmp_path):
    monkeypatch.setenv("RIME_API_KEY", "test-key")
    headers = []

    async def peer(socket):
        headers.append(socket.request.headers)
        await socket.send(json.dumps({"ready": {"protocol": 1, "languages": ["en"]}}))
        start = json.loads(await socket.recv())
        await socket.recv()  # text
        await socket.recv()  # end
        await socket.send(json.dumps({"contextId": start["contextId"], "audio": "AQA="}))
        # Disconnect after partial audio, without done.
        await socket.close()

    async with serve(peer, "127.0.0.1", 0, subprotocols=["rime.v1.json"]) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        args = ["--url", url, "--text", "Hello.", "--no-playback", "--output-dir", str(tmp_path)]
        with pytest.raises(ExceptionGroup):
            await asyncio.wait_for(example.main(example.arguments(args)), 5)
    assert headers[0]["Authorization"] == "Bearer test-key"
    assert not list(tmp_path.iterdir())


async def test_auto_end_finishes_before_next_line_and_reuses_connection(
    example, monkeypatch, tmp_path
):
    monkeypatch.setenv("RIME_API_KEY", "test-key")
    first_finished = asyncio.Event()
    peers = []
    contexts = []

    async def peer(socket):
        peers.append(socket)
        await socket.send(json.dumps({"ready": {"protocol": 1, "languages": ["en"]}}))
        for text in ("First complete turn. ", "Second complete turn. "):
            start = json.loads(await socket.recv())
            context_id = start["contextId"]
            contexts.append(context_id)
            assert start["start"]["text"] == ""
            assert json.loads(await socket.recv()) == {"contextId": context_id, "text": text}
            # The client must send end without waiting for another line or audio.
            assert json.loads(await socket.recv()) == {"contextId": context_id, "end": {}}
            await socket.send(json.dumps({"contextId": context_id, "audio": "AQA="}))
            await socket.send(json.dumps({"contextId": context_id, "done": {}}))
            first_finished.set()

    async with serve(peer, "127.0.0.1", 0, subprotocols=["rime.v1.json"]) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            example.__file__,
            "--url",
            url,
            "--auto-end",
            "--no-playback",
            "--output-dir",
            str(tmp_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )
        try:
            process.stdin.write(b"First complete turn.\n")
            await process.stdin.drain()
            await asyncio.wait_for(first_finished.wait(), 5)
            stdout, stderr = await asyncio.wait_for(
                process.communicate(b"Second complete turn.\n/quit\n"), 5
            )
            assert process.returncode == 0, stderr.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
    assert len(peers) == 1
    assert len(set(contexts)) == 2
    assert stdout.count(b"Done:") == 2
    assert sorted(path.name for path in tmp_path.iterdir()) == ["turn-001.wav", "turn-002.wav"]


@pytest.mark.parametrize("complete_text", [True, False])
async def test_complete_text_and_streaming_lookahead_use_distinct_protocol_modes(
    example, monkeypatch, tmp_path, complete_text
):
    monkeypatch.setenv("RIME_API_KEY", "test-key")
    requests = []

    async def peer(socket):
        await socket.send(json.dumps({"ready": {"protocol": 1, "languages": ["en"]}}))
        start = json.loads(await socket.recv())
        requests.append(start)
        context_id = start["contextId"]
        if complete_text:
            # Full input is in start; no text or end frames should follow.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(socket.recv(), 0.05)
        else:
            requests.append(json.loads(await socket.recv()))
            requests.append(json.loads(await socket.recv()))
        await socket.send(json.dumps({"contextId": context_id, "audio": "AQA="}))
        await socket.send(json.dumps({"contextId": context_id, "done": {}}))

    async with serve(peer, "127.0.0.1", 0, subprotocols=["rime.v1.json"]) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        args = ["--url", url, "--text", "Hello.", "--no-playback", "--output-dir", str(tmp_path)]
        args.extend(["--complete-text"] if complete_text else ["--lookahead-tokens", "8"])
        await asyncio.wait_for(example.main(example.arguments(args)), 5)
    start = requests[0]["start"]
    if complete_text:
        assert start["text"] == "Hello."
        assert "codaParameters" not in start
        assert len(requests) == 1
    else:
        assert start["text"] == ""
        assert start["codaParameters"] == {"textLookaheadTokens": 8}
        assert requests[1]["text"] == "Hello. "
        assert "end" in requests[2]
    assert (tmp_path / "turn-001.wav").exists()
