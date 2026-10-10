"""Terminal turns use native SDK RPCs and simulated audio device processes."""

import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_stt

from rimelabs_sdk import RimeInputError, RimeUnavailableError
from rimelabs_sdk.stt import _transport

stt_setup = test_stt.stt_setup


@pytest.fixture
def voice(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "examples"))
    return importlib.import_module("stt.voice")


@pytest.fixture
def devices(voice, monkeypatch, tmp_path):
    subprocess = asyncio.create_subprocess_exec
    state = SimpleNamespace(
        processes=[],
        commands=[],
        fail=False,
        played=tmp_path / "played.pcm",
        playback_started=asyncio.Event(),
        hold=False,
        playback_fail=False,
        release=tmp_path / "release",
    )
    script = """
import signal, sys, time, wave
from pathlib import Path
mode, live, fail, output, destination, hold, release = sys.argv[1:]
if mode == 'capture':
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    sys.stdout.buffer.write(b'\\x01\\x00' * 640)
    sys.stdout.buffer.flush()
    if fail == 'True': sys.exit(7)
    if live == 'True':
        while True: time.sleep(1)
elif mode == 'afplay':
    with wave.open(destination, 'rb') as audio:
        assert (audio.getframerate(), audio.getnchannels(), audio.getsampwidth()) == (24000, 1, 2)
        Path(output).write_bytes(audio.readframes(audio.getnframes()))
    if fail == 'True': sys.exit(7)
    while hold == 'True' and not Path(release).exists(): time.sleep(0.01)
else:
    data = sys.stdin.buffer.read()
    if destination == '-d': Path(output).write_bytes(data)
    else:
        with wave.open(destination, 'wb') as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(24000)
            audio.writeframes(data)
"""

    async def spawn(*args, **kwargs):
        capture = kwargs.get("stdout") == asyncio.subprocess.PIPE
        live = capture and args[4] == "-d"
        child = await subprocess(
            sys.executable,
            "-u",
            "-c",
            script,
            "capture" if capture else args[0],
            str(live),
            str(state.playback_fail if args[0] == "afplay" else state.fail),
            str(state.played),
            args[-1],
            str(state.hold),
            str(state.release),
            **kwargs,
        )
        state.processes.append(child)
        state.commands.append(args)
        if args[0] == "afplay":
            state.playback_started.set()
        return child

    monkeypatch.setattr(voice.asyncio, "create_subprocess_exec", spawn)
    return state


@pytest.fixture
def native_playback(voice, monkeypatch):
    monkeypatch.setattr(voice, "sys", SimpleNamespace(platform="darwin"))


@pytest.mark.parametrize("cancel", [False, True])
async def test_native_playback_waits_for_device_and_cleans_up(
    voice, devices, setup, native_playback, cancel
):
    _, client = setup
    devices.hold = True
    operation = asyncio.create_task(voice.speak(client, "Hello.", "en", None))
    try:
        await asyncio.wait_for(devices.playback_started.wait(), 3)
        reply = Path(devices.commands[-1][-1])
        assert reply.exists()
        assert not operation.done(), "generation completion is not playback completion"
        if cancel:
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
        else:
            devices.release.touch()
            await asyncio.wait_for(operation, 3)
            assert devices.played.read_bytes() == b"\x01\x00" * 2400
        assert not reply.parent.exists()
        assert all(process.returncode is not None for process in devices.processes)
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_native_playback_failure_is_reported(voice, devices, setup, native_playback):
    _, client = setup
    devices.playback_fail = True
    with pytest.raises(RuntimeError, match="Audio playback failed"):
        await voice.speak(client, "Hello.", "en", None)
    assert not Path(devices.commands[-1][-1]).parent.exists()


async def test_rejected_tts_never_starts_playback(voice, devices, setup, native_playback):
    synthesis, client = setup
    synthesis.mode = "error_before_audio"
    with pytest.raises(RimeUnavailableError):
        await voice.speak(client, "Hello.", "en", None)
    assert not devices.playback_started.is_set()
    assert not Path(devices.commands[0][-1]).parent.exists()


async def test_explicit_output_does_not_open_speaker(
    voice, devices, setup, native_playback, tmp_path
):
    _, client = setup
    reply = tmp_path / "reply.wav"
    await voice.speak(client, "Hello.", "en", None, reply)
    assert reply.exists()
    assert not devices.playback_started.is_set()


def settings(**overrides):
    values = {
        "language": "en",
        "mode": "written",
        "term": ["Rime"],
        "voice": None,
        "input": "fixture.wav",
        "output": None,
    }
    return SimpleNamespace(**(values | overrides))


async def test_two_turns_speak_only_final_snapshot(voice, devices, setup, stt_setup, capsys):
    synthesis, _ = setup
    recognition, client = stt_setup
    for _ in range(2):
        await voice.turn(client, settings())
    assert recognition.input_finished.is_set()
    assert len(synthesis.calls) == 2
    assert all(
        "".join(message.text_chunk for message in call[1:]) == "Ice cream"
        for call in synthesis.calls
    )
    assert devices.played.read_bytes()
    assert all(process.returncode == 0 for process in devices.processes)
    assert "final: Ice cream" in capsys.readouterr().out


async def test_silence_does_not_start_tts(voice, devices, setup, stt_setup):
    synthesis, _ = setup
    recognition, client = stt_setup
    recognition.mode = "silence"
    await voice.turn(client, settings())
    assert not synthesis.calls
    assert len(devices.processes) == 1


async def test_recorder_failure_does_not_commit_or_speak(
    voice, devices, setup, stt_setup, monkeypatch
):
    synthesis, _ = setup
    _, client = stt_setup
    devices.fail = True
    committed = []

    async def finish_input(call):
        committed.append(call)

    monkeypatch.setattr(_transport.TranscriptionCall, "finish_input", finish_input)
    with pytest.raises(RimeInputError):
        await voice.turn(client, settings())
    assert committed == []
    assert not synthesis.calls
    assert all(process.returncode is not None for process in devices.processes)


@pytest.mark.parametrize("cancel", [False, True])
async def test_microphone_stops_on_enter_or_cancellation(voice, devices, cancel):
    lines = asyncio.StreamReader()
    source = voice.capture(lines=lines)
    assert await asyncio.wait_for(anext(source), 2)
    if cancel:
        pending = asyncio.create_task(anext(source))
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    else:
        lines.feed_data(b"\n")
        assert [chunk async for chunk in source] == []
    await source.aclose()
    assert all(process.returncode is not None for process in devices.processes)


async def test_lazy_rejection_never_opens_microphone(voice, devices, stt_setup):
    recognition, client = stt_setup
    recognition.mode = "reject"
    with pytest.raises(RimeUnavailableError):
        await voice.turn(client, settings(input=None), asyncio.StreamReader())
    assert not devices.processes


async def test_stt_failure_stops_pending_microphone(voice, devices, stt_setup, monkeypatch):
    recognition, client = stt_setup
    recognition.mode = "partial_error"
    partial = asyncio.Event()

    def output(message, **kwargs):
        if message.startswith("partial:"):
            partial.set()

    monkeypatch.setattr(voice, "print", output, raising=False)
    operation = asyncio.create_task(
        voice.turn(client, settings(input=None), asyncio.StreamReader())
    )
    try:
        await asyncio.wait_for(partial.wait(), 2)
        recognition.release.set()
        with pytest.raises(RimeUnavailableError):
            await asyncio.wait_for(operation, 3)
        assert all(process.returncode is not None for process in devices.processes)
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
