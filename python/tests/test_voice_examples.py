"""Exercise the example audio boundary without microphones or a model server."""

import asyncio
import base64
import importlib
import wave
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_realtime import Peer

from rimelabs_sdk import realtime as r
from rimelabs_sdk.realtime import _client


@pytest.fixture
def examples(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "examples"))
    return SimpleNamespace(
        devices=importlib.import_module("audio_devices"),
        voice=importlib.import_module("realtime.voice"),
        recorded=importlib.import_module("realtime.recorded"),
        save=importlib.import_module("tts.save"),
    )


class Stream:
    def __init__(self, **kwargs):
        self.aborted = False
        self.closed = False

    def start(self):
        pass

    def abort(self):
        self.aborted = True

    def close(self):
        self.closed = True


@pytest.fixture
def devices(examples, monkeypatch):
    monkeypatch.setattr(
        examples.devices,
        "sounddevice",
        lambda: SimpleNamespace(RawInputStream=Stream, RawOutputStream=Stream),
    )
    return examples.devices


class DeviceClock:
    """Control device time and completion callbacks without changing asyncio's clock."""

    def __init__(self):
        self.now = 100.0
        self.callbacks = []
        self.create_future = asyncio.get_running_loop().create_future

    def monotonic(self):
        return self.now

    def call_soon_threadsafe(self, callback, *args):
        self.call_later(0, callback, *args)

    def call_later(self, delay, callback, *args):
        self.callbacks.append((self.now + delay, callback, args))

    def advance(self, seconds):
        end = self.now + seconds
        while self.callbacks:
            self.callbacks.sort(key=lambda entry: entry[0])
            if self.callbacks[0][0] > end:
                break
            self.now, callback, args = self.callbacks.pop(0)
            callback(*args)
        self.now = end


async def wait_for_sealed_playback(speaker, previous=None):
    async with asyncio.timeout(1):
        while speaker.current is previous or not speaker.current.sealed:
            await asyncio.sleep(0)


async def test_output_waits_for_device_tail_and_excludes_unplayed_audio(devices, monkeypatch):
    clock = DeviceClock()
    monkeypatch.setattr(devices, "time", clock)
    with devices.Speaker() as speaker:
        speaker.loop = clock
        playback = speaker.begin()
        speaker.write(playback, b"\x01\0" * 480)
        speaker.end(playback)
        speaker._render(
            bytearray(960), 480, SimpleNamespace(outputBufferDacTime=1.05, currentTime=1.0), None
        )
        clock.advance(0.01)
        assert not playback.done.done()
        clock.advance(0.061)
        assert playback.done.result() == pytest.approx(0.02)
        second = speaker.begin()
        second.runs.extend([(clock.now - 0.1, 0.05), (clock.now + 1, 0.02)])
        speaker.write(second, bytes(48000))
        speaker.interrupt()
        assert await second.done == pytest.approx(0.05)
        assert speaker.stream.aborted and not second.buffer
        speaker.write(second, b"\0\0")
        assert not second.buffer
    assert speaker.stream.closed


@pytest.mark.parametrize("status", ["output underflow", "priming output"])
async def test_output_callback_flags_do_not_stop_playback(devices, monkeypatch, status):
    clock = DeviceClock()
    monkeypatch.setattr(devices, "time", clock)
    with devices.Speaker() as speaker:
        speaker.loop = clock
        playback = speaker.begin()
        frame = b"\x01\0" * 480
        speaker.write(playback, frame)
        speaker.end(playback)
        output = bytearray(960)
        speaker._render(output, 480, SimpleNamespace(outputBufferDacTime=0, currentTime=0), status)
        assert output == frame and not playback.buffer
        clock.advance(0.021)
        assert playback.done.result() == pytest.approx(0.02)
        assert not speaker.stream.aborted


async def test_input_overflow_does_not_stop_capture(devices):
    with devices.Microphone() as microphone:
        frame = b"\x01\0" * 640
        microphone._capture(frame, 640, None, "input overflow")
        assert await asyncio.wait_for(microphone.read(), 1) == frame
        microphone._capture(frame, 640, None, None)
        assert await asyncio.wait_for(microphone.read(), 1) == frame


async def test_microphone_continues_during_playback_and_has_a_bound(devices):
    with devices.Speaker() as speaker, devices.Microphone() as microphone:
        speaker.begin()
        frame = b"\1\0" * 640
        microphone._capture(frame, 640, None, None)
        assert await microphone.read() == frame
        for _ in range(101):
            microphone._offer(frame)
        assert microphone.queue.qsize() == 100
        for _ in range(99):
            await microphone.read()
        with pytest.raises(RuntimeError, match="queue filled"):
            await microphone.read()
    assert microphone.stream.closed


async def test_tts_wav_is_playable(examples, setup, monkeypatch, tmp_path):
    service, _ = setup
    monkeypatch.setenv("RIME_API_KEY", "example-key")
    path = tmp_path / "speech.wav"
    await examples.save.main(path)
    with wave.open(str(path), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 24000)
        assert wav.readframes(wav.getnframes()) == service.payload


async def test_recorded_speech_sends_audio_not_text_and_saves_wav(examples, monkeypatch, tmp_path):
    peer = Peer()

    async def connect(*args, **kwargs):
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    monkeypatch.setenv("RIME_API_KEY", "test")
    monkeypatch.setenv("PRISM_URL", "ws://localhost/v1/realtime")
    path = tmp_path / "reply.wav"
    async with asyncio.timeout(3):
        task = asyncio.create_task(examples.recorded.main(output=path))
        request = await peer.next("input_audio_buffer.append")
        assert len(base64.b64decode(request["audio"])) == 1280
        peer.emit("response.created", response={"id": "speech-reply"})
        peer.emit(
            "response.audio.delta",
            response_id="speech-reply",
            item_id="message",
            output_index=0,
            content_index=0,
            delta=base64.b64encode(b"\1\0" * 40).decode(),
        )
        peer.ended("speech-reply")
        await task
    assert peer.closed
    with wave.open(str(path), "rb") as wav:
        assert wav.getframerate() == 24000 and wav.getnframes() == 40


class Session:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.reports = asyncio.Queue()
        self.info = SimpleNamespace(interrupt_on_speech=True)
        self.results = []
        self.result_recorded = asyncio.Event()
        self.continued = []

    @property
    def events(self):
        async def read():
            while True:
                yield SimpleNamespace(payload=await self.queue.get())

        return read()

    async def send_audio(self, chunk):
        pass

    async def report_playback(self, report):
        await self.reports.put(report)

    async def submit_tool_result(self, call, result):
        self.results.append((call, result))
        self.result_recorded.set()

    async def continue_reply(self, response):
        self.continued.append(response)


def turn(session, id):
    ref = r.ResponseRef(session_id="s", response_id=id)
    output = r.OutputRef(response=ref, item_id=id, output_index=0, content_index=0)
    session.queue.put_nowait(r.ResponseStarted(response=ref, cause="speech"))
    session.queue.put_nowait(r.MessageStarted(output=output))
    session.queue.put_nowait(
        r.AudioDelta(
            output=output,
            audio=r.AudioChunk(data=b"\1\0" * 480, format=r.PCMFormat(sample_rate=24000)),
        )
    )
    return ref


async def test_voice_interruption_clears_audio_and_next_turn_finishes(examples, devices):
    session = Session()
    with devices.Microphone() as microphone, devices.Speaker() as speaker:
        task = asyncio.create_task(examples.voice.converse(session, microphone, speaker))
        try:
            first = turn(session, "first")
            await asyncio.sleep(0)
            session.queue.put_nowait(r.SpeechStarted(item_id="user", audio_start_ms=0))
            report = await asyncio.wait_for(session.reports.get(), 1)
            assert isinstance(report, r.PlaybackInterrupted) and report.audio_end_ms == 0
            assert not speaker.current.buffer
            first_playback = speaker.current
            session.queue.put_nowait(
                r.ResponseEnded(response=first, status="cancelled", reason="turn_detected")
            )
            await asyncio.sleep(0)
            second = turn(session, "second")
            session.queue.put_nowait(
                r.ResponseEnded(response=second, status="completed", reason="stop")
            )
            await wait_for_sealed_playback(speaker, first_playback)
            speaker._render(
                bytearray(960), 480, SimpleNamespace(outputBufferDacTime=0, currentTime=0), None
            )
            report = await asyncio.wait_for(session.reports.get(), 1)
            assert isinstance(report, r.PlaybackFinished)
            assert report.response == second and report.played_ms == pytest.approx(20)
            assert not task.done()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_new_speech_during_tool_work_records_result_without_old_continuation(
    examples, devices
):
    session = Session()
    started, release = asyncio.Event(), asyncio.Event()

    async def tool(call):
        started.set()
        await release.wait()
        return '{"status":"shipped"}'

    with devices.Microphone() as microphone, devices.Speaker() as speaker:
        task = asyncio.create_task(examples.voice.converse(session, microphone, speaker, tool))
        ref = r.ResponseRef(session_id="s", response_id="tool")
        try:
            session.queue.put_nowait(r.ResponseStarted(response=ref, cause="speech"))
            session.queue.put_nowait(
                r.ToolCall(
                    call=r.ToolCallRef(session_id="s", response_id="tool", call_id="c"),
                    item_id="t",
                    name="lookup_order",
                    arguments={"order_id": "demo-123"},
                )
            )
            session.queue.put_nowait(
                r.ResponseEnded(response=ref, status="completed", reason="stop")
            )
            await asyncio.wait_for(started.wait(), 1)
            session.queue.put_nowait(r.SpeechStarted(item_id="user", audio_start_ms=0))
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(session.result_recorded.wait(), 1)
            assert len(session.results) == 1 and not session.continued
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("code", ["tool_continuation_unavailable", "unexpected_error"])
async def test_tool_continuation_refusal_keeps_next_turn_open(examples, devices, code):
    session = Session()
    requested, refuse = asyncio.Event(), asyncio.Event()

    async def continue_reply(response):
        session.continued.append(response)
        requested.set()
        await refuse.wait()
        raise r.RimeRealtimeError(r.RealtimeFault(scope="event", code=code, message="Refused"))

    session.continue_reply = continue_reply

    async def tool(call):
        return '{"status":"shipped"}'

    with devices.Microphone() as microphone, devices.Speaker() as speaker:
        task = asyncio.create_task(examples.voice.converse(session, microphone, speaker, tool))
        ref = r.ResponseRef(session_id="s", response_id="tool")
        try:
            async with asyncio.timeout(2):
                session.queue.put_nowait(r.ResponseStarted(response=ref, cause="speech"))
                session.queue.put_nowait(
                    r.ToolCall(
                        call=r.ToolCallRef(session_id="s", response_id="tool", call_id="c"),
                        item_id="t",
                        name="lookup_order",
                        arguments={"order_id": "demo-123"},
                    )
                )
                session.queue.put_nowait(
                    r.ResponseEnded(response=ref, status="completed", reason="stop")
                )
                await requested.wait()
                await session.reports.get()  # Tool round playback is complete.
                session.queue.put_nowait(r.SpeechStarted(item_id="user", audio_start_ms=0))
                await asyncio.sleep(0)
                refuse.set()
                if code == "unexpected_error":
                    with pytest.raises(ExceptionGroup) as raised:
                        await task
                    assert any(
                        isinstance(error, r.RimeRealtimeError) for error in raised.value.exceptions
                    )
                else:
                    second = turn(session, "second")
                    session.queue.put_nowait(
                        r.ResponseEnded(response=second, status="completed", reason="stop")
                    )
                    await wait_for_sealed_playback(speaker)
                    speaker._render(
                        bytearray(960),
                        480,
                        SimpleNamespace(outputBufferDacTime=0, currentTime=0),
                        None,
                    )
                    report = await session.reports.get()
                    assert isinstance(report, r.PlaybackFinished) and report.response == second
                    assert not task.done()
                assert session.continued == [ref] and len(session.results) == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
