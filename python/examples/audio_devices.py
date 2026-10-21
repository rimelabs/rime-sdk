"""Local PCM device adapters for the SDK examples.

Playback receipts use PortAudio's DAC clock, including the last device buffer.
Use headphones for simultaneous speaking and listening. Capture stays active
during playback; this helper does not implement acoustic echo cancellation.
"""

import asyncio
import math
import threading
import time
from dataclasses import dataclass, field

from rimelabs_sdk.realtime import AudioChunk, PCMFormat


def sounddevice():
    try:
        import sounddevice as sd
    except (ImportError, OSError) as error:
        raise RuntimeError(
            "Install the examples audio extra and PortAudio; see docs/examples.md"
        ) from error
    return sd


class Microphone:
    """16 kHz mono PCM16, in 40 ms chunks, with a bounded input queue."""

    def __init__(self, device=None):
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue(maxsize=100)
        self.closed = False
        self.stream = sounddevice().RawInputStream(
            device=device,
            samplerate=16000,
            channels=1,
            dtype="int16",
            blocksize=640,
            callback=self._capture,
        )

    def _capture(self, data, frames, clock, status):
        if status:
            self.loop.call_soon_threadsafe(print, f"Microphone: {status}")
        self.loop.call_soon_threadsafe(self._offer, bytes(data))

    def _offer(self, value):
        if self.closed:
            return
        if self.queue.full():
            self.queue.get_nowait()
            value = RuntimeError("Microphone queue filled; the sender could not keep up")
        self.queue.put_nowait(value)

    async def read(self):
        value = await self.queue.get()
        if isinstance(value, Exception):
            raise value
        return AudioChunk(data=value, format=PCMFormat(sample_rate=16000))

    def __enter__(self):
        self.stream.start()
        return self

    def __exit__(self, *exc):
        self.closed = True
        self.stream.close()


@dataclass(eq=False)
class Playback:
    done: asyncio.Future
    buffer: bytearray = field(default_factory=bytearray)
    runs: list[tuple[float, float]] = field(default_factory=list)
    sealed: bool = False
    interrupted: bool = False
    completion_scheduled: bool = False
    timer: asyncio.TimerHandle | None = None
    elapsed: float = 0.0

    def played(self, now: float) -> float:
        return self.elapsed + sum(
            min(duration, max(0, now - start)) for start, duration in self.runs
        )


class Speaker:
    """One active 24 kHz mono stream. All public methods run on the event loop."""

    def __init__(self, device=None):
        self.loop = asyncio.get_running_loop()
        self.lock = threading.Lock()
        self.current = None
        self.pending = []
        self.stopping = False
        self.paused = False
        self.failure = None
        self.stream = sounddevice().RawOutputStream(
            device=device,
            samplerate=24000,
            channels=1,
            dtype="int16",
            blocksize=480,
            callback=self._render,
            finished_callback=self._finished,
        )

    def _finished(self):
        if not self.stopping:
            self.loop.call_soon_threadsafe(
                self._fail, RuntimeError("Speaker stopped before playback could be measured")
            )

    def _fail(self, error):
        self.failure = error
        with self.lock:
            for playback in self.pending:
                playback.buffer.clear()
                if playback.timer:
                    playback.timer.cancel()
                if not playback.done.done():
                    playback.done.set_exception(error)

    def open(self, output):
        return DeviceOutput(self, self.begin())

    async def stop(self):
        # PortAudio abort returns after the device stops; no remote ACK is needed.
        self._stop()

    def _stop(self):
        try:
            for playback in list(self.pending):
                self.interrupt(playback)
        finally:
            # Also stop the device if a failed receipt no longer has a pending wait.
            self.stopping = True
            try:
                self.stream.abort()
            finally:
                self.stopping = False

    def begin(self):
        with self.lock:
            if self.failure:
                raise self.failure
            self.pending = [item for item in self.pending if not item.done.done()]
            if len(self.pending) >= 128:
                raise RuntimeError("Speaker output limit exceeded")
            playback = Playback(done=self.loop.create_future())
            self.pending.append(playback)
            if self.current is None or self.current.done.done():
                self.current = playback
        self._resume()
        return playback

    def _resume(self):
        if self.paused:
            self.stream.start()
            self.paused = False

    def write(self, playback, data):
        with self.lock:
            if playback.done.done() and not playback.done.cancelled():
                error = playback.done.exception()
                if error is not None:
                    raise error
            if playback.interrupted:
                return
            if playback.sealed:
                raise RuntimeError("Audio arrived after playback was sealed")
            if sum(len(item.buffer) for item in self.pending) + len(data) > 48000 * 30:
                raise RuntimeError("Speaker queue exceeded 30 seconds")
            playback.buffer.extend(data)
        self._resume()

    def end(self, playback):
        with self.lock:
            playback.sealed = True

    @staticmethod
    def _complete(playback, position):
        if not playback.done.done():
            playback.done.set_result(position)

    def _schedule_completion(self, playback, delay, position):
        if not playback.done.done():
            playback.timer = self.loop.call_later(delay, self._complete, playback, position)

    def _render(self, output, frames, clock, status):
        output[:] = bytes(len(output))
        # Callback flags report gaps or priming, not a failed stream.
        if status:
            self.loop.call_soon_threadsafe(print, f"Speaker: {status}")
        with self.lock:
            self.pending = [item for item in self.pending if not item.done.done()]
            playback = self.pending[0] if self.pending else None
            if playback is None or playback.interrupted:
                return
            self.current = playback
            now = time.monotonic()
            if not all(
                math.isfinite(value)
                for value in (now, clock.outputBufferDacTime, clock.currentTime)
            ):
                self.loop.call_soon_threadsafe(self._fail, RuntimeError("Invalid speaker clock"))
                return
            # Keep timing records only for audio that has not yet reached the DAC.
            playback.elapsed += sum(
                length for start, length in playback.runs if start + length <= now
            )
            playback.runs = [
                (start, length) for start, length in playback.runs if start + length > now
            ]
            count = min(len(output), len(playback.buffer))
            if count:
                output[:count] = bytes(playback.buffer[:count])
                del playback.buffer[:count]
                start = now + max(0, clock.outputBufferDacTime - clock.currentTime)
                playback.runs.append((start, count / 48000))
            if playback.sealed and not playback.buffer and not playback.completion_scheduled:
                playback.completion_scheduled = True
                finish = max((start + length for start, length in playback.runs), default=now)
                duration = playback.elapsed + sum(length for _, length in playback.runs)
                self.loop.call_soon_threadsafe(
                    self._schedule_completion, playback, max(0, finish - now), duration
                )

    def interrupt(self, playback=None):
        with self.lock:
            playback = playback or self.current
            if playback is None or playback.done.done() or playback.interrupted:
                return None
            playback.interrupted = True
            playback.buffer.clear()
            if playback.timer:
                playback.timer.cancel()
        # Abort discards device buffers as well as our Python queue.
        active = playback is self.current
        if active:
            self.stopping = True
            try:
                self.stream.abort()
                self.paused = True
            finally:
                self.stopping = False
        position = playback.played(time.monotonic())
        self._complete(playback, position)
        # The runner may still be stopping later outputs. Resume only when new
        # output is opened or written, not as a side effect of interruption.
        return playback

    def __enter__(self):
        self.stream.start()
        return self

    def __exit__(self, *exc):
        self._stop()
        self.stopping = True
        self.stream.close()


class DeviceOutput:
    """Translate the device receipt in seconds to the runner's milliseconds."""

    def __init__(self, speaker, playback):
        self.speaker = speaker
        self.playback = playback

    def write(self, chunk):
        if chunk.format != PCMFormat(sample_rate=24000):
            raise ValueError("Speaker requires mono 24 kHz PCM16")
        self.speaker.write(self.playback, chunk.data)

    def end(self):
        self.speaker.end(self.playback)

    def interrupt(self):
        self.speaker.interrupt(self.playback)

    async def completion(self):
        return 1000 * await asyncio.shield(self.playback.done)
