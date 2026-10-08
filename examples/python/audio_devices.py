"""Local PCM devices only. No SDK, protocol, or LiveKit dependencies.

Playback receipts use PortAudio's DAC clock, including the last device buffer.
Use headphones for simultaneous speaking and listening. Capture stays active
during playback; this helper does not implement acoustic echo cancellation.
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field


def sounddevice():
    try:
        import sounddevice as sd
    except (ImportError, OSError) as error:
        raise RuntimeError(
            "Install the examples audio extra and PortAudio; see examples/README.md"
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
        value = RuntimeError(f"Microphone: {status}") if status else bytes(data)
        self.loop.call_soon_threadsafe(self._offer, value)

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
        return value

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

    def played(self, now: float) -> float:
        return sum(min(duration, max(0, now - start)) for start, duration in self.runs)


class Speaker:
    """One active 24 kHz mono stream. All public methods run on the event loop."""

    def __init__(self, device=None):
        self.loop = asyncio.get_running_loop()
        self.lock = threading.Lock()
        self.current = None
        self.stream = sounddevice().RawOutputStream(
            device=device,
            samplerate=24000,
            channels=1,
            dtype="int16",
            blocksize=480,
            callback=self._render,
        )

    def begin(self):
        with self.lock:
            if self.current is not None and not self.current.done.done():
                raise RuntimeError("New playback started before the previous segment finished")
            self.current = Playback(done=self.loop.create_future())
            return self.current

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
            if len(playback.buffer) + len(data) > 48000 * 30:
                raise RuntimeError("Speaker queue exceeded 30 seconds")
            playback.buffer.extend(data)

    def end(self, playback):
        with self.lock:
            playback.sealed = True

    @staticmethod
    def _complete(playback, position):
        if not playback.done.done():
            playback.done.set_result(position)

    def _schedule_completion(self, playback, delay, position):
        self.loop.call_later(delay, self._complete, playback, position)

    def _render(self, output, frames, clock, status):
        output[:] = bytes(len(output))
        with self.lock:
            playback = self.current
            if playback is None or playback.interrupted or playback.done.done():
                return
            if status:
                self.loop.call_soon_threadsafe(
                    self._fail, playback, RuntimeError(f"Speaker: {status}")
                )
                return
            now = time.monotonic()
            count = min(len(output), len(playback.buffer))
            if count:
                output[:count] = bytes(playback.buffer[:count])
                del playback.buffer[:count]
                start = now + max(0, clock.outputBufferDacTime - clock.currentTime)
                playback.runs.append((start, count / 48000))
            if playback.sealed and not playback.buffer and not playback.completion_scheduled:
                playback.completion_scheduled = True
                finish = max((start + length for start, length in playback.runs), default=now)
                duration = sum(length for _, length in playback.runs)
                self.loop.call_soon_threadsafe(
                    self._schedule_completion, playback, max(0, finish - now), duration
                )

    @staticmethod
    def _fail(playback, error):
        if not playback.done.done():
            playback.done.set_exception(error)

    def interrupt(self):
        with self.lock:
            playback = self.current
            if playback is None or playback.done.done() or playback.interrupted:
                return None
            playback.interrupted = True
            playback.buffer.clear()
        # Abort discards device buffers as well as our Python queue.
        self.stream.abort()
        position = playback.played(time.monotonic())
        self._complete(playback, position)
        self.stream.start()
        return playback

    def __enter__(self):
        self.stream.start()
        return self

    def __exit__(self, *exc):
        self.interrupt()
        self.stream.close()
