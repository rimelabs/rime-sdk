# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rimelabs-sdk==0.1.0a19",
#     "aiohttp>=3.9,<4",
# ]
# ///
"""Talk to Prism from your browser. This server keeps your API key: the browser sends
microphone audio here, and the server passes it to Prism and sends the replies back.

Run: RIME_API_KEY=... uv run server.py, then open http://localhost:3000
"""

import asyncio
import base64
import itertools
import json
import os
import sys
from pathlib import Path

from aiohttp import WSMsgType, web

from rimelabs_sdk import Rime
from rimelabs_sdk import realtime as r

PORT = int(os.getenv("PORT", "3000"))
VOICES = ("astra", "aurora", "brando", "wawona")
PUBLIC = Path(__file__).parent / "public"
FILES = {
    "/": "index.html",
    "/app.js": "app.js",
    "/player.js": "player.js",
    "/capture.js": "capture.js",
}
MIC_FORMAT = r.PCMFormat(sample_rate=48000, channels=1)


def page_id(output):
    """The page's name for one reply's audio."""
    ref = output.response.response_id, output.item_id, output.output_index, output.content_index
    return json.dumps(ref)


class Output:
    """One reply on the page. The page reports how much of it played."""

    def __init__(self, browser, output_id):
        self.browser = browser
        self.id = output_id
        self.receipt = asyncio.get_running_loop().create_future()

    def write(self, chunk):
        data = base64.b64encode(chunk.data).decode()
        self.browser.send({"type": "audio", "id": self.id, "data": data})

    def end(self):
        self.browser.send({"type": "end", "id": self.id})

    async def completion(self):
        try:
            return await asyncio.shield(self.receipt)
        finally:
            if self.receipt.done():
                self.browser.outputs.pop(self.id, None)


class Browser:
    """One browser tab. The conversation runner reads the microphone from it (source)
    and plays each reply through it (playback)."""

    def __init__(self, ws):
        self.ws = ws
        self.frames = asyncio.Queue(maxsize=100)
        self.outbox = asyncio.Queue(maxsize=2000)
        self.outputs: dict[str, Output] = {}
        self.stopping: tuple[str, asyncio.Future] | None = None
        self.stop_ids = itertools.count(1)
        self.closed = asyncio.Event()

    def send(self, value):
        """Queue a message for the page without blocking. One task sends them in order."""
        if self.closed.is_set():
            raise ConnectionError("The browser disconnected")
        self.outbox.put_nowait(json.dumps(value))  # QueueFull means the page is not keeping up

    async def deliver(self):
        try:
            while True:
                await self.ws.send_str(await self.outbox.get())
        except ConnectionError:
            self.closed.set()

    async def receive(self):
        async for message in self.ws:
            if message.type == WSMsgType.BINARY:
                if not self.frames.full():
                    self.frames.put_nowait(message.data)  # 40 ms of 48 kHz mono PCM16
            elif message.type == WSMsgType.TEXT:
                report = json.loads(message.data)
                output = self.outputs.get(report.get("id"))
                if report.get("type") == "played" and output and not output.receipt.done():
                    output.receipt.set_result(float(report["playedMs"]))
                if report.get("type") == "stopped" and self.stopping:
                    stop_id, confirmed = self.stopping
                    if report.get("id") == stop_id and not confirmed.done():
                        confirmed.set_result(None)
        # The page closed: end the input, and fail a stop that can no longer be confirmed.
        self.closed.set()
        if not self.frames.full():
            self.frames.put_nowait(None)
        if self.stopping and not self.stopping[1].done():
            self.stopping[1].set_exception(ConnectionError("The browser closed during shutdown"))

    # Source: the next microphone frame, or None when the page closes.
    async def read(self):
        if self.closed.is_set() and self.frames.empty():
            return None
        frame = await self.frames.get()
        return None if frame is None else r.AudioChunk(data=frame, format=MIC_FORMAT)

    # Playback: one output per reply. The page plays outputs in order, one at a time.
    def open(self, output):
        output_id = page_id(output)
        self.outputs[output_id] = Output(self, output_id)
        self.send({"type": "begin", "id": output_id})
        return self.outputs[output_id]

    # Playback: stop these replies together. The page then plays what is still queued.
    def interrupt(self, outputs):
        self.send({"type": "interrupt", "ids": [page_id(output) for output in outputs]})

    # Playback: stop everything, and return when the page confirms that it is silent.
    async def stop(self):
        stop_id = str(next(self.stop_ids))
        confirmed = asyncio.get_running_loop().create_future()
        self.stopping = (stop_id, confirmed)
        self.send({"type": "stop", "id": stop_id})
        await confirmed


async def talk(ws, start):
    browser = Browser(ws)
    voice = start.get("voice") if start.get("voice") in VOICES else "astra"
    instructions = (
        str(start.get("instructions") or "").strip() or "Answer in one or two short sentences."
    )

    def show(event):
        # The runner calls this for every event. Show the transcript on the page.
        payload = event.payload
        if isinstance(payload, r.TranscriptFinal):
            browser.send({"type": "text", "role": "You", "text": payload.text})
        elif isinstance(payload, r.TextDone):
            browser.send({"type": "text", "role": "Prism", "text": payload.text})
        elif isinstance(payload, r.FaultEvent):
            browser.send({"type": "error", "message": f"Prism: {payload.error.message}"})

    tasks = [asyncio.create_task(browser.deliver()), asyncio.create_task(browser.receive())]
    try:
        async with (
            Rime() as client,
            # The SDK uses the public Prism address when PRISM_URL is unset.
            client.realtime.connect(
                endpoint=os.getenv("PRISM_URL"), voice=voice, instructions=instructions
            ) as session,
        ):
            browser.send({"type": "ready"})
            # The runner sends the microphone, plays replies in order, handles
            # interruptions, and reports what the caller heard.
            await r.run_conversation(session, source=browser, playback=browser, on_event=show)
    except Exception as error:  # noqa: BLE001
        if not browser.closed.is_set():
            print(f"{type(error).__name__}: {error}", file=sys.stderr)
            browser.send({"type": "error", "message": str(error)})
            await asyncio.sleep(0.2)  # let the error reach the page
    finally:
        for task in tasks:
            task.cancel()


current = None  # The page connection that holds the conversation


async def audio_socket(request):
    """Accept only this page, from this computer, one conversation at a time. A new
    conversation can start as soon as the page closes the old one's connection."""
    global current
    host = request.headers.get("Host", "")
    local = host in (f"localhost:{PORT}", f"127.0.0.1:{PORT}")
    in_use = current is not None and not current.closed
    if in_use or not local or request.headers.get("Origin") != f"http://{host}":
        raise web.HTTPForbidden()
    ws = current = web.WebSocketResponse(max_msg_size=192000)
    await ws.prepare(request)
    try:
        start = json.loads(await ws.receive_str())
        await talk(ws, start)
    except (TypeError, ValueError) as error:
        # The page closed, or sent something other than its start message.
        print(f"Ignored a connection without a start message: {error}", file=sys.stderr)
    finally:
        await ws.close()
    return ws


async def static(request):
    name = FILES.get(request.path)
    if name is None:
        raise web.HTTPNotFound()
    kind = "text/html" if name.endswith(".html") else "text/javascript"
    body = (PUBLIC / name).read_bytes()
    return web.Response(body=body, content_type=kind, headers={"Cache-Control": "no-store"})


def main():
    if not os.getenv("RIME_API_KEY"):
        raise SystemExit('Set RIME_API_KEY first: export RIME_API_KEY="your-api-key"')
    app = web.Application()
    app.router.add_get("/audio", audio_socket)
    app.router.add_get("/{path:.*}", static)
    print(f"Open http://localhost:{PORT} and press Start.", flush=True)
    web.run_app(app, host="127.0.0.1", port=PORT, print=None)


if __name__ == "__main__":
    main()
