// Talk to Prism from your browser. This server keeps your API key: the browser sends
// microphone audio here, and the server passes it to Prism and sends the replies back.
// Run: RIME_API_KEY=... node server.mjs, then open http://localhost:3000
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { WebSocketServer } from "ws";
import { Rime, runConversation } from "@rimelabs/sdk";

const PORT = Number(process.env.PORT ?? 3000);
const VOICES = ["astra", "aurora", "brando", "wawona"];
const FILES = {
  "/": ["index.html", "text/html"],
  "/app.js": ["app.js", "text/javascript"],
  "/player.js": ["player.js", "text/javascript"],
  "/capture.js": ["capture.js", "text/javascript"],
};

if (!process.env.RIME_API_KEY) {
  console.error('Set RIME_API_KEY first: export RIME_API_KEY="your-api-key"');
  process.exit(1);
}

// The page's name for one reply's audio.
function outputId(output) {
  return JSON.stringify([
    output.response.responseId,
    output.itemId,
    output.outputIndex,
    output.contentIndex,
  ]);
}

// One browser tab. The conversation runner reads the microphone from it (source)
// and plays each reply through it (playback). The page reports what it played.
class Browser {
  constructor(socket) {
    this.socket = socket;
    this.frames = [];
    this.receipts = new Map();
    this.stopping = undefined;
    this.stops = 0;
    this.closed = false;
    socket.on("message", (data, binary) => {
      if (binary) {
        if (this.frames.length < 100) this.frames.push(Buffer.from(data)); // 40 ms of 48 kHz mono PCM16
        this.wake?.();
        return;
      }
      const report = JSON.parse(data.toString());
      if (report.type === "played")
        this.receipts.get(report.id)?.resolve(report.playedMs);
      if (report.type === "stopped" && report.id === this.stopping?.id)
        this.stopping.resolve();
    });
    socket.on("close", () => this.close());
  }

  send(value) {
    if (this.closed) throw new Error("The browser disconnected");
    if (this.socket.bufferedAmount > 2 * 1024 * 1024)
      throw new Error("The browser is not keeping up");
    this.socket.send(JSON.stringify(value));
  }

  // Source: the next microphone frame, or null when the page closes.
  async read(signal) {
    while (!this.closed) {
      signal.throwIfAborted();
      const data = this.frames.shift();
      if (data) return { data, format: { sampleRate: 48000, channels: 1 } };
      await new Promise((resolve, reject) => {
        const abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
        this.wake = () => {
          signal.removeEventListener("abort", abort);
          this.wake = undefined;
          resolve();
        };
      });
    }
    return null;
  }

  // Playback: one output per reply. The page plays outputs in order, one at a time.
  open(output) {
    const id = outputId(output);
    let resolve;
    const receipt = new Promise((accept) => (resolve = accept));
    this.receipts.set(id, { resolve });
    this.send({ type: "begin", id });
    return {
      write: (chunk) =>
        this.send({
          type: "audio",
          id,
          data: Buffer.from(chunk.data).toString("base64"),
        }),
      end: () => this.send({ type: "end", id }),
      completion: async (signal) => {
        signal.throwIfAborted();
        try {
          return await new Promise((accept, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason), {
              once: true,
            });
            receipt.then(accept);
          });
        } finally {
          this.receipts.delete(id);
        }
      },
    };
  }

  // Playback: stop these replies together. The page then plays what is still queued.
  interrupt(outputs) {
    this.send({ type: "interrupt", ids: outputs.map(outputId) });
  }

  // Playback: stop everything, and return when the page confirms that it is silent.
  async stop(signal) {
    const id = String(++this.stops);
    await new Promise((resolve, reject) => {
      this.stopping = { id, resolve };
      signal.addEventListener("abort", () => reject(signal.reason), {
        once: true,
      });
      this.send({ type: "stop", id });
    });
  }

  close() {
    this.closed = true;
    this.wake?.();
  }
}

async function talk(ws, start) {
  const browser = new Browser(ws);
  const client = new Rime();
  try {
    const session = await client.realtime.connect({
      endpoint: process.env.PRISM_URL, // The SDK uses the public Prism address when this is unset.
      voice: VOICES.includes(start.voice) ? start.voice : "astra",
      instructions:
        String(start.instructions ?? "").trim() ||
        "Answer in one or two short sentences.",
    });
    browser.send({ type: "ready" });
    // The runner sends the microphone, plays replies in order, handles interruptions,
    // and reports what the caller heard. The callback shows the transcript.
    await runConversation(session, {
      source: browser,
      playback: browser,
      onEvent({ payload }) {
        if (payload.kind === "transcript.final")
          browser.send({ type: "text", role: "You", text: payload.text });
        if (payload.kind === "text.done")
          browser.send({ type: "text", role: "Prism", text: payload.text });
        if (payload.kind === "error")
          browser.send({
            type: "error",
            message: `Prism: ${payload.error.message}`,
          });
      },
    });
  } catch (error) {
    if (!browser.closed) {
      console.error(error);
      browser.send({ type: "error", message: String(error.message ?? error) });
    }
  } finally {
    browser.close();
    ws.close();
    await client.close();
  }
}

const server = createServer(async (request, response) => {
  const file = FILES[request.url];
  if (!file) return void response.writeHead(404).end();
  response.writeHead(200, {
    "Content-Type": file[1],
    "Cache-Control": "no-store",
  });
  response.end(await readFile(new URL(`./public/${file[0]}`, import.meta.url)));
});

// Accept only this page, from this computer, one conversation at a time. A new
// conversation can start as soon as the page closes the old one's connection.
const sockets = new WebSocketServer({ noServer: true, maxPayload: 192000 });
let current; // The page connection that holds the conversation
server.on("upgrade", (request, socket, head) => {
  const host = request.headers.host;
  const local = host === `localhost:${PORT}` || host === `127.0.0.1:${PORT}`;
  if (
    current?.readyState <= 1 || // CONNECTING or OPEN
    request.url !== "/audio" ||
    !local ||
    request.headers.origin !== `http://${host}`
  )
    return void socket.destroy();
  const placeholder = (current = { readyState: 0 });
  socket.once("close", () => current === placeholder && (current = undefined));
  sockets.handleUpgrade(request, socket, head, (ws) => {
    current = ws;
    ws.once("message", (data) => void talk(ws, JSON.parse(data.toString())));
  });
});

server.listen(PORT, "127.0.0.1", () =>
  console.log(`Open http://localhost:${PORT} and press Start.`),
);
