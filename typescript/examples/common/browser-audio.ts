import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { WebSocketServer, WebSocket } from "ws";

export interface Playback {
  interrupted: boolean;
  playedMs: number;
}

/** Local demo transport. The browser owns microphone capture and the output clock. */
export class BrowserAudio {
  private queue: Buffer[] = [];
  private wake: (() => void) | undefined;
  private ended = false;
  private error: Error | undefined;
  private playbacks = new Map<
    string,
    {
      done: Promise<Playback>;
      resolve: (value: Playback) => void;
      reject: (error: Error) => void;
    }
  >();
  constructor(private socket: WebSocket) {
    socket.on("message", (raw, binary) => {
      try {
        const data = Buffer.from(raw as Buffer);
        if (binary) {
          if (data.length !== 3840)
            throw new Error("Expected 40 ms of mono 48 kHz PCM16");
          if (this.queue.length >= 100)
            throw new Error("Microphone queue exceeded four seconds");
          this.queue.push(data);
          this.wake?.();
        } else {
          const report = JSON.parse(data.toString());
          if (
            report.type !== "played" ||
            typeof report.id !== "string" ||
            typeof report.interrupted !== "boolean" ||
            !Number.isFinite(report.playedMs) ||
            report.playedMs < 0
          )
            throw new Error("Invalid playback report");
          this.playbacks.get(report.id)?.resolve(report);
        }
      } catch (error) {
        this.close(error as Error);
      }
    });
    socket.on("close", () => this.close());
    socket.on("error", (error) => this.close(error));
  }
  send(value: object) {
    if (this.ended) throw this.error ?? new Error("Browser disconnected");
    if (this.socket.bufferedAmount > 2 * 1024 * 1024)
      throw new Error("Browser output queue is full");
    this.socket.send(JSON.stringify(value));
  }
  async *frames() {
    while (!this.ended) {
      const frame = this.queue.shift();
      if (frame) yield frame;
      else
        await new Promise<void>((resolve) => {
          this.wake = resolve;
        });
    }
    if (this.error) throw this.error;
  }
  begin(id: string) {
    let resolve!: (value: Playback) => void;
    let reject!: (error: Error) => void;
    const done = new Promise<Playback>((accept, refuse) => {
      resolve = accept;
      reject = refuse;
    });
    // Disconnect can occur before generation ends and finish() starts waiting.
    void done.catch(() => {});
    this.playbacks.set(id, { done, resolve, reject });
    this.send({ type: "begin", id });
  }
  write(id: string, data: Uint8Array) {
    this.send({
      type: "audio",
      id,
      data: Buffer.from(data).toString("base64"),
    });
  }
  async finish(id: string): Promise<Playback> {
    const playback = this.playbacks.get(id);
    if (!playback) throw new Error("Unknown playback");
    try {
      this.send({ type: "end", id });
      return await playback.done;
    } finally {
      this.playbacks.delete(id);
    }
  }
  interrupt() {
    this.send({ type: "interrupt" });
  }
  close(error?: Error) {
    if (this.ended) return;
    this.error = error;
    this.ended = true;
    this.queue = [];
    this.wake?.();
    // A lost connection gives no measurement of the device's playback position.
    for (const playback of this.playbacks.values())
      playback.reject(error ?? new Error("Browser disconnected"));
    this.socket.close();
  }
}

export function serve(
  mode: "tts" | "prism",
  run: (audio: BrowserAudio) => Promise<void>,
) {
  const port = Number(process.env.PORT ?? 3000);
  const origin = `http://127.0.0.1:${port}`;
  const files: Record<string, [string, string]> = {
    "/": ["index.html", "text/html"],
    "/app.js": ["app.js", "text/javascript"],
    "/player.js": ["player.js", "text/javascript"],
    "/capture.js": ["capture.js", "text/javascript"],
  };
  const server = createServer(async (request, response) => {
    const file = files[request.url ?? ""];
    if (request.headers.host !== `127.0.0.1:${port}` || !file) {
      response.writeHead(404).end();
      return;
    }
    try {
      response.writeHead(200, {
        "Content-Type": file[1],
        "Cache-Control": "no-store",
      });
      response.end(
        await readFile(new URL(`../browser/${file[0]}`, import.meta.url)),
      );
    } catch {
      response.end();
    }
  });
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 192000 });
  let busy = false;
  server.on("upgrade", (request, socket, head) => {
    if (
      busy ||
      request.url !== "/audio" ||
      request.headers.origin !== origin ||
      request.headers.host !== `127.0.0.1:${port}`
    ) {
      socket.destroy();
      return;
    }
    busy = true;
    sockets.handleUpgrade(request, socket, head, (ws) =>
      sockets.emit("connection", ws),
    );
  });
  sockets.on("connection", (socket) => {
    const audio = new BrowserAudio(socket);
    audio.send({ type: "mode", mode });
    void run(audio)
      .catch((error) => {
        console.error(error);
        try {
          audio.send({ type: "error", message: String(error) });
        } catch {
          /* disconnected */
        }
      })
      .finally(() => {
        audio.close();
        busy = false;
      });
  });
  server.listen(port, "127.0.0.1", () =>
    console.log(`Open ${origin}. Use headphones. Ctrl-C stops the server.`),
  );
  const stop = () => {
    for (const socket of sockets.clients) socket.terminate();
    sockets.close();
    server.close();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  return server;
}
