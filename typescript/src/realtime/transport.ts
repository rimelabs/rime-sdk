import WebSocket from "ws";
import {
  RimeAuthenticationError,
  RimeError,
  RimeStreamError,
  RimeUnavailableError,
} from "../errors.js";
import { Deferred } from "./async.js";

/** Private transport. The public declarations do not require ws types. */
export class RealtimeConnection {
  private readonly socket;
  private readonly opened = new Deferred<void>();
  private readonly ended = new Deferred<void>();
  private closing: Promise<void> | null = null;
  private connected = false;
  readonly ready = this.opened.promise;
  onMessage: (raw: string) => void = () => {};
  onFailure: (error: RimeError) => void = () => {};

  constructor(endpoint: string, authorization: string) {
    this.socket = new WebSocket(endpoint, {
      headers: { Authorization: authorization },
      followRedirects: false,
      perMessageDeflate: false,
      maxPayload: 1024 * 1024,
    });
    this.socket.on("open", () => {
      this.connected = true;
      this.opened.resolve();
    });
    this.socket.on("unexpected-response", (request, response) => {
      const error = [401, 403].includes(response.statusCode ?? 0)
        ? new RimeAuthenticationError("Realtime endpoint rejected credentials")
        : new RimeUnavailableError("Realtime endpoint rejected the connection");
      this.failure(error);
      response.destroy();
      request.destroy();
      this.socket.terminate();
    });
    this.socket.on("error", () =>
      this.failure(
        !this.connected
          ? new RimeUnavailableError("Realtime handshake failed")
          : new RimeStreamError("Invalid event or lost realtime connection"),
      ),
    );
    this.socket.on("message", (data, binary) => {
      if (binary) {
        this.failure(new RimeStreamError("Expected a JSON text event"));
        return;
      }
      this.onMessage(data.toString());
    });
    this.socket.on("close", () => {
      this.ended.resolve();
      this.failure(
        new RimeStreamError(
          "Realtime connection closed; reconnect is not automatic",
        ),
      );
    });
  }

  private failure(error: RimeError) {
    this.opened.reject(error);
    this.onFailure(error);
  }

  send(encoded: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket.send(encoded, (error) => {
        if (error)
          reject(new RimeStreamError("Realtime write failed; outcome unknown"));
        else resolve();
      });
    });
  }

  close(): Promise<void> {
    if (!this.closing) {
      this.closing = (async () => {
        if (this.socket.readyState === WebSocket.CLOSED) return;
        const timer = setTimeout(() => this.socket.terminate(), 2000);
        try {
          if (this.socket.readyState === WebSocket.CONNECTING)
            this.socket.terminate();
          else this.socket.close();
          await this.ended.promise;
        } finally {
          clearTimeout(timer);
        }
      })();
    }
    return this.closing;
  }
}
