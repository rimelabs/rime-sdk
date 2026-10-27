import WebSocket from "ws";
import {
  RimeAuthenticationError,
  RimeError,
  RimeResourceLimitError,
  RimeStreamError,
  RimeUnavailableError,
} from "../errors.js";
import { Deferred } from "./async.js";

function handshakeError(
  status: number,
  body: Buffer,
  requestId: string | null,
) {
  let errorType = RimeUnavailableError;
  let reason = "Realtime endpoint rejected the connection";
  if ([401, 403].includes(status)) {
    errorType = RimeAuthenticationError;
    reason = "Realtime endpoint rejected credentials";
  } else if (status === 429) {
    errorType = RimeResourceLimitError;
    reason = "Realtime connection or rate limit reached";
  } else if (status === 503) {
    reason = "Realtime endpoint is unavailable";
  }
  let message = `${reason} (HTTP ${status})`;
  if (body.length > 0 && body.length < 1024) {
    try {
      let detail: unknown = new TextDecoder("utf-8", { fatal: true })
        .decode(body)
        .trim();
      if (typeof detail === "string" && /^[{[]/.test(detail))
        detail = JSON.parse(detail)?.error?.message;
      if (
        typeof detail === "string" &&
        !detail.trimStart().startsWith("<") &&
        !/[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(detail)
      ) {
        detail = detail.trim().replace(/\s+/gu, " ");
        if (detail) message += `: ${detail}`;
      }
    } catch {
      // Invalid response bodies do not change the HTTP error category.
    }
  }
  return new errorType(message, requestId);
}

/** Private transport. The public declarations do not require ws types. */
export class RealtimeConnection {
  private readonly socket;
  private readonly opened = new Deferred<void>();
  private readonly ended = new Deferred<void>();
  private closing: Promise<void> | null = null;
  private connected = false;
  private failed = false;
  private refusal: (() => void) | undefined;
  readonly ready = this.opened.promise;
  onMessage: (raw: string) => void = () => {};
  onFailure: (error: RimeError) => void = () => {};

  constructor(endpoint: string, authorization: string, requestId?: string) {
    this.socket = new WebSocket(endpoint, {
      headers: {
        Authorization: authorization,
        ...(requestId === undefined ? {} : { "x-request-id": requestId }),
      },
      followRedirects: false,
      perMessageDeflate: false,
      maxPayload: 1024 * 1024,
    });
    this.socket.on("open", () => {
      this.connected = true;
      this.opened.resolve();
    });
    this.socket.on("unexpected-response", (request, response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      let finished = false;
      const finish = (complete = false) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.refusal = undefined;
        const id = response.headers["x-request-id"];
        this.failure(
          handshakeError(
            response.statusCode ?? 0,
            complete ? Buffer.concat(chunks) : Buffer.alloc(0),
            typeof id === "string" ? id : null,
          ),
        );
        response.destroy();
        request.destroy();
        this.socket.terminate();
      };
      const timer = setTimeout(finish, 1000);
      this.refusal = finish;
      response.on("data", (chunk: Buffer) => {
        size += chunk.length;
        if (size >= 1024) finish();
        else chunks.push(chunk);
      });
      response.on("end", () => finish(true));
      response.on("error", () => finish());
      response.on("aborted", () => finish());
    });
    this.socket.on("error", () =>
      this.refusal
        ? this.refusal()
        : this.failure(
            !this.connected
              ? new RimeUnavailableError("Realtime handshake failed")
              : new RimeStreamError(
                  "Invalid event or lost realtime connection",
                ),
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
      this.refusal?.();
      this.ended.resolve();
      this.failure(
        new RimeStreamError(
          "Realtime connection closed; reconnect is not automatic",
        ),
      );
    });
  }

  private failure(error: RimeError) {
    if (this.failed) return;
    this.failed = true;
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
        this.refusal?.();
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
