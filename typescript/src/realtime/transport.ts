import WebSocket from "ws";
import {
  RimeAuthenticationError,
  RimeError,
  RimeResourceLimitError,
  RimeStreamError,
  RimeUnavailableError,
} from "../errors.js";
import { Deferred } from "./async.js";

const months = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(" ");
const dayName = "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)";
const monthName = `(?:${months.join("|")})`;
const clock = "([0-9]{2}):([0-9]{2}):([0-9]{2})";
const imfDate = new RegExp(
  `^${dayName}, ([0-9]{2}) (${monthName}) ([0-9]{4}) ${clock} GMT$`,
);
const rfc850Date = new RegExp(
  `^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), ([0-9]{2})-(${monthName})-([0-9]{2}) ${clock} GMT$`,
);
const asctimeDate = new RegExp(
  `^${dayName} (${monthName}) ([0-9]{2}| [0-9]) ${clock} ([0-9]{4})$`,
);

function httpDate(value: string, now: number): number | null {
  let parts: string[] | null = imfDate.exec(value);
  let shortYear = false;
  if (!parts) {
    parts = rfc850Date.exec(value);
    shortYear = parts !== null;
  }
  if (!parts) {
    const legacy = asctimeDate.exec(value);
    if (!legacy) return null;
    parts = [
      legacy[0],
      legacy[2],
      legacy[1],
      legacy[6],
      legacy[3],
      legacy[4],
      legacy[5],
    ];
  }
  const day = Number(parts[1]);
  const month = months.indexOf(parts[2]);
  let year = Number(parts[3]);
  const hour = Number(parts[4]);
  const minute = Number(parts[5]);
  const second = Number(parts[6]);
  if (shortYear) {
    const current = new Date(now);
    const limit = [
      current.getUTCFullYear() + 50,
      current.getUTCMonth(),
      current.getUTCDate(),
      current.getUTCHours(),
      current.getUTCMinutes(),
      current.getUTCSeconds(),
    ];
    year += Math.floor(limit[0] / 100) * 100;
    const candidate = [year, month, day, hour, minute, second];
    const difference = candidate.findIndex(
      (part, index) => part !== limit[index],
    );
    if (difference !== -1 && candidate[difference] > limit[difference])
      year -= 100;
  }
  // Use setters so years 0000–0099 aren't silently changed to 1900–1999.
  const date = new Date(0);
  date.setUTCFullYear(year, month, day);
  date.setUTCHours(hour, minute, second, 0);
  if (
    year < 1 ||
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month ||
    date.getUTCDate() !== day ||
    date.getUTCHours() !== hour ||
    date.getUTCMinutes() !== minute ||
    date.getUTCSeconds() !== second
  )
    return null;
  return date.getTime();
}

function retryAfter(values: string[] | undefined): number | null {
  if (values?.length !== 1 || values[0].length > 256) return null;
  const value = values[0].trim();
  if (/^[0-9]+$/.test(value)) {
    const seconds = Number(value);
    return Number.isFinite(seconds) ? seconds : null;
  }
  const now = Date.now();
  const date = httpDate(value, now);
  return date === null ? null : Math.max(0, (date - now) / 1000);
}

function handshakeError(
  status: number,
  body: Buffer,
  requestId: string | null,
  retryAfterSeconds: number | null,
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
  return new errorType(message, requestId, {
    statusCode: status,
    retryAfterSeconds,
  });
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
            retryAfter(response.headersDistinct["retry-after"]),
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
    this.socket.on("close", (code) => {
      this.refusal?.();
      this.ended.resolve();
      this.failure(
        new RimeStreamError(
          "Realtime connection closed; reconnect is not automatic",
          null,
          { closeCode: code === 1005 || code === 1006 ? null : code },
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
