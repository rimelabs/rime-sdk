import { Credentials } from "../auth.js";
import { abortable } from "../cancellation.js";
import {
  RimeCancelledError,
  RimeInputError,
  RimeUnavailableError,
} from "../errors.js";
import { cancellation, scope } from "./async.js";
import { RealtimeSession } from "./session.js";
import { RealtimeConnection } from "./transport.js";
import type { RealtimeConnectOptions, RealtimeTimeouts } from "./types.js";
import { toolSettings } from "./settings.js";
import type { SessionSettings } from "./protocol.js";

const DEFAULT_ENDPOINT = "wss://prism.api.rime.ai/v1/realtime";

function endpoint(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new RimeInputError(
      "endpoint must be a full ws:// or wss:// URL ending in /v1/realtime",
    );
  }
  if (
    !["ws:", "wss:"].includes(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.hash ||
    url.pathname.replace(/\/+$/, "") !== "/v1/realtime"
  )
    throw new RimeInputError(
      "endpoint must be a full ws:// or wss:// URL ending in /v1/realtime",
    );
  if (
    url.protocol === "ws:" &&
    !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)
  )
    throw new RimeInputError(
      "Use wss:// outside localhost to protect credentials",
    );
  return url.href;
}

function timeouts(options: RealtimeTimeouts = {}): Required<RealtimeTimeouts> {
  const result = {
    connectS: options.connectS ?? 10,
    readyS: options.readyS ?? 30,
    requestS: options.requestS ?? 10,
  };
  if (
    Object.values(result).some(
      (value) =>
        typeof value !== "number" || !Number.isFinite(value) || value <= 0,
    )
  )
    throw new RimeInputError("Timeouts must be finite and positive");
  return result;
}

export class Realtime {
  private readonly sessions = new Set<RealtimeSession>();
  private readonly opening = new Map<
    AbortController,
    Promise<RealtimeSession>
  >();
  constructor(
    private readonly credentials: Credentials,
    private readonly checkOpen: () => void,
  ) {}

  async connect(
    options: RealtimeConnectOptions = {},
  ): Promise<RealtimeSession> {
    this.checkOpen();
    const address = endpoint(
      options.endpoint === undefined ? DEFAULT_ENDPOINT : options.endpoint,
    );
    const limits = timeouts(options.timeouts);
    if ((options.model ?? "prism") !== "prism")
      throw new RimeInputError("The realtime model must be prism");
    if (
      options.interruptOnSpeech !== undefined &&
      typeof options.interruptOnSpeech !== "boolean"
    )
      throw new RimeInputError("interruptOnSpeech must be a boolean");
    const settings: SessionSettings = {
      modalities: ["text", "audio"],
      input_audio_format: "pcm16",
      turn_detection: { interrupt_response: options.interruptOnSpeech ?? true },
      tools: toolSettings(options.tools ?? []),
    };
    if (options.turnDetection !== undefined) {
      const detection = options.turnDetection;
      if (
        !detection ||
        typeof detection !== "object" ||
        Array.isArray(detection)
      )
        throw new RimeInputError("turnDetection must be an object");
      if (detection.eagerness !== undefined) {
        if (!["low", "medium", "high", "auto"].includes(detection.eagerness))
          throw new RimeInputError(
            "Turn eagerness must be low, medium, high, or auto",
          );
        settings.turn_detection.eagerness = detection.eagerness;
      }
      if (detection.endOfTurnTimeoutMs !== undefined) {
        const value = detection.endOfTurnTimeoutMs;
        if (!Number.isInteger(value) || value < 1000 || value > 10000)
          throw new RimeInputError(
            "End-of-turn timeout must be an integer from 1000 to 10000 ms",
          );
        settings.turn_detection.prism_end_of_turn_timeout_ms = value;
      }
    }
    for (const name of ["voice", "instructions"] as const) {
      const value = options[name];
      if (value !== undefined && value !== null) {
        if (typeof value !== "string" || !value.trim())
          throw new RimeInputError(`${name} must be nonblank`);
        settings[name] = value;
      }
    }
    const owner = new AbortController();
    const caller = cancellation(options.signal);
    const openingScope = scope([caller.signal, owner.signal]);
    const task = (async () => {
      let session: RealtimeSession | undefined;
      try {
        openingScope.signal.throwIfAborted();
        let socket: RealtimeConnection;
        try {
          socket = new RealtimeConnection(
            address,
            this.credentials.realtimeAuthorization(),
          );
        } catch {
          throw new RimeUnavailableError("Realtime handshake failed");
        }
        session = new RealtimeSession(socket, limits, () =>
          this.sessions.delete(session!),
        );
        this.sessions.add(session);
        await abortable(
          session.initialize(settings, openingScope.signal),
          openingScope.signal,
        );
        openingScope.signal.throwIfAborted();
        this.checkOpen();
        return session;
      } catch (error) {
        await session?.close();
        throw error;
      }
    })();
    this.opening.set(owner, task);
    try {
      return await task;
    } finally {
      this.opening.delete(owner);
      openingScope.dispose();
      caller.dispose();
    }
  }

  async close(): Promise<void> {
    for (const controller of this.opening.keys())
      controller.abort(new RimeCancelledError("Client closed"));
    await Promise.allSettled(this.opening.values());
    await Promise.all([...this.sessions].map((session) => session.close()));
  }
}
