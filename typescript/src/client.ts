import { AudioFormat } from "./audio.js";
import { AudioStream, constructionKey, type TextSource } from "./stream.js";
import { Credentials } from "./auth.js";
import { policy, resolve, timeout, nonempty, abortable } from "./policy.js";
import {
  transport,
  connect,
  metadata,
  discover,
  type Connection,
  type DiscoveryKind,
} from "./transport.js";
import {
  RimeAuthenticationError,
  RimeInputError,
  RimeAudioFormatError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "./errors.js";
export interface RimeOptions {
  apiKey?: string | null;
  /** Select "coda" (default) or "mistv3" for speech and discovery. */
  model?: string;
  endpoint?: string | null;
  timeout?: number | null;
}
export interface SynthesisOptions {
  voice?: string | null;
  language?: string;
  audioFormat?: AudioFormat | null;
  timeout?: number | null;
}
export interface DiscoveryOptions {
  timeout?: number | null;
}
export interface VoiceListOptions extends DiscoveryOptions {
  language?: string | null;
}
export class Rime {
  private readonly deployment: Readonly<typeof policy>;
  private credentials: Credentials;
  private client: Connection | null = null;
  private closed = false;
  private streams = new Set<AudioStream>();
  private discoveryControllers = new Set<AbortController>();
  private closing: Promise<void> | null = null;
  private defaultTimeout: number | null;
  readonly tts: {
    stream: (text: TextSource, options?: SynthesisOptions) => AudioStream;
  };
  readonly voices: { list: (options?: VoiceListOptions) => Promise<string[]> };
  readonly languages: {
    list: (options?: DiscoveryOptions) => Promise<string[]>;
  };
  constructor(options: RimeOptions = {}) {
    const key =
      options.apiKey === undefined || options.apiKey === null
        ? process.env.RIME_API_KEY
        : options.apiKey;
    if (typeof key !== "string" || !key.trim())
      throw new RimeAuthenticationError("Provide apiKey or set RIME_API_KEY");
    this.defaultTimeout = timeout(options.timeout);
    this.deployment = resolve(options.model ?? "coda", options.endpoint);
    this.credentials = new Credentials(key, this.deployment);
    this.tts = { stream: (text, options = {}) => this.stream(text, options) };
    this.voices = {
      list: async (options = {}) => {
        if (options.language !== undefined && options.language !== null)
          nonempty(options.language, "language");
        return this.discover("voices", options.language, options.timeout);
      },
    };
    this.languages = {
      list: (options = {}) =>
        this.discover("languages", undefined, options.timeout),
    };
  }
  private checkOpen() {
    if (this.closed) throw new RimeInputError("The Rime client is closed");
  }
  private async prepare(signal: AbortSignal) {
    this.checkOpen();
    const auth = await this.credentials.metadata(signal);
    this.checkOpen();
    this.client ??= transport.makeClient(this.deployment.target);
    await connect(this.client, signal);
    return { client: this.client, metadata: metadata(auth) };
  }
  private stream(text: TextSource, options: SynthesisOptions) {
    this.checkOpen();
    if (typeof text === "string") {
      if (!text.trim())
        throw new RimeInputError("Text must contain non-whitespace characters");
    } else if (!text || typeof text[Symbol.asyncIterator] !== "function")
      throw new RimeInputError(
        "text must be a string or an async iterable of strings",
      );
    const voice =
      options.voice === undefined || options.voice === null
        ? this.deployment.defaultVoice
        : nonempty(options.voice, "voice");
    const language = nonempty(options.language ?? "en", "language");
    const format = options.audioFormat ?? AudioFormat.PCM_24000;
    if (format !== AudioFormat.PCM_24000 && format !== AudioFormat.MULAW_8000)
      throw new RimeAudioFormatError("Select a named AudioFormat profile");
    const audio = new AudioStream(
      constructionKey,
      {
        prepare: (s) => this.prepare(s),
        checkOpen: () => this.checkOpen(),
        forget: (s) => this.streams.delete(s),
      },
      text,
      voice,
      language,
      format,
      timeout(options.timeout, this.defaultTimeout),
    );
    this.streams.add(audio);
    return audio;
  }
  private async discover(
    kind: DiscoveryKind,
    language: string | null | undefined,
    override: number | null | undefined,
  ): Promise<string[]> {
    this.checkOpen();
    const budget = Math.min(
      timeout(override, this.defaultTimeout) ?? Infinity,
      policy.discoveryTimeout,
    );
    const controller = new AbortController();
    let requestId: string | null = null;
    this.discoveryControllers.add(controller);
    const timer = setTimeout(
      () =>
        controller.abort(new RimeTimeoutError("Discovery deadline expired")),
      budget * 1000,
    );
    try {
      const prepared = await this.prepare(controller.signal);
      for (let attempt = 0; ; attempt++) {
        requestId = null;
        try {
          return await discover(prepared, kind, language, controller.signal);
        } catch (error) {
          if (!(error instanceof RimeUnavailableError) || attempt === 2)
            throw error;
          requestId = error.requestId;
          await abortable(
            new Promise<void>((resolve) =>
              setTimeout(resolve, 50 * 2 ** attempt),
            ),
            controller.signal,
          );
        }
      }
    } catch (error) {
      if (error instanceof RimeTimeoutError && requestId !== null)
        throw new RimeTimeoutError(error.message, requestId, {
          cause: error.cause,
        });
      throw error;
    } finally {
      clearTimeout(timer);
      this.discoveryControllers.delete(controller);
    }
  }
  async close(): Promise<void> {
    if (!this.closing) {
      this.closed = true;
      this.closing = (async () => {
        for (const controller of this.discoveryControllers)
          controller.abort(
            new RimeCancelledError("Client closed during discovery"),
          );
        await Promise.all([...this.streams].map((stream) => stream.cancel()));
        await this.credentials.close();
        this.client?.close();
      })();
    }
    await this.closing;
  }
  async [Symbol.asyncDispose]() {
    await this.close();
  }
}
