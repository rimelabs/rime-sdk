import { AudioFormat, resolveAudio } from "./audio.js";
import { maxBufferBytes, finalizeWav } from "./buffer.js";
import { snapshot, type PronunciationEntry } from "./lexicon.js";
import { AudioStream, constructionKey, type TextSource } from "./stream.js";
import { Credentials } from "../auth.js";
import { policy, timeout, nonempty } from "./policy.js";
import { abortable } from "../cancellation.js";
import {
  transport,
  connect,
  metadata,
  discover,
  type Connection,
  type DiscoveryKind,
} from "./transport.js";
import {
  RimeInputError,
  RimeResourceLimitError,
  RimeCancelledError,
  RimeTimeoutError,
  RimeUnavailableError,
} from "../errors.js";
export interface SynthesisOptions {
  voice?: string | null;
  language?: string;
  audioFormat?: AudioFormat | null;
  /** Requested output rate: 8000, 16000, or 24000 Hz. Defaults to 24000 for PCM/WAV/MP3. */
  sampleRate?: number | null;
  /** Duration multiplier from 0.4 to 2.5. Below 1 speeds speech up; above 1 slows it down. */
  timeScaleFactor?: number | null;
  /** Request final word timestamps. Supported only with model="mistv3". */
  timestamps?: boolean;
  /** Request-wide pronunciation overrides; requires a phoneme-capable Coda deployment. */
  customLexicon?: readonly PronunciationEntry[];
  timeout?: number | null;
}
export interface DiscoveryOptions {
  timeout?: number | null;
}
export interface VoiceListOptions extends DiscoveryOptions {
  language?: string | null;
}
export class TTS {
  private client: Connection | null = null;
  private streams = new Set<AudioStream>();
  private discoveryOperations = new Map<AbortController, Promise<string[]>>();
  readonly voices: { list: (options?: VoiceListOptions) => Promise<string[]> };
  readonly languages: {
    list: (options?: DiscoveryOptions) => Promise<string[]>;
  };
  constructor(
    private readonly credentials: Credentials,
    private readonly deployment: Readonly<typeof policy>,
    private readonly defaultTimeout: number | null,
    private readonly checkOpen: () => void,
  ) {
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
  private async prepare(signal: AbortSignal) {
    this.checkOpen();
    const auth = await this.credentials.metadata(signal);
    this.checkOpen();
    this.client ??= transport.makeClient(this.deployment.target);
    await connect(this.client, signal);
    return { client: this.client, metadata: metadata(auth) };
  }
  /** Synthesize complete text and return an audio stream. Do not await this call. */
  synthesize(text: string, options: SynthesisOptions = {}) {
    this.checkOpen();
    if (typeof text !== "string")
      throw new RimeInputError(
        "text must be a string; use stream() for an async text source",
      );
    if (!text.trim())
      throw new RimeInputError("Text must contain non-whitespace characters");
    return this.stream(
      (async function* () {
        yield text;
      })(),
      options,
    );
  }
  /** Await complete audio in memory, finalizing WAV lengths. Never returns partial audio.
   * The fixed output limit is 64 MiB (67,108,864 bytes); exceeding it raises
   * RimeResourceLimitError. Use streaming synthesis for larger outputs or timestamps.
   */
  async synthesizeToBuffer(
    text: string,
    options: SynthesisOptions = {},
  ): Promise<Buffer> {
    const audio = this.synthesize(text, options);
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for await (const chunk of audio) {
        if (chunk.byteLength > maxBufferBytes - size)
          throw new RimeResourceLimitError(
            "Audio exceeds the 64 MiB buffer limit",
            audio.requestId,
          );
        chunks.push(chunk);
        size += chunk.byteLength;
      }
      const data = Buffer.concat(chunks, size);
      if (audio.format === AudioFormat.WAV)
        finalizeWav(data, audio.sampleRate, audio.requestId);
      return data;
    } finally {
      await audio.cancel();
    }
  }
  /** Stream text chunks into synthesis and return an audio stream. Do not await this call. */
  stream(text: TextSource, options: SynthesisOptions = {}) {
    this.checkOpen();
    if (
      typeof text === "string" ||
      !text ||
      typeof text[Symbol.asyncIterator] !== "function"
    )
      throw new RimeInputError(
        "text must be an async iterable of strings; use synthesize() for a string",
      );
    const lexicon = snapshot(options.customLexicon);
    const timestamps =
      options.timestamps === undefined ? false : options.timestamps;
    if (typeof timestamps !== "boolean")
      throw new RimeInputError("timestamps must be a boolean");
    if (timestamps && this.deployment.model !== "mistv3")
      throw new RimeInputError(
        "Word timestamps are supported only with model='mistv3'",
      );
    const voice =
      options.voice === undefined || options.voice === null
        ? this.deployment.defaultVoice
        : nonempty(options.voice, "voice");
    const language = nonempty(options.language ?? "en", "language");
    const { format, sampleRate } = resolveAudio(
      options.audioFormat,
      options.sampleRate,
      options.timeScaleFactor,
    );
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
      timestamps,
      lexicon,
      sampleRate,
      options.timeScaleFactor ?? undefined,
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
    const timer = setTimeout(
      () =>
        controller.abort(new RimeTimeoutError("Discovery deadline expired")),
      budget * 1000,
    );
    const operation = (async () => {
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
        this.discoveryOperations.delete(controller);
      }
    })();
    this.discoveryOperations.set(controller, operation);
    return operation;
  }
  async close(): Promise<void> {
    const discoveries = [...this.discoveryOperations.values()];
    for (const controller of this.discoveryOperations.keys())
      controller.abort(
        new RimeCancelledError("Client closed during discovery"),
      );
    await Promise.all([
      ...[...this.streams].map((stream) => stream.cancel()),
      Promise.allSettled(discoveries),
    ]);
    this.client?.close();
  }
}
