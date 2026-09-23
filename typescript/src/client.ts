import { AudioFormat } from "./audio.js";
import { AudioStream, constructionKey, type TextSource } from "./stream.js";
import { factory, call, translate, type NativeClient } from "./native.js";
import {
  RimeAuthenticationError,
  RimeInputError,
  RimeAudioFormatError,
} from "./errors.js";
export interface RimeOptions {
  apiKey?: string | null;
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
  private native: NativeClient;
  private closed = false;
  private streams = new Set<AudioStream>();
  private closing: Promise<void> | null = null;
  readonly tts: {
    stream: (text: TextSource, options?: SynthesisOptions) => AudioStream;
  };
  readonly voices: { list: (options?: VoiceListOptions) => Promise<string[]> };
  readonly languages: {
    list: (options?: DiscoveryOptions) => Promise<string[]>;
  };
  constructor(options: RimeOptions = {}) {
    const key = options.apiKey ?? process.env.RIME_API_KEY;
    if (typeof key !== "string" || !key.trim())
      throw new RimeAuthenticationError("Provide apiKey or set RIME_API_KEY");
    if (
      typeof options.timeout === "number" &&
      !Number.isFinite(options.timeout)
    )
      throw new RimeInputError("timeout must be finite");
    this.native = call(() =>
      factory.create(
        JSON.stringify({
          api_key: key,
          model: options.model ?? "coda",
          endpoint: options.endpoint,
          timeout: options.timeout,
        }),
      ),
    );
    this.tts = { stream: (text, options = {}) => this.stream(text, options) };
    this.voices = {
      list: (options = {}) =>
        this.discover(true, options.language, options.timeout),
    };
    this.languages = {
      list: (options = {}) => this.discover(false, null, options.timeout),
    };
  }
  private checkOpen() {
    if (this.closed) throw new RimeInputError("The Rime client is closed");
  }
  private stream(text: TextSource, options: SynthesisOptions): AudioStream {
    this.checkOpen();
    if (
      typeof text === "string"
        ? !text.trim()
        : !text || typeof text[Symbol.asyncIterator] !== "function"
    )
      throw new RimeInputError(
        "text must be a non-empty string or an async iterable of strings",
      );
    const format = options.audioFormat ?? AudioFormat.PCM_24000;
    if (format !== AudioFormat.PCM_24000 && format !== AudioFormat.MULAW_8000)
      throw new RimeAudioFormatError("Select a named AudioFormat profile");
    if (
      typeof options.timeout === "number" &&
      !Number.isFinite(options.timeout)
    )
      throw new RimeInputError("timeout must be finite");
    const native = call(() =>
      this.native.stream(
        JSON.stringify({
          voice: options.voice ?? "clementine",
          language: options.language ?? "en",
          profile:
            format === AudioFormat.PCM_24000 ? "PCM_24000" : "MULAW_8000",
          timeout: options.timeout,
          inherit_timeout: options.timeout === undefined,
        }),
      ),
    );
    const stream = new AudioStream(
      constructionKey,
      {
        forget: (s) => this.streams.delete(s),
      },
      native,
      text,
      format,
    );
    this.streams.add(stream);
    return stream;
  }
  private async discover(
    voices: boolean,
    language: string | null | undefined,
    timeout: number | null | undefined,
  ): Promise<string[]> {
    this.checkOpen();
    if (language != null && (typeof language !== "string" || !language.trim()))
      throw new RimeInputError("language must be a non-empty string");
    if (
      timeout != null &&
      (typeof timeout !== "number" || !Number.isFinite(timeout) || timeout <= 0)
    )
      throw new RimeInputError(
        "timeout must be a positive finite number or null",
      );
    try {
      return await this.native.discover(
        voices,
        language ?? null,
        timeout ?? null,
        timeout === undefined,
      );
    } catch (error) {
      throw translate(error);
    }
  }
  async close(): Promise<void> {
    if (!this.closing) {
      this.closed = true;
      this.native.cancel();
      this.closing = (async () => {
        await Promise.all([...this.streams].map((s) => s.cancel()));
        await this.native.close();
      })();
    }
    await this.closing;
  }
  async [Symbol.asyncDispose]() {
    await this.close();
  }
}
