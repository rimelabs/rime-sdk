import { STT } from "./stt/client.js";
import type { AudioSource, TranscriptionOptions } from "./stt/types.js";
import type { TranscriptStream } from "./stt/stream.js";
import { Credentials } from "./auth.js";
import { RimeAuthenticationError, RimeInputError } from "./errors.js";
import {
  TTS,
  type SynthesisOptions,
  type DiscoveryOptions,
  type VoiceListOptions,
} from "./tts/client.js";
import { resolve, timeout } from "./tts/policy.js";
import { type AudioStream, type TextSource } from "./tts/stream.js";
import { Realtime } from "./realtime/client.js";
import type { RealtimeConnectOptions } from "./realtime/types.js";
import type { RealtimeSession } from "./realtime/session.js";

export type {
  SynthesisOptions,
  DiscoveryOptions,
  VoiceListOptions,
} from "./tts/client.js";

export interface RimeOptions {
  apiKey?: string | null;
  /** Select "coda" (default) or "mistv3" for speech and discovery. */
  model?: string;
  endpoint?: string | null;
  timeout?: number | null;
  /** Recognition hostname and optional port; defaults to stt.api.rime.ai:50051. */
  sttEndpoint?: string | null;
}

export class Rime {
  private readonly credentials: Credentials;
  private readonly ttsClient: TTS;
  private readonly sttClient: STT;
  readonly stt: {
    stream: (
      audio: AudioSource,
      options: TranscriptionOptions,
    ) => TranscriptStream;
  };
  private readonly realtimeClient: Realtime;
  readonly realtime: {
    connect: (options: RealtimeConnectOptions) => Promise<RealtimeSession>;
  };
  private closed = false;
  private closing: Promise<void> | null = null;
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
    const defaultTimeout = timeout(options.timeout);
    const deployment = resolve(options.model ?? "coda", options.endpoint);
    this.credentials = new Credentials(key, deployment);
    this.sttClient = new STT(this.credentials, options.sttEndpoint, () =>
      this.checkOpen(),
    );
    this.stt = {
      stream: (audio, options) => this.sttClient.stream(audio, options),
    };
    this.realtimeClient = new Realtime(this.credentials, () =>
      this.checkOpen(),
    );
    this.realtime = {
      connect: (options) => this.realtimeClient.connect(options),
    };
    this.ttsClient = new TTS(this.credentials, deployment, defaultTimeout, () =>
      this.checkOpen(),
    );
    this.tts = {
      stream: (text, options) => this.ttsClient.stream(text, options),
    };
    this.voices = this.ttsClient.voices;
    this.languages = this.ttsClient.languages;
  }

  private checkOpen() {
    if (this.closed) throw new RimeInputError("The Rime client is closed");
  }

  async close(): Promise<void> {
    if (!this.closing) {
      this.closed = true;
      this.closing = (async () => {
        try {
          const results = await Promise.allSettled([
            this.ttsClient.close(),
            this.sttClient.close(),
            this.realtimeClient.close(),
          ]);
          for (const result of results)
            if (result.status === "rejected") throw result.reason;
        } finally {
          await this.credentials.close();
        }
      })();
    }
    await this.closing;
  }

  async [Symbol.asyncDispose]() {
    await this.close();
  }
}
