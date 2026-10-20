import type * as grpc from "@grpc/grpc-js";
import { Credentials } from "../auth.js";
import { connect, metadata } from "../grpc.js";
import { timeout } from "../validation.js";
import {
  RimeInputError,
  RimeAudioFormatError,
  RimeResourceLimitError,
  RimeCancelledError,
  RimeTimeoutError,
} from "../errors.js";
import { scope } from "../cancellation.js";
import { resolvePCMFormat } from "../pcm.js";
import {
  transport,
  requireSchema,
  transcribe,
  MAX_WAV_BYTES,
} from "./transport.js";
import { TranscriptStream } from "./stream.js";
import { policy } from "./policy.js";
import type {
  AudioSource,
  TranscriptionOptions,
  TranscribeOptions,
  Transcription,
} from "./types.js";

export class STT {
  private connection: grpc.Client | null = null;
  private streams = new Set<TranscriptStream>();
  private readonly cancellation = new AbortController();
  private readonly requests = new Set<Promise<Transcription>>();
  constructor(
    private readonly credentials: Credentials,
    private readonly checkOpen: () => void,
  ) {}
  /** Transcribe complete WAV bytes (at most 16 MiB), without replay or retries. */
  transcribe(
    audio: Uint8Array,
    options: TranscribeOptions,
  ): Promise<Transcription> {
    this.checkOpen();
    if (!(audio instanceof Uint8Array) || audio.byteLength === 0)
      throw new RimeAudioFormatError("audio must contain complete WAV bytes");
    if (audio.byteLength > MAX_WAV_BYTES)
      throw new RimeResourceLimitError("WAV input exceeds 16 MiB");
    validateOptions(options);
    const settings = {
      ...options,
      contextTerms: [...(options.contextTerms ?? [])],
    };
    const data = Buffer.from(audio);
    const deadline = scope(
      [options.signal, this.cancellation.signal],
      timeout(options.timeout) ?? undefined,
      new RimeTimeoutError("Overall transcription deadline expired"),
    );
    const operation = (async () => {
      try {
        const prepared = await this.prepare(deadline.signal, policy.target);
        return await transcribe(
          prepared.client,
          prepared.metadata,
          data,
          settings,
          deadline.signal,
        );
      } catch (error) {
        if (
          deadline.signal.aborted &&
          !(error instanceof RimeTimeoutError) &&
          !(error instanceof RimeCancelledError)
        ) {
          if (deadline.signal.reason instanceof RimeTimeoutError)
            throw deadline.signal.reason;
          throw new RimeCancelledError("Transcription cancelled");
        }
        throw error;
      } finally {
        deadline.dispose();
      }
    })();
    this.requests.add(operation);
    void operation.then(
      () => this.requests.delete(operation),
      () => this.requests.delete(operation),
    );
    return operation;
  }
  private async prepare(signal: AbortSignal, target: string) {
    this.checkOpen();
    requireSchema();
    signal.throwIfAborted();
    const headers = metadata(this.credentials.authorization());
    this.connection ??= transport.makeClient(target);
    await connect(this.connection, signal, policy.connectionTimeout);
    signal.throwIfAborted();
    return { client: this.connection, metadata: headers };
  }
  stream(source: AudioSource, options: TranscriptionOptions): TranscriptStream {
    this.checkOpen();
    if (!source || typeof source[Symbol.asyncIterator] !== "function")
      throw new RimeInputError(
        "audio must be an async iterable of Uint8Array chunks",
      );
    validateOptions(options);
    const outputContract = options.outputContract ?? "revised_hypotheses";
    if (
      outputContract !== "revised_hypotheses" &&
      outputContract !== "append_fragments"
    )
      throw new RimeInputError(
        "outputContract must be revised_hypotheses or append_fragments",
      );
    const mode = options.mode ?? "written";
    const terms = options.contextTerms ?? [];
    const format = resolvePCMFormat(options.inputFormat);
    const target = policy.target;
    const stream = new TranscriptStream(
      {
        checkOpen: this.checkOpen,
        forget: (stream) => this.streams.delete(stream),
        prepare: (signal) => this.prepare(signal, target),
      },
      source,
      {
        language: options.language,
        outputContract,
        mode,
        contextTerms: [...terms],
        inputFormat: format,
        timeout: timeout(options.timeout),
        signal: options.signal,
      },
    );
    this.streams.add(stream);
    return stream;
  }
  async close() {
    this.cancellation.abort(new RimeCancelledError("Client closed"));
    try {
      const results = await Promise.allSettled([
        ...[...this.streams].map((stream) => stream.cancel()),
        ...[...this.requests].map((request) => request.catch(() => {})),
      ]);
      for (const result of results)
        if (result.status === "rejected") throw result.reason;
    } finally {
      this.connection?.close();
    }
  }
}

function validateOptions(options: TranscribeOptions) {
  if (!options || typeof options.language !== "string")
    throw new RimeInputError("language must be a string");
  const mode = options.mode ?? "written";
  if (mode !== "written" && mode !== "verbatim")
    throw new RimeInputError("mode must be written or verbatim");
  const terms = options.contextTerms ?? [];
  if (!Array.isArray(terms) || terms.some((term) => typeof term !== "string"))
    throw new RimeInputError("contextTerms must be an array of strings");
}
