import type * as grpc from "@grpc/grpc-js";
import { Credentials } from "../auth.js";
import { connect, metadata } from "../grpc.js";
import { endpointAddress, timeout } from "../validation.js";
import { RimeInputError } from "../errors.js";
import { resolvePCMFormat } from "../pcm.js";
import { transport, requireSchema } from "./transport.js";
import { TranscriptStream } from "./stream.js";
import { policy } from "./policy.js";
import type { AudioSource, TranscriptionOptions } from "./types.js";

export class STT {
  private connection: grpc.Client | null = null;
  private streams = new Set<TranscriptStream>();
  constructor(
    private readonly credentials: Credentials,
    private readonly endpoint: string | null | undefined,
    private readonly checkOpen: () => void,
  ) {}
  stream(source: AudioSource, options: TranscriptionOptions): TranscriptStream {
    this.checkOpen();
    if (!source || typeof source[Symbol.asyncIterator] !== "function")
      throw new RimeInputError(
        "audio must be an async iterable of Uint8Array chunks",
      );
    if (!options || typeof options.language !== "string")
      throw new RimeInputError("language must be a string");
    const mode = options.mode ?? "written";
    if (mode !== "written" && mode !== "verbatim")
      throw new RimeInputError("mode must be written or verbatim");
    const terms = options.contextTerms ?? [];
    if (!Array.isArray(terms) || terms.some((term) => typeof term !== "string"))
      throw new RimeInputError("contextTerms must be an array of strings");
    const format = resolvePCMFormat(options.inputFormat);
    const target =
      this.endpoint == null
        ? policy.target
        : endpointAddress(this.endpoint).target;
    const stream = new TranscriptStream(
      {
        checkOpen: this.checkOpen,
        forget: (stream) => this.streams.delete(stream),
        prepare: async (signal) => {
          this.checkOpen();
          requireSchema();
          signal.throwIfAborted();
          const headers = metadata(this.credentials.authorization());
          this.connection ??= transport.makeClient(target);
          await connect(this.connection, signal, policy.connectionTimeout);
          signal.throwIfAborted();
          return {
            client: this.connection,
            metadata: headers,
          };
        },
      },
      source,
      {
        language: options.language,
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
    try {
      const results = await Promise.allSettled(
        [...this.streams].map((stream) => stream.cancel()),
      );
      for (const result of results)
        if (result.status === "rejected") throw result.reason;
    } finally {
      this.connection?.close();
    }
  }
}
