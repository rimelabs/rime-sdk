import * as grpc from "@grpc/grpc-js";
import {
  create,
  fromBinary,
  toBinary,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { policy, abortable } from "./policy.js";
import * as errors from "./errors.js";
export type Connection = grpc.Client;
export interface PreparedConnection {
  client: Connection;
  metadata: grpc.Metadata;
}
export type DiscoveryKind = "voices" | "languages";
export function rpcError(
  code: number,
  requestId: string | null = null,
): errors.RimeError {
  const classes: Record<number, typeof errors.RimeError> = {
    [grpc.status.UNAUTHENTICATED]: errors.RimeAuthenticationError,
    [grpc.status.PERMISSION_DENIED]: errors.RimePermissionError,
    [grpc.status.INVALID_ARGUMENT]: errors.RimeInputError,
    [grpc.status.RESOURCE_EXHAUSTED]: errors.RimeResourceLimitError,
    [grpc.status.UNAVAILABLE]: errors.RimeUnavailableError,
    [grpc.status.DEADLINE_EXCEEDED]: errors.RimeTimeoutError,
    [grpc.status.CANCELLED]: errors.RimeCancelledError,
  };
  const ErrorType = classes[code] ?? errors.RimeStreamError;
  return new ErrorType(
    `Rime operation failed: ${grpc.status[code] ?? "UNKNOWN"}`,
    requestId,
  );
}
export const transport = {
  makeClient: (target: string) =>
    new grpc.Client(target, grpc.credentials.createSsl(), {
      "grpc.enable_retries": 0,
      "grpc.max_receive_message_length": policy.receiveBytes,
      "grpc.max_send_message_length": policy.sentenceBytes + 65536,
    }),
};
function serializer<D extends DescMessage>(desc: D) {
  return (message: MessageShape<D>) => Buffer.from(toBinary(desc, message));
}
function deserializer<D extends DescMessage>(desc: D) {
  return (data: Buffer) => fromBinary(desc, data);
}
export async function connect(client: grpc.Client, signal: AbortSignal) {
  await abortable(
    new Promise<void>((resolve, reject) =>
      client.waitForReady(
        Date.now() + policy.connectionTimeout * 1000,
        (error) =>
          error
            ? reject(
                new errors.RimeTimeoutError(
                  "Connection establishment timed out",
                ),
              )
            : resolve(),
      ),
    ),
    signal,
  );
}
export function metadata(value: string) {
  const result = new grpc.Metadata();
  result.set("authorization", value);
  return result;
}
function openStream(client: grpc.Client, auth: grpc.Metadata) {
  return client.makeBidiStreamRequest(
    "/rime.TextToSpeech/SynthesizeStreaming",
    serializer(schema.StreamingSynthesisRequestSchema),
    deserializer(schema.SynthesisResponseStreamSchema),
    auth,
  );
}
function header(voice: string, language: string) {
  return create(schema.StreamingSynthesisRequestSchema, {
    payload: {
      case: "header",
      value: create(schema.SynthesisRequestSchema, {
        speaker: voice,
        language,
        audioParameters: { audioFormat: "audio/pcm", samplingRate: 24000 },
      }),
    },
  });
}
function textMessage(text: string) {
  return create(schema.StreamingSynthesisRequestSchema, {
    payload: { case: "textChunk", value: text },
  });
}

function requestId(metadata?: grpc.Metadata): string | null {
  return metadata?.get("x-request-id")[0]?.toString() ?? null;
}

function nativeError(error: unknown, id: string | null): errors.RimeError {
  if (error instanceof errors.RimeError) return error;
  if (
    error &&
    typeof error === "object" &&
    "code" in error &&
    typeof error.code === "number"
  ) {
    const metadata = "metadata" in error ? error.metadata : null;
    return rpcError(
      error.code,
      id ?? (metadata instanceof grpc.Metadata ? requestId(metadata) : null),
    );
  }
  return new errors.RimeStreamError("Rime transport failed", id, {
    cause: error,
  });
}

async function unary<I extends DescMessage, O extends DescMessage>(
  prepared: PreparedConnection,
  name: string,
  input: I,
  output: O,
  request: MessageShape<I>,
  signal: AbortSignal,
): Promise<MessageShape<O>> {
  let call: grpc.ClientUnaryCall | undefined;
  let id: string | null = null;
  try {
    signal.throwIfAborted();
    const operation = new Promise<MessageShape<O>>((resolve, reject) => {
      call = prepared.client.makeUnaryRequest(
        "/rime.TextToSpeech/" + name,
        serializer(input),
        deserializer(output),
        request,
        prepared.metadata,
        (error, result) => (error ? reject(error) : resolve(result!)),
      );
      call.once("metadata", (metadata: grpc.Metadata) => {
        id = requestId(metadata);
      });
    });
    return await abortable(operation, signal);
  } catch (error) {
    call?.cancel();
    if (signal.aborted) {
      if (signal.reason instanceof errors.RimeTimeoutError)
        throw new errors.RimeTimeoutError(signal.reason.message, id, {
          cause: signal.reason.cause,
        });
      throw signal.reason;
    }
    throw nativeError(error, id);
  }
}

export async function discover(
  prepared: PreparedConnection,
  kind: DiscoveryKind,
  language: string | null | undefined,
  signal: AbortSignal,
): Promise<string[]> {
  // Exactly one attempt. The client owns retries and the shared deadline.
  if (kind === "voices") {
    const method = schema.TextToSpeech.method.getSupportedSpeakers;
    const response = await unary(
      prepared,
      method.name,
      method.input,
      method.output,
      create(method.input, { language: language ?? undefined }),
      signal,
    );
    return response.speakers;
  }
  const method = schema.TextToSpeech.method.getSupportedLanguages;
  const response = await unary(
    prepared,
    method.name,
    method.input,
    method.output,
    create(method.input),
    signal,
  );
  return response.languages;
}

export class SynthesisCall {
  private readonly call: ReturnType<typeof openStream>;
  private readonly metadataPromise: Promise<grpc.Metadata>;
  private readonly statusPromise: Promise<grpc.StatusObject>;
  private status: grpc.StatusObject | null = null;
  private id: string | null = null;

  constructor(
    prepared: PreparedConnection,
    private signal: AbortSignal,
  ) {
    this.call = openStream(prepared.client, prepared.metadata);
    this.metadataPromise = new Promise((resolve, reject) => {
      this.call.once("metadata", (metadata: grpc.Metadata) => {
        this.id = requestId(metadata);
        resolve(metadata);
      });
      // A trailers-only response has no metadata event.
      this.call.once("status", () => resolve(new grpc.Metadata()));
      this.call.once("error", reject);
    });
    this.metadataPromise.catch(() => {});
    this.statusPromise = new Promise((resolve) => {
      this.call.once("status", (status: grpc.StatusObject) => {
        this.status = status;
        this.id ??= requestId(status.metadata);
        resolve(status);
      });
    });
    this.call.on("error", () => {});
  }

  get requestId(): string | null {
    return this.id;
  }
  get done(): boolean {
    return this.status !== null || this.call.destroyed;
  }
  cancel(): void {
    this.call.cancel();
  }

  private error(error: unknown): errors.RimeError {
    const failure = nativeError(error, this.id);
    this.id ??= failure.requestId;
    return failure;
  }

  private async writeMessage(
    message: schema.StreamingSynthesisRequest,
  ): Promise<void> {
    try {
      this.signal.throwIfAborted();
      if (this.status) {
        if (this.status.code !== grpc.status.OK)
          throw rpcError(this.status.code, this.id);
        throw new errors.RimeStreamError(
          "The service completed before input finished",
          this.id,
        );
      }
      await abortable(
        new Promise<void>((resolve, reject) => {
          this.call.write(message, (error: Error | null | undefined) =>
            error ? reject(error) : resolve(),
          );
        }),
        this.signal,
      );
    } catch (error) {
      if (this.signal.aborted) throw this.signal.reason;
      if (error instanceof errors.RimeError) throw error;
      // A native write error can precede the final status event. Preserve the
      // server rejection even if the reader has not consumed a response.
      const status = await abortable(this.statusPromise, this.signal);
      if (status.code !== grpc.status.OK) throw rpcError(status.code, this.id);
      throw this.error(error);
    }
  }

  async start(voice: string, language: string): Promise<void> {
    // Writes do not depend on response metadata.
    await this.writeMessage(header(voice, language));
  }
  async write(sentence: string): Promise<void> {
    await this.writeMessage(textMessage(sentence));
  }
  finishInput(): void {
    try {
      this.call.end();
    } catch (error) {
      throw this.error(error);
    }
  }
  private checkFormat(contentType: unknown): void {
    if (contentType !== "audio/pcm")
      throw new errors.RimeAudioFormatError(
        "Expected raw audio/pcm from the service",
        this.id,
      );
  }

  async *audio(): AsyncGenerator<Uint8Array> {
    try {
      const metadata = await abortable(this.metadataPromise, this.signal);
      const contentType = metadata.get("x-rime-audio-content-type")[0];
      for await (const response of this.call) {
        if (response.audio.length) this.checkFormat(contentType);
        yield response.audio;
      }
      const status = await abortable(this.statusPromise, this.signal);
      if (status.code !== grpc.status.OK) throw rpcError(status.code, this.id);
      // Validate empty success only after giving a rejection its proper error.
      this.checkFormat(contentType);
    } catch (error) {
      if (this.signal.aborted) throw this.signal.reason;
      throw this.error(error);
    }
  }
}
