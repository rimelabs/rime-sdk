import { abortable } from "./cancellation.js";
import * as grpc from "@grpc/grpc-js";
import {
  fromBinary,
  toBinary,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";
import * as errors from "./errors.js";

export function rpcError(
  code: number,
  requestId: string | null = null,
  details?: string,
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
    details?.trim()
      ? details
      : `Rime operation failed: ${grpc.status[code] ?? "UNKNOWN"}`,
    requestId,
  );
}
export function serializer<D extends DescMessage>(desc: D) {
  return (message: MessageShape<D>) => Buffer.from(toBinary(desc, message));
}
export function deserializer<D extends DescMessage>(desc: D) {
  return (data: Buffer) => fromBinary(desc, data);
}
export function requestId(metadata?: grpc.Metadata): string | null {
  return metadata?.get("x-request-id")[0]?.toString() ?? null;
}

export function nativeError(
  error: unknown,
  id: string | null,
): errors.RimeError {
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
      "details" in error && typeof error.details === "string"
        ? error.details
        : undefined,
    );
  }
  return new errors.RimeStreamError("Rime transport failed", id, {
    cause: error,
  });
}

export class ResponseCall<Output> {
  private readonly metadataPromise: Promise<grpc.Metadata>;
  protected readonly statusPromise: Promise<grpc.StatusObject>;
  protected status: grpc.StatusObject | null = null;
  protected id: string | null = null;

  constructor(
    private readonly call: grpc.ClientReadableStream<Output>,
    protected signal: AbortSignal,
  ) {
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

  protected error(error: unknown): errors.RimeError {
    const failure = nativeError(error, this.id);
    this.id ??= failure.requestId;
    return failure;
  }

  async headers(): Promise<grpc.Metadata> {
    try {
      return await abortable(this.metadataPromise, this.signal);
    } catch (error) {
      if (this.signal.aborted) throw this.signal.reason;
      throw this.error(error);
    }
  }

  async *responses(): AsyncGenerator<Output> {
    try {
      await this.headers();
      const responses: AsyncIterable<Output> = this.call;
      for await (const response of responses) yield response;
      const status = await abortable(this.statusPromise, this.signal);
      if (status.code !== grpc.status.OK)
        throw rpcError(status.code, this.id, status.details);
    } catch (error) {
      if (this.signal.aborted) throw this.signal.reason;
      throw this.error(error);
    }
  }
}

export class BidiCall<Input, Output> extends ResponseCall<Output> {
  constructor(
    private readonly duplex: grpc.ClientDuplexStream<Input, Output>,
    signal: AbortSignal,
  ) {
    super(duplex, signal);
  }
  async write(message: Input): Promise<void> {
    try {
      this.signal.throwIfAborted();
      if (this.status) {
        if (this.status.code !== grpc.status.OK)
          throw rpcError(this.status.code, this.id, this.status.details);
        throw new errors.RimeStreamError(
          "The service completed before input finished",
          this.id,
        );
      }
      await abortable(
        new Promise<void>((resolve, reject) => {
          const finish = (error?: unknown) => {
            this.duplex.removeListener("status", onStatus);
            if (error) reject(error);
            else resolve();
          };
          // A rejected native write can emit final status without invoking its
          // callback (for example, when exceeding the send-message limit).
          const onStatus = (status: grpc.StatusObject) =>
            finish(
              status.code === grpc.status.OK
                ? new errors.RimeStreamError(
                    "The service completed before input finished",
                    this.id,
                  )
                : rpcError(status.code, this.id, status.details),
            );
          this.duplex.once("status", onStatus);
          try {
            this.duplex.write(message, finish);
          } catch (error) {
            finish(error);
          }
        }),
        this.signal,
      );
    } catch (error) {
      if (this.signal.aborted) throw this.signal.reason;
      if (error instanceof errors.RimeError) throw error;
      // A native write error can precede the final status event. Preserve the
      // server rejection even if the reader has not consumed a response.
      const status = await abortable(this.statusPromise, this.signal);
      if (status.code !== grpc.status.OK)
        throw rpcError(status.code, this.id, status.details);
      throw this.error(error);
    }
  }

  finishInput(): void {
    try {
      this.duplex.end();
    } catch (error) {
      throw this.error(error);
    }
  }
}

export async function connect(
  client: grpc.Client,
  signal: AbortSignal,
  seconds: number,
) {
  await abortable(
    new Promise<void>((resolve, reject) =>
      client.waitForReady(Date.now() + seconds * 1000, (error) =>
        error
          ? reject(
              new errors.RimeTimeoutError("Connection establishment timed out"),
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
