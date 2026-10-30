/** Optional connection diagnostics. These fields do not initiate retries. */
export interface RimeErrorOptions extends ErrorOptions {
  /** Refused HTTP upgrade status, such as 503; null when unavailable. */
  statusCode?: number | null;
  /** Received WebSocket close code, such as 1013; null when unavailable. */
  closeCode?: number | null;
  /** Server-requested delay in seconds. Zero is valid; null means unavailable. */
  retryAfterSeconds?: number | null;
}

export class RimeError extends Error {
  readonly requestId: string | null;
  readonly statusCode: number | null;
  readonly closeCode: number | null;
  readonly retryAfterSeconds: number | null;
  constructor(
    message: string,
    requestId: string | null = null,
    options?: RimeErrorOptions,
  ) {
    super(message, options);
    this.name = new.target.name;
    this.requestId = requestId;
    this.statusCode = options?.statusCode ?? null;
    this.closeCode = options?.closeCode ?? null;
    this.retryAfterSeconds = options?.retryAfterSeconds ?? null;
  }
}
export class RimeAuthenticationError extends RimeError {}
export class RimePermissionError extends RimeError {}
export class RimeInputError extends RimeError {}
export class RimeResourceLimitError extends RimeError {}
export class RimeUnavailableError extends RimeError {}
export class RimeTimeoutError extends RimeError {}
export class RimeAudioFormatError extends RimeError {}
export class RimeCancelledError extends RimeError {}
export class RimeStreamError extends RimeError {}
