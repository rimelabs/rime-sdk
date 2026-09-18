export class RimeError extends Error {
  readonly requestId: string | null;
  constructor(
    message: string,
    requestId: string | null = null,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = new.target.name;
    this.requestId = requestId;
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
