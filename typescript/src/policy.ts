import { RimeInputError } from "./errors.js";
export const policy = {
  target: "coda.api.rime.ai:443",
  audience: "coda.api.rime.ai",
  exchangeUrl: "https://themis.api.rime.ai/v1/token",
  sentenceBytes: 65536,
  sourceChars: 1024,
  outputBytes: 96000,
  outputChunkBytes: 9600,
  receiveBytes: 4194304,
  authTimeout: 10,
  connectionTimeout: 10,
  firstAudioTimeout: 30,
  progressTimeout: 60,
  discoveryTimeout: 10,
  cleanupTimeout: 2,
};
export function timeout(
  value: unknown,
  inherited: number | null = null,
): number | null {
  if (value === undefined) return inherited;
  if (
    value !== null &&
    (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
  )
    throw new RimeInputError("timeout must be finite positive seconds or null");
  return value as number | null;
}
export function nonempty(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.trim())
    throw new RimeInputError(`${name} must be a non-empty string`);
  return value;
}
export function abortable<T>(
  promise: PromiseLike<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const abort = () => {
      signal.removeEventListener("abort", abort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", abort, { once: true });
    Promise.resolve(promise).then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}
