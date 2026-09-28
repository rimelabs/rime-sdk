import { RimeInputError } from "./errors.js";
const codaHostname = "coda.api.rime.ai";
export const policy = {
  target: `${codaHostname}:443`,
  audience: codaHostname,
  defaultVoice: "clementine",
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
export function resolve(
  model: string,
  endpoint?: string | null,
): Readonly<typeof policy> {
  const deployment = { ...policy };
  if (model === "mistv3") {
    const hostname = "mist.api.rime.ai";
    deployment.target = `${hostname}:443`;
    deployment.audience = hostname;
    deployment.defaultVoice = "astra";
  } else if (model !== "coda") {
    throw new RimeInputError("model must be 'coda' or 'mistv3'");
  }
  if (endpoint === undefined || endpoint === null) return deployment;
  const error =
    "endpoint must be a hostname with an optional port (1-65535), without a scheme or path";
  if (typeof endpoint !== "string" || /\s/.test(endpoint))
    throw new RimeInputError(error);
  const [host, port, extra] = endpoint.split(":");
  if (
    !host ||
    host.length > 253 ||
    extra !== undefined ||
    host
      .split(".")
      .some(
        (label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label),
      ) ||
    (port !== undefined &&
      (!/^[0-9]{1,5}$/.test(port) || Number(port) < 1 || Number(port) > 65535))
  )
    throw new RimeInputError(error);
  return {
    ...deployment,
    target: `${host.toLowerCase()}:${port === undefined ? 443 : Number(port)}`,
    audience: host.toLowerCase(),
  };
}
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
