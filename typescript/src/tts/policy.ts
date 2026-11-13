import { endpointAddress } from "../validation.js";
export { timeout } from "../validation.js";
import { RimeInputError } from "../errors.js";
const codaHostname = "coda.api.rime.ai";
const mistHostname = "mist.api.rime.ai";
export const hostedTargets: ReadonlySet<string> = new Set(
  [codaHostname, mistHostname].map((hostname) => `${hostname}:443`),
);
export const policy = {
  model: "coda",
  target: `${codaHostname}:443`,
  audience: codaHostname,
  defaultVoice: "clementine",
  exchangeUrl: "https://themis.api.rime.ai/v1/token",
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
  enableKeepalive: false,
};
export function resolve(
  model: string,
  endpoint?: string | null,
  enableKeepalive = false,
): Readonly<typeof policy> {
  if (typeof enableKeepalive !== "boolean")
    throw new RimeInputError("enableTTSKeepalive must be a boolean");
  const deployment = { ...policy, model, enableKeepalive };
  if (model === "mistv3") {
    deployment.target = `${mistHostname}:443`;
    deployment.audience = mistHostname;
    deployment.defaultVoice = "astra";
  } else if (model !== "coda") {
    throw new RimeInputError("model must be 'coda' or 'mistv3'");
  }
  if (endpoint === undefined || endpoint === null) return deployment;
  return { ...deployment, ...endpointAddress(endpoint) };
}

export function nonempty(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.trim())
    throw new RimeInputError(`${name} must be a non-empty string`);
  return value;
}
