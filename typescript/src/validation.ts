import { RimeInputError } from "./errors.js";

export function endpointAddress(endpoint: string): {
  target: string;
  audience: string;
} {
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
