import type { NormalizedRealtimeFault, RealtimeFault } from "./types.js";

// Reuse SDK-normalized faults so events and rejected requests share one object.
const normalizedFaults = new WeakSet<NormalizedRealtimeFault>();

export function normalizeFault(fault: RealtimeFault): NormalizedRealtimeFault {
  if (normalizedFaults.has(fault as NormalizedRealtimeFault))
    return fault as NormalizedRealtimeFault;
  const snapshot = {
    code: fault.code,
    message: fault.message,
    scope: fault.scope,
    requestId: fault.requestId,
    responseId: fault.responseId,
    itemId: fault.itemId,
    callIds: Object.freeze([...(fault.callIds ?? [])]),
    correlationId: fault.correlationId,
    parameter: fault.parameter,
  };
  const normalized = Object.freeze({
    ...snapshot,
    retryable:
      snapshot.code === "capacity_exceeded" ||
      snapshot.code === "unavailable" ||
      snapshot.code === "response_timeout",
  });
  normalizedFaults.add(normalized);
  return normalized;
}
