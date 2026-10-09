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

/** A disposable deadline avoids retaining timers and caller abort listeners. */
export function scope(
  signals: (AbortSignal | undefined)[],
  seconds?: number,
  error?: Error,
) {
  const controller = new AbortController();
  const handlers = new Map<AbortSignal, () => void>();
  for (const signal of new Set(signals)) {
    if (!signal) continue;
    const abort = () => controller.abort(signal.reason);
    if (signal.aborted) abort();
    else {
      signal.addEventListener("abort", abort, { once: true });
      handlers.set(signal, abort);
    }
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  if (seconds !== undefined) {
    const end = performance.now() + seconds * 1000;
    const expire = () => {
      const remaining = end - performance.now();
      if (remaining <= 0) controller.abort(error);
      else timer = setTimeout(expire, Math.min(remaining, 2147483647));
    };
    expire();
  }
  return {
    signal: controller.signal,
    dispose() {
      clearTimeout(timer);
      for (const [signal, handler] of handlers)
        signal.removeEventListener("abort", handler);
    },
  };
}
