export { scope } from "../cancellation.js";
import { abortable } from "../cancellation.js";
import { RimeCancelledError } from "../errors.js";

export class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;
  reject!: (error: unknown) => void;
  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
    // Session failures can arrive before a caller starts waiting.
    this.promise.catch(() => {});
  }
}
export class Flag {
  private waiters = new Set<() => void>();
  isSet = false;
  set() {
    this.isSet = true;
    for (const wake of this.waiters) wake();
  }
  clear() {
    this.isSet = false;
  }
  wait(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(signal.reason);
    if (this.isSet) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        this.waiters.delete(wake);
        signal.removeEventListener("abort", abort);
      };
      const wake = () => {
        cleanup();
        resolve();
      };
      const abort = () => {
        cleanup();
        reject(signal.reason);
      };
      this.waiters.add(wake);
      signal.addEventListener("abort", abort, { once: true });
    });
  }
}
export class Mutex {
  private locked = false;
  private waiters = new Set<() => void>();
  async acquire(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    while (this.locked) {
      const wake = new Deferred<void>();
      const notify = () => wake.resolve();
      this.waiters.add(notify);
      try {
        await abortable(wake.promise, signal);
      } finally {
        this.waiters.delete(notify);
      }
      signal.throwIfAborted();
    }
    this.locked = true;
    return () => {
      this.locked = false;
      for (const wake of this.waiters) wake();
    };
  }
}
export function cancellation(signal?: AbortSignal) {
  const controller = new AbortController();
  const abort = () =>
    controller.abort(new RimeCancelledError("Realtime operation cancelled"));
  if (signal?.aborted) abort();
  else signal?.addEventListener("abort", abort, { once: true });
  return {
    signal: controller.signal,
    dispose: () => signal?.removeEventListener("abort", abort),
  };
}
