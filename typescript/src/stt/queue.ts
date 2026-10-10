import type { TranscriptionUpdate } from "./types.js";

/** Bounded, atomic transcript events; failure wakes producers and consumers. */
export class TranscriptQueue {
  private items: TranscriptionUpdate[] = [];
  private closed = false;
  private error: Error | null = null;
  private waiters = new Set<() => void>();
  constructor(private readonly limit: number) {}
  private wait() {
    return new Promise<void>((resolve) => this.waiters.add(resolve));
  }
  private wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }
  async put(value: TranscriptionUpdate) {
    while (!this.closed && this.items.length >= this.limit) await this.wait();
    if (!this.closed) {
      this.items.push(value);
      this.wake();
    }
  }
  async get(): Promise<IteratorResult<TranscriptionUpdate>> {
    while (!this.closed && !this.items.length) await this.wait();
    if (this.error) throw this.error;
    const value = this.items.shift();
    this.wake();
    return value ? { done: false, value } : { done: true, value: undefined };
  }
  finish() {
    this.closed = true;
    this.wake();
  }
  fail(error: Error) {
    this.error ??= error;
    this.items = [];
    this.finish();
  }
}
