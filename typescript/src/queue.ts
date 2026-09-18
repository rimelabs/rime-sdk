// finish() drains queued audio; fail() discards it and keeps the first error.
// Subsequent puts discard their input. Await the last put before finish().
// AudioStream owns consumer-observed completion and checks for cancellation
// after awaiting get(), since cancellation can follow promise resolution.
export class ByteQueue {
  private byteCount = 0;
  private items: Buffer[] = [];
  private closed = false;
  private error: Error | null = null;
  private waiters = new Set<() => void>();
  private writers = 0;
  private readonly chunkSize: number;
  constructor(
    private readonly limit: number,
    chunkSize: number,
  ) {
    if (
      !Number.isSafeInteger(limit) ||
      !Number.isSafeInteger(chunkSize) ||
      limit <= 0 ||
      chunkSize <= 0
    )
      throw new RangeError("Audio queue limits must be positive integers");
    this.chunkSize = Math.min(chunkSize, limit);
  }
  get size(): number {
    return this.byteCount;
  }
  get hasPendingOutput(): boolean {
    return this.byteCount > 0 || this.writers > 0;
  }
  private wait() {
    return new Promise<void>((resolve) => this.waiters.add(resolve));
  }
  private wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }
  async put(data: Buffer) {
    this.writers++;
    try {
      for (let offset = 0; offset < data.length; offset += this.chunkSize) {
        const part = data.subarray(offset, offset + this.chunkSize);
        while (!this.closed && this.byteCount + part.length > this.limit)
          await this.wait();
        if (this.closed) return;
        this.items.push(part);
        this.byteCount += part.length;
        this.wake();
      }
    } finally {
      this.writers--;
    }
  }
  async get(): Promise<IteratorResult<Uint8Array>> {
    while (!this.closed && !this.items.length) await this.wait();
    if (this.error) throw this.error;
    const value = this.items.shift();
    if (value) {
      this.byteCount -= value.length;
      this.wake();
      return { done: false, value };
    }
    return { done: true, value: undefined };
  }
  finish() {
    this.closed = true;
    this.wake();
  }
  fail(error: Error) {
    this.error ??= error;
    this.items = [];
    this.byteCount = 0;
    this.finish();
  }
}
