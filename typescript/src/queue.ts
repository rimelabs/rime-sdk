export class ByteQueue {
  size = 0;
  private items: Buffer[] = [];
  private done = false;
  private error: Error | null = null;
  private waiters = new Set<() => void>();
  constructor(readonly limit: number) {}
  private wait() {
    return new Promise<void>((resolve) => this.waiters.add(resolve));
  }
  private wake() {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }
  async put(data: Buffer) {
    while (!this.done && this.size + data.length > this.limit)
      await this.wait();
    if (this.done) return;
    this.items.push(data);
    this.size += data.length;
    this.wake();
  }
  async get(): Promise<IteratorResult<Uint8Array>> {
    while (!this.done && !this.items.length) await this.wait();
    if (this.error) throw this.error;
    const value = this.items.shift();
    if (value) {
      this.size -= value.length;
      this.wake();
      return { done: false, value };
    }
    return { done: true, value: undefined };
  }
  finish(error: Error | null = null) {
    this.done = true;
    this.error = error;
    if (error) {
      this.items = [];
      this.size = 0;
    }
    this.wake();
  }
}
