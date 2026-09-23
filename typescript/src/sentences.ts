import { native, call } from "./native.js";
export const ready = Promise.resolve();
export class SentenceBuffer {
  private inner: InstanceType<typeof native.SentenceBuffer>;
  constructor(limit: number) {
    this.inner = new native.SentenceBuffer(limit);
  }
  get retainedBytes() {
    return this.inner.retainedBytes;
  }
  get scans() {
    return this.inner.scans;
  }
  feed(text: string, final = false): string[] {
    return call(() => this.inner.feed(text, final));
  }
}
