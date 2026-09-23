import { AudioFormat } from "./audio.js";
import { call, translate, type NativeStream } from "./native.js";
import { RimeInputError } from "./errors.js";
export type TextSource = string | AsyncIterable<string>;
export const constructionKey = Symbol("private stream constructor");
interface StreamOwner {
  forget(stream: AudioStream): void;
}
async function nextOrStopped(
  iterator: AsyncIterator<string>,
  signal: AbortSignal,
): Promise<IteratorResult<string> | null> {
  if (signal.aborted) return null;
  let onStop!: () => void;
  const stopped = new Promise<null>((resolve) => {
    onStop = () => resolve(null);
    signal.addEventListener("abort", onStop, { once: true });
  });
  try {
    return await Promise.race([iterator.next(), stopped]);
  } finally {
    signal.removeEventListener("abort", onStop);
  }
}
async function cleanup(task: Promise<unknown>, timeout: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      task,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeout * 1000);
      }),
    ]);
  } catch {
    /* Source cleanup must not replace the terminal result. */
  } finally {
    clearTimeout(timer);
  }
}
export class AudioStream implements AsyncIterableIterator<Uint8Array> {
  private pump: Promise<void> | null = null;
  private finished = false;
  private reading = false;
  private cause: unknown;
  constructor(
    key: symbol,
    private owner: StreamOwner,
    private native: NativeStream,
    private source: TextSource | null,
    private formatValue: AudioFormat,
  ) {
    if (key !== constructionKey)
      throw new TypeError("AudioStream is returned by client.tts.stream()");
  }
  get format() {
    return this.formatValue;
  }
  get requestId(): string | null {
    return this.native.requestId ?? null;
  }
  private start() {
    if (!this.pump) {
      call(() => this.native.start());
      this.pump = this.produce();
      void this.native.waitStopped().then(() => this.cleanup());
    }
  }
  private async produce(): Promise<void> {
    let iterator: AsyncIterator<string> | undefined;
    const stop = new AbortController();
    void this.native.waitStopped().then(() => stop.abort());
    try {
      const source = this.source!;
      iterator = (
        typeof source === "string"
          ? (async function* () {
              yield source;
            })()
          : source
      )[Symbol.asyncIterator]();
      let item = "",
        offset = 0;
      for (;;) {
        const request = await this.native.inputRequest();
        if (request == null) break;
        if (request === 0) {
          const result = await nextOrStopped(iterator, stop.signal);
          if (result === null) break;
          if (result.done) {
            this.native.inputReply('{"kind":"end"}');
            break;
          }
          if (typeof result.value !== "string")
            throw new RimeInputError("The text source must yield strings");
          item = result.value;
          offset = 0;
          this.native.inputReply('{"kind":"item"}');
        } else {
          const end = Math.min(item.length, offset + this.native.sourceChars);
          const units: number[] = [];
          for (; offset < end; offset++) units.push(item.charCodeAt(offset));
          this.native.inputReply(
            JSON.stringify({
              kind: "utf16",
              units,
              last: offset === item.length,
            }),
          );
          if (offset === item.length) item = "";
        }
      }
    } catch (error) {
      this.cause = error;
      this.native.failSource();
    } finally {
      if (iterator?.return)
        await cleanup(
          Promise.resolve().then(() => iterator!.return!()),
          this.native.cleanupTimeout,
        );
      this.source = null;
    }
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  async next(): Promise<IteratorResult<Uint8Array>> {
    if (this.finished) return { done: true, value: undefined };
    if (this.reading)
      throw new RimeInputError(
        "AudioStream permits only one concurrent reader",
      );
    this.reading = true;
    try {
      this.start();
      const result = await this.native.read();
      if (result.data == null) await this.cleanup();
      this.native.acceptRead(result.ticket);
      if (result.data == null) {
        this.finished = true;
        return { done: true, value: undefined };
      }
      return { done: false, value: result.data };
    } catch (error) {
      await this.cleanup();
      throw translate(error, this.cause);
    } finally {
      this.reading = false;
    }
  }
  private async cleanup() {
    if (this.pump) await cleanup(this.pump, this.native.cleanupTimeout);
    this.owner.forget(this);
    this.source = null;
  }
  async cancel(): Promise<void> {
    this.native.cancel();
    await this.cleanup();
  }
  async return(): Promise<IteratorResult<Uint8Array>> {
    await this.cancel();
    return { done: true, value: undefined };
  }
  async [Symbol.asyncDispose]() {
    await this.cancel();
  }
}
