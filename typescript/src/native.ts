/** Private native ABI. No service policy belongs in this module. */
import { createRequire } from "node:module";
import * as errors from "./errors.js";
export interface NativeStream {
  readonly requestId: string | null;
  readonly sourceChars: number;
  readonly cleanupTimeout: number;
  waitProduced(): Promise<void>;
  start(): void;
  cancel(): void;
  failSource(): void;
  waitStopped(): Promise<void>;
  inputRequest(): Promise<number | null>;
  inputReply(value: string): void;
  read(): Promise<{ data: Buffer | null; ticket: number }>;
  acceptRead(ticket: number): void;
}
export interface NativeClient {
  stream(options: string): NativeStream;
  discover(
    voices: boolean,
    language: string | null,
    timeout: number | null,
    inherit: boolean,
  ): Promise<string[]>;
  cancel(): void;
  close(): Promise<void>;
}
interface NativeModule {
  NativeClient: {
    new (config: string): NativeClient;
    testing(config: string, target: string, policy: string): NativeClient;
  };
  Converter: new (profile: string) => {
    process(data: Buffer, final: boolean): Buffer;
  };
  SentenceBuffer: new (limit: number) => {
    readonly retainedBytes: number;
    readonly scans: number;
    feed(text: string, final: boolean): string[];
  };
}
const require = createRequire(import.meta.url);
const libc = process.platform === "linux" ? "-gnu" : "";
export const native: NativeModule = (() => {
  const target = `${process.platform}-${process.arch}${libc}`;
  if (
    process.platform === "linux" &&
    !(
      process.report.getReport() as {
        header?: { glibcVersionRuntime?: string };
      }
    ).header?.glibcVersionRuntime
  )
    throw new Error(
      "@rimelabs/sdk requires glibc on Linux. Musl packages are not available.",
    );
  try {
    return require(`../native/rime-sdk.${target}.node`);
  } catch (cause) {
    throw new Error(
      `Cannot load @rimelabs/sdk native extension for ${target}. Install a complete release package, or build the extension for this source checkout.`,
      { cause },
    );
  }
})();
export const factory = {
  create: (config: string): NativeClient => new native.NativeClient(config),
};
export function translate(error: unknown, cause?: unknown): errors.RimeError {
  if (error instanceof errors.RimeError) return error;
  try {
    const value = JSON.parse((error as Error).message);
    const name = `Rime${value.kind}Error` as keyof typeof errors;
    const Constructor = errors[name] ?? errors.RimeStreamError;
    return new Constructor(value.message, value.request_id ?? null, { cause });
  } catch {
    return new errors.RimeStreamError("Native SDK operation failed", null, {
      cause: error,
    });
  }
}
export function call<T>(fn: () => T): T {
  try {
    return fn();
  } catch (error) {
    throw translate(error);
  }
}
