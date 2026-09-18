import { policy, abortable } from "./policy.js";
import {
  RimeError,
  RimeAuthenticationError,
  RimePermissionError,
  RimeTimeoutError,
  RimeCancelledError,
  RimeResourceLimitError,
  RimeUnavailableError,
} from "./errors.js";
export interface Token {
  value: string;
  expiresAt: number;
  audience: string;
}
export async function exchangeKey(
  key: string,
  signal: AbortSignal,
  configuration: Readonly<typeof policy> = policy,
): Promise<Token> {
  try {
    const response = await fetch(configuration.exchangeUrl, {
      method: "POST",
      redirect: "error",
      signal,
      headers: {
        Authorization: `Api-Key ${key}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ audience: configuration.audience }),
    });
    if (response.status !== 200) {
      // The refresh timeout ends when this function rejects. Cancel the body
      // now so an unfinished error response cannot retain the connection.
      await response.body?.cancel().catch(() => {});
      if (response.status === 403)
        throw new RimePermissionError("Credential exchange denied permission");
      if (response.status === 429)
        throw new RimeResourceLimitError(
          "Credential exchange rate limit exceeded",
        );
      if (response.status >= 500 && response.status < 600)
        throw new RimeUnavailableError(
          "Credential exchange service unavailable",
        );
      throw new RimeAuthenticationError(
        "Credential exchange rejected the API key",
      );
    }
    if (!response.body) throw new Error("Missing response body");
    const reader = response.body.getReader();
    let size = 0;
    const chunks: Uint8Array[] = [];
    try {
      for (;;) {
        const result = await reader.read();
        if (result.done) break;
        size += result.value.length;
        if (size > 65536) throw new Error("Response too large");
        chunks.push(result.value);
      }
    } finally {
      await reader.cancel();
      reader.releaseLock();
    }
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (
      typeof body.access_token !== "string" ||
      !body.access_token ||
      /[^\x21-\x7e]/.test(body.access_token) ||
      typeof body.expires_in !== "number" ||
      !Number.isFinite(body.expires_in) ||
      body.expires_in <= 0 ||
      body.audience !== configuration.audience
    )
      throw new RimeAuthenticationError(
        "Credential exchange returned an invalid token",
      );
    return {
      value: body.access_token,
      expiresAt: Date.now() / 1000 + body.expires_in,
      audience: body.audience,
    };
  } catch (error) {
    if (error instanceof RimeError) throw error;
    if (signal.aborted) throw signal.reason;
    throw new RimeAuthenticationError("Credential exchange failed");
  }
}
// Private dependency seam for local conformance tests; not exported by the package.
export const authentication = { exchangeKey };
export class Credentials {
  private token: Token | null = null;
  private refresh: Promise<Token> | null = null;
  private refreshAt = 0;
  private controller = new AbortController();
  private closed = false;
  constructor(
    private key: string,
    private readonly configuration: Readonly<typeof policy> = policy,
  ) {}
  async metadata(signal: AbortSignal): Promise<string> {
    // TEMPORARY until Themis is ready: replace this body with the call below.
    // return this.themisMetadata(signal);
    if (this.closed)
      throw new RimeAuthenticationError("Credentials are closed");
    signal.throwIfAborted();
    return `Bearer ${this.key}`;
  }
  async themisMetadata(signal: AbortSignal): Promise<string> {
    if (this.closed)
      throw new RimeAuthenticationError("Credentials are closed");
    if (!this.token || Date.now() / 1000 >= this.refreshAt) {
      if (!this.refresh) {
        const timeoutController = new AbortController();
        const timer = setTimeout(
          () =>
            timeoutController.abort(
              new RimeTimeoutError("Credential acquisition timed out"),
            ),
          this.configuration.authTimeout * 1000,
        );
        const combined = AbortSignal.any([
          this.controller.signal,
          timeoutController.signal,
        ]);
        this.refresh = abortable(
          authentication.exchangeKey(this.key, combined, this.configuration),
          combined,
        )
          .then((token) => {
            if (
              !token.value ||
              token.expiresAt <= Date.now() / 1000 ||
              token.audience !== this.configuration.audience
            )
              throw new RimeAuthenticationError(
                "Credential exchange returned an invalid token",
              );
            this.token = token;
            this.refreshAt =
              token.expiresAt -
              Math.min(30, (token.expiresAt - Date.now() / 1000) / 10);
            return token;
          })
          .finally(() => {
            clearTimeout(timer);
            this.refresh = null;
          });
        this.refresh.catch(() => {});
      }
      const token = await abortable(this.refresh, signal);
      return `Bearer ${token.value}`;
    }
    return `Bearer ${this.token.value}`;
  }
  async close() {
    this.closed = true;
    this.controller.abort(new RimeCancelledError("Client closed"));
    await this.refresh?.catch(() => {});
    this.token = null;
    this.key = "";
  }
}
