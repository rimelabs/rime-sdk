import { RimeInputError, RimeResourceLimitError } from "../errors.js";
import type {
  JsonValue,
  PrismFiller,
  ToolDefinition,
  TurnDetection,
  TurnDetectionUpdate,
  Review,
  ReviewDecision,
} from "./types.js";
import type { SessionSettings } from "./protocol.js";

export function speechSpeed(value: number): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0.85 ||
    value > 1.3
  )
    throw new RimeInputError("speed must be a finite number from 0.85 to 1.3");
  return value;
}

export function turnSettings(
  value: TurnDetection | TurnDetectionUpdate,
  initial = false,
): Partial<SessionSettings["turn_detection"]> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new RimeInputError("turnDetection must be an object");
  const result: Partial<SessionSettings["turn_detection"]> = {};
  for (const [name, wire, min, max] of [
    ["interruptMinWords", "interrupt_min_words", 1, 10],
    ["endOfTurnMinSilenceMs", "end_of_turn_min_silence_ms", 0, 5000],
    ["endOfTurnTimeoutMs", "prism_end_of_turn_timeout_ms", 1000, 10000],
  ] as const) {
    const n = (value as TurnDetection)[name];
    if (n === undefined) continue;
    if (!initial && name !== "interruptMinWords")
      throw new RimeInputError(`${name} is fixed at connection setup`);
    if (!Number.isInteger(n) || n < min || n > max)
      throw new RimeInputError(
        `${name} must be an integer from ${min} to ${max}`,
      );
    result[wire] = n;
  }
  const eagerness = (value as TurnDetection).eagerness;
  if (eagerness !== undefined) {
    if (!initial)
      throw new RimeInputError("eagerness is fixed at connection setup");
    if (!["low", "medium", "high", "auto"].includes(eagerness))
      throw new RimeInputError(
        "Turn eagerness must be low, medium, high, or auto",
      );
    result.eagerness = eagerness;
  }
  for (const [name, wire] of [
    ["interruptPhrases", "interrupt_phrases"],
    ["acknowledgmentPhrases", "acknowledgment_phrases"],
  ] as const) {
    const phrases = value[name];
    if (phrases === undefined) continue;
    if (
      !Array.isArray(phrases) ||
      phrases.length > 100 ||
      Array.from(phrases).some((item) => typeof item !== "string")
    )
      throw new RimeInputError(`${name} must contain at most 100 strings`);
    result[wire] = phrases.map(toolString);
  }
  return result;
}

export function reviewSettings(
  value: Review,
): NonNullable<SessionSettings["prism_review"]> {
  if (
    !value ||
    typeof value !== "object" ||
    typeof value.callback !== "function"
  )
    throw new RimeInputError("review requires an async callback");
  const timeout = value.timeoutMs === undefined ? 1000 : value.timeoutMs;
  if (!Number.isInteger(timeout) || timeout < 100 || timeout > 5000)
    throw new RimeInputError(
      "review.timeoutMs must be an integer from 100 to 5000",
    );
  const policy =
    value.onTimeout === undefined
      ? { action: "stop" as const }
      : value.onTimeout;
  if (
    !policy ||
    typeof policy !== "object" ||
    !["stop", "speak", "say"].includes(policy.action)
  )
    throw new RimeInputError("review.onTimeout must be stop, speak, or say");
  if (policy.action === "say") {
    if (
      typeof policy.text !== "string" ||
      !/\P{White_Space}/u.test(policy.text) ||
      [...policy.text].length > 500
    )
      throw new RimeInputError(
        "review timeout text must be nonblank and at most 500 characters",
      );
    return {
      timeout_ms: timeout,
      on_timeout: "say",
      timeout_line: toolString(policy.text),
    };
  }
  if ("text" in policy && policy.text !== undefined)
    throw new RimeInputError("Only say accepts timeout text");
  return { timeout_ms: timeout, on_timeout: policy.action };
}

export function reviewDecision(value: ReviewDecision): ReviewDecision {
  if (
    !value ||
    typeof value !== "object" ||
    !["allow", "replace", "stop"].includes(value.action)
  )
    throw new RimeInputError(
      "Review callback must return allow, replace, or stop",
    );
  if (value.action === "replace") {
    if (typeof value.text !== "string" || !/\P{White_Space}/u.test(value.text))
      throw new RimeInputError("Review replacement must be nonblank text");
    return { action: "replace", text: toolString(value.text) };
  }
  if (value.text !== undefined)
    throw new RimeInputError("Only replace accepts review text");
  return { action: value.action };
}

function toolString(value: string): string {
  // Unicode mode matches lone surrogates without rejecting valid UTF-16 pairs.
  if (/[\uD800-\uDFFF]/u.test(value))
    throw new RimeInputError("Tool strings must be valid UTF-8");
  return value;
}

/** Reject values that JSON.stringify would silently drop or change. */
function copyJson(value: unknown): JsonValue {
  if (typeof value === "string") return toolString(value);
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return Array.from(value, copyJson);
  if (
    value &&
    typeof value === "object" &&
    [Object.prototype, null].includes(Object.getPrototypeOf(value))
  )
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        toolString(key),
        copyJson(item),
      ]),
    );
  throw new RimeInputError("Tool schemas must contain finite JSON values");
}

function fillerSettings(value: PrismFiller): PrismFiller {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new RimeInputError("prismFiller must be a filler object");
  const kind = value.type;
  if (!["none", "text", "auto"].includes(kind))
    throw new RimeInputError("Prism filler type must be none, text, or auto");
  const field =
    kind === "text" ? "text" : kind === "auto" ? "instructions" : null;
  if (
    Object.entries(value).some(
      ([key, item]) => item !== undefined && key !== "type" && key !== field,
    )
  )
    throw new RimeInputError(
      "Prism filler has fields not allowed for its type",
    );
  const text = field === null ? undefined : value[field];
  if (kind === "text" || text !== undefined) {
    // Rust str::trim uses Unicode White_Space, which differs from JS trim.
    if (
      typeof text !== "string" ||
      !/\P{White_Space}/u.test(text) ||
      [...text].length > 500
    )
      throw new RimeInputError(
        "Prism filler text must be nonblank and at most 500 characters",
      );
    // Unicode mode matches lone surrogates but preserves valid UTF-16 pairs.
    if (/[\uD800-\uDFFF]/u.test(text))
      throw new RimeInputError("Prism filler text must be valid UTF-8");
  }
  if (kind === "text") return { type: "text", text: text as string };
  if (kind === "auto")
    return text === undefined
      ? { type: "auto" }
      : { type: "auto", instructions: text };
  return { type: "none" };
}

export function toolSettings(
  tools: readonly ToolDefinition[],
): SessionSettings["tools"] {
  if (!Array.isArray(tools)) throw new RimeInputError("tools must be an array");
  const names = new Set<string>();
  return tools.map((tool) => {
    if (
      !tool ||
      typeof tool !== "object" ||
      typeof tool.name !== "string" ||
      !/\P{White_Space}/u.test(tool.name) ||
      [...tool.name].length > 4000 ||
      names.has(tool.name)
    )
      throw new RimeInputError(
        "Tool names must be nonblank, unique, and at most 4000 characters",
      );
    toolString(tool.name);
    names.add(tool.name);
    if (
      !tool.parameters ||
      Array.isArray(tool.parameters) ||
      typeof tool.parameters !== "object" ||
      (tool.description !== undefined && typeof tool.description !== "string")
    )
      throw new RimeInputError(
        "Tool parameters must be a JSON object and description must be a string",
      );
    let parameters: JsonValue;
    try {
      parameters = copyJson(tool.parameters);
    } catch (error) {
      if (error instanceof RimeInputError) throw error;
      throw new RimeInputError("Tool schemas must contain finite JSON values");
    }
    if (
      parameters === null ||
      typeof parameters !== "object" ||
      Array.isArray(parameters)
    )
      throw new RimeInputError("Tool parameters must be an object");
    return {
      type: "function",
      ...(tool.prismFiller === undefined
        ? {}
        : { prism_filler: fillerSettings(tool.prismFiller) }),
      function: {
        name: tool.name,
        description: toolString(tool.description ?? ""),
        parameters,
      },
    };
  });
}

/** Bounded policy changes used to resolve each reply's settings revision. */
export class InterruptionPolicies {
  private changes: {
    revision: number;
    interrupt: boolean;
    unknown?: boolean;
  }[] = [];
  private current = true;
  private lastRevision: number | null = null;
  record(revision: number | null, interrupt: boolean): void {
    const previous = this.changes.at(-1);
    if (!previous) {
      this.changes.push({ revision: revision ?? 0, interrupt });
    } else if (revision === null) {
      if (interrupt !== this.current && !previous.unknown) {
        // Earlier published revisions stay known; later ones are ambiguous
        // until an acknowledgment supplies the new policy's revision.
        this.changes.push({
          revision: this.lastRevision === null ? 0 : this.lastRevision + 1,
          interrupt,
          unknown: true,
        });
      }
    } else if (previous.unknown || interrupt !== this.current) {
      this.changes.push({ revision, interrupt });
    }
    if (revision !== null) this.lastRevision = revision;
    if (this.changes.length > 128) this.changes.shift();
    this.current = interrupt;
  }
  resolve(revision: number | null | undefined): boolean {
    if (revision == null || this.changes.length === 0) return this.current;
    for (let i = this.changes.length - 1; i >= 0; i--) {
      if (this.changes[i].revision <= revision) {
        if (this.changes[i].unknown)
          throw new RimeResourceLimitError(
            "Reply settings revision crosses an unreported interruption policy change",
          );
        return this.changes[i].interrupt;
      }
    }
    throw new RimeResourceLimitError(
      "Reply settings revision is no longer retained",
    );
  }
}
