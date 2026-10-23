import { RimeInputError, RimeResourceLimitError } from "../errors.js";
import type { JsonValue, PrismFiller, ToolDefinition } from "./types.js";
import type { SessionSettings } from "./protocol.js";

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
