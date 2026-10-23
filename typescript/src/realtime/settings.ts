import { RimeInputError } from "../errors.js";
import type { JsonValue, PrismFiller, ToolDefinition } from "./types.js";
import type { SessionSettings } from "./protocol.js";

/** Reject values that JSON.stringify would silently drop or change. */
function copyJson(value: unknown): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return Array.from(value, copyJson);
  if (
    value &&
    typeof value === "object" &&
    [Object.prototype, null].includes(Object.getPrototypeOf(value))
  )
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, copyJson(item)]),
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
      !tool.name.trim() ||
      names.has(tool.name)
    )
      throw new RimeInputError("Tool names must be nonblank and unique");
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
    } catch {
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
        description: tool.description ?? "",
        parameters,
      },
    };
  });
}
