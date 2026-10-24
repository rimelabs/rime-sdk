import { RimeInputError, RimeStreamError } from "../errors.js";
import type { HistoryItem } from "./types.js";
function utf8(value: unknown): value is string {
  return typeof value === "string" && !/[\uD800-\uDFFF]/u.test(value);
}
export function itemSettings(item: HistoryItem): Record<string, string> {
  if (!item || typeof item !== "object")
    throw new RimeInputError("history item must be an object");
  const value: Record<string, string> = { type: item.type };
  if (item.id !== undefined) {
    if (!utf8(item.id) || [...item.id].length < 1 || [...item.id].length > 32)
      throw new RimeInputError("item.id must contain 1 to 32 characters");
    value.id = item.id;
  }
  switch (item.type) {
    case "message":
      if (
        !["user", "assistant", "system"].includes(item.role) ||
        !utf8(item.text) ||
        [...item.text].length > 4000
      )
        throw new RimeInputError("invalid history message");
      value.role = item.role;
      value.text = item.text;
      break;
    case "function_call":
      value.call_id = item.callId;
      value.name = item.name;
      value.arguments = item.arguments;
      if (!item.callId || !item.name)
        throw new RimeInputError("past calls require callId and name");
      break;
    case "function_call_output":
      value.call_id = item.callId;
      value.output = item.output;
      if (!item.callId) throw new RimeInputError("tool results require callId");
      break;
    default:
      throw new RimeInputError("invalid history item type");
  }
  if (!Object.values(value).every(utf8))
    throw new RimeInputError("history fields must be valid UTF-8 strings");
  return value;
}
export function historyId(value: string): string {
  if (!utf8(value) || !value)
    throw new RimeInputError("history ID must be nonempty valid UTF-8");
  return value;
}
export function listedItem(value: unknown): HistoryItem {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new RimeStreamError("invalid history item");
  const o = value as Record<string, unknown>;
  const field = (name: string): string => {
    if (typeof o[name] !== "string")
      throw new RimeStreamError("invalid history field " + name);
    if ((name === "call_id" || name === "name") && !o[name])
      throw new RimeStreamError("invalid history identity");
    return o[name] as string;
  };
  const id = field("id");
  if (!id) throw new RimeStreamError("invalid history item id");
  switch (o.type) {
    case "message": {
      const role = field("role");
      if (role !== "user" && role !== "assistant" && role !== "system")
        throw new RimeStreamError("invalid history role");
      return Object.freeze({ type: o.type, id, role, text: field("text") });
    }
    case "function_call":
      return Object.freeze({
        type: o.type,
        id,
        callId: field("call_id"),
        name: field("name"),
        arguments: field("arguments"),
      });
    case "function_call_output":
      return Object.freeze({
        type: o.type,
        id,
        callId: field("call_id"),
        output: field("output"),
      });
    default:
      throw new RimeStreamError("invalid history item type");
  }
}
