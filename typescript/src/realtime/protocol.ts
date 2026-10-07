// SPDX-License-Identifier: Apache-2.0
/** Decode only the protocol fields the SDK uses. Unknown events and fields stay open. */
import { RimeStreamError } from "../errors.js";
import * as t from "./types.js";

export interface SessionSettings {
  modalities: ("text" | "audio")[];
  input_audio_format: "pcm16";
  turn_detection: { interrupt_response: boolean };
  tools: {
    type: "function";
    function: { name: string; description: string; parameters: t.JsonObject };
  }[];
  voice?: string;
  instructions?: string;
}
export interface SessionView {
  id: string;
  voice: string;
  interruptOnSpeech: boolean | null;
  toolResultTimeoutS: number | null;
  toolContinuationTimeoutS: number | null;
}
export interface SessionCreated {
  kind: "session.created";
  session: SessionView;
}
export interface SessionUpdated {
  kind: "session.updated";
  session: SessionView;
}
export interface ItemCreated {
  kind: "item.created";
  itemId: string;
}
export interface ToolResultCreated {
  kind: "tool.result.created";
  callId: string;
}
export interface AudioCleared {
  kind: "audio.cleared";
}
export interface ItemTruncated {
  kind: "item.truncated";
  itemId: string;
  contentIndex: number;
}
export interface ResponseEnded extends t.ResponseEnded {
  drainToken: string | null;
}
export type Acknowledgment =
  | SessionUpdated
  | ItemCreated
  | ToolResultCreated
  | AudioCleared
  | ItemTruncated
  | t.ResponseStarted
  | ResponseEnded;
export type ServerPayload =
  | SessionCreated
  | Acknowledgment
  | t.InputEvent
  | t.MessageStarted
  | t.TextDelta
  | t.TextDone
  | t.AudioDelta
  | t.AudioDone
  | t.ToolCall
  | t.FaultEvent;
export interface ServerEvent {
  eventId: string;
  requestId: string | null;
  payload: ServerPayload;
}
export type RequestKind =
  | "session.update"
  | "conversation.item.create"
  | "response.create"
  | "response.cancel"
  | "input_audio_buffer.clear"
  | "conversation.item.truncate";

/** A request carries its result type and checks it before exposing the result. */
export class Request<T extends Acknowledgment> {
  constructor(
    readonly kind: RequestKind,
    private readonly accepts: (event: Acknowledgment) => event is T,
    readonly allowsItemCreated = false,
  ) {}
  result(event: Acknowledgment): T {
    if (!this.accepts(event))
      throw new RimeStreamError(
        `Unexpected ${event.kind} acknowledgment for ${this.kind}`,
      );
    return event;
  }
  check(event: Acknowledgment): void {
    this.result(event);
  }
}
export const requests = {
  update: new Request(
    "session.update",
    (e): e is SessionUpdated => e.kind === "session.updated",
  ),
  item: new Request(
    "conversation.item.create",
    (e): e is ItemCreated => e.kind === "item.created",
  ),
  toolResult: new Request(
    "conversation.item.create",
    (e): e is ToolResultCreated => e.kind === "tool.result.created",
  ),
  create: new Request(
    "response.create",
    (e): e is t.ResponseStarted => e.kind === "response.started",
  ),
  text: new Request(
    "response.create",
    (e): e is t.ResponseStarted => e.kind === "response.started",
    true,
  ),
  cancel: new Request(
    "response.cancel",
    (e): e is ResponseEnded => e.kind === "response.ended",
  ),
  clear: new Request(
    "input_audio_buffer.clear",
    (e): e is AudioCleared => e.kind === "audio.cleared",
  ),
  truncate: new Request(
    "conversation.item.truncate",
    (e): e is ItemTruncated => e.kind === "item.truncated",
  ),
};

type ObjectValue = Record<string, unknown>;
function invalid(field: string): never {
  throw new RimeStreamError(`Invalid realtime field: ${field}`);
}
function object(value: unknown, field: string): ObjectValue {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return invalid(field);
  return value as ObjectValue;
}
function string(value: unknown, field: string): string {
  if (typeof value !== "string") return invalid(field);
  return value;
}
function id(value: unknown, field: string): string {
  const result = string(value, field);
  if (!result) return invalid(field);
  return result;
}
function optionalString(value: unknown, field: string): string | null {
  return value == null ? null : string(value, field);
}
function integer(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    return invalid(field);
  return value;
}
function duration(value: unknown, field: string): number | null {
  if (value == null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
    return invalid(field);
  return value;
}
function session(value: unknown): SessionView {
  const view = object(value, "session");
  const waits =
    view.prsm_tool_waits === undefined
      ? {}
      : object(view.prsm_tool_waits, "session.prsm_tool_waits");
  if (
    view.prsm_tool_waits !== undefined &&
    (!("result_timeout_s" in waits) || !("continuation_timeout_s" in waits))
  )
    invalid("session.prsm_tool_waits");
  const interrupt = view.prsm_effective_interrupt_response;
  if (interrupt !== undefined && typeof interrupt !== "boolean")
    invalid("session.prsm_effective_interrupt_response");
  return {
    id: id(view.id, "session.id"),
    voice: view.voice === undefined ? "" : string(view.voice, "session.voice"),
    interruptOnSpeech: interrupt ?? null,
    toolResultTimeoutS: duration(waits.result_timeout_s, "result_timeout_s"),
    toolContinuationTimeoutS: duration(
      waits.continuation_timeout_s,
      "continuation_timeout_s",
    ),
  };
}
function fault(value: unknown): t.RealtimeFault {
  const error = object(value, "error");
  const owner = error.owner == null ? {} : object(error.owner, "error.owner");
  const scope =
    error.scope === undefined ? "unknown" : string(error.scope, "error.scope");
  const calls = owner.call_ids ?? [];
  if (!Array.isArray(calls)) return invalid("error.owner.call_ids");
  return Object.freeze({
    code:
      error.code === undefined ? "unknown" : string(error.code, "error.code"),
    message:
      error.message === undefined ? "" : string(error.message, "error.message"),
    scope:
      scope === "event" ||
      scope === "utterance" ||
      scope === "response" ||
      scope === "tool_roundtrip" ||
      scope === "session"
        ? scope
        : "unknown",
    requestId: optionalString(owner.event_id, "error.owner.event_id"),
    responseId: optionalString(owner.response_id, "error.owner.response_id"),
    itemId: optionalString(owner.item_id, "error.owner.item_id"),
    callIds: Object.freeze(
      calls.map((value) => id(value, "error.owner.call_ids")),
    ),
    correlationId: optionalString(error.correlation_id, "error.correlation_id"),
    parameter: optionalString(error.param, "error.param"),
  });
}
function json(value: unknown): t.JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(json);
  const entries = object(value, "arguments");
  return Object.fromEntries(
    Object.entries(entries).map(([key, value]) => [key, json(value)]),
  );
}
function argumentsObject(value: unknown): t.JsonObject {
  const result = json(JSON.parse(string(value, "arguments")));
  if (!result || typeof result !== "object" || Array.isArray(result))
    return invalid("arguments");
  return result;
}
function audio(value: unknown): t.AudioDelta["audio"] {
  const encoded = string(value, "delta");
  if (
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      encoded,
    )
  )
    return invalid("audio.delta");
  const data = Buffer.from(encoded, "base64");
  if (data.length % 2) return invalid("audio.delta");
  return Object.freeze({
    data,
    format: Object.freeze({
      sampleRate: 24000,
      channels: 1,
      encoding: "pcm_s16le",
    }),
  });
}

export function decode(
  raw: string,
  sessionId: string | null,
): ServerEvent | null {
  try {
    return decodeValue(JSON.parse(raw), sessionId);
  } catch (error) {
    if (error instanceof RimeStreamError) throw error;
    throw new RimeStreamError("Invalid realtime event");
  }
}
function decodeValue(
  value: unknown,
  sessionId: string | null,
): ServerEvent | null {
  const event = object(value, "event"),
    kind = string(event.type, "type");
  const ref = (value: unknown): t.ResponseRef => {
    if (sessionId === null)
      throw new RimeStreamError("Event arrived before session.created");
    return Object.freeze({ sessionId, responseId: id(value, "response.id") });
  };
  let payload: ServerPayload;
  switch (kind) {
    case "session.created":
      payload = { kind: "session.created", session: session(event.session) };
      break;
    case "session.updated":
      payload = { kind: "session.updated", session: session(event.session) };
      break;
    case "conversation.item.created": {
      const item = object(event.item, "item");
      if (item.type === "function_call_output")
        payload = {
          kind: "tool.result.created",
          callId: id(item.call_id, "item.call_id"),
        };
      else {
        if (item.type !== undefined && item.type !== "message")
          invalid("item.type");
        payload = { kind: "item.created", itemId: id(item.id, "item.id") };
      }
      break;
    }
    case "input_audio_buffer.cleared":
      payload = { kind: "audio.cleared" };
      break;
    case "conversation.item.truncated":
      payload = {
        kind: "item.truncated",
        itemId: id(event.item_id, "item_id"),
        contentIndex: integer(event.content_index, "content_index"),
      };
      break;
    case "prsm.typed_input.ready":
      payload = { kind: "input.ready" };
      break;
    case "input_audio_buffer.speech_started":
      payload = {
        kind: "speech.started",
        itemId: id(event.item_id, "item_id"),
        audioStartMs: integer(event.audio_start_ms, "audio_start_ms"),
      };
      break;
    case "input_audio_buffer.speech_stopped":
      payload = {
        kind: "speech.stopped",
        itemId: id(event.item_id, "item_id"),
        audioEndMs: integer(event.audio_end_ms, "audio_end_ms"),
      };
      break;
    case "input_audio_buffer.committed":
      payload = {
        kind: "input.committed",
        itemId: id(event.item_id, "item_id"),
      };
      break;
    case "conversation.item.input_audio_transcription.delta":
      payload = {
        kind: "transcript.delta",
        itemId: id(event.item_id, "item_id"),
        delta: string(event.delta, "delta"),
      };
      break;
    case "conversation.item.input_audio_transcription.completed":
      payload = {
        kind: "transcript.final",
        itemId: id(event.item_id, "item_id"),
        text: string(event.transcript, "transcript"),
      };
      break;
    case "conversation.item.input_audio_transcription.failed": {
      const error = object(event.error, "error"),
        itemId = id(event.item_id, "item_id");
      payload = {
        kind: "transcript.failed",
        itemId,
        error: fault({
          code: string(error.code, "error.code"),
          message: string(error.message, "error.message"),
          scope: "utterance",
          owner: { item_id: itemId },
        }),
      };
      break;
    }
    case "response.created": {
      const response = object(event.response, "response"),
        metadata =
          response.metadata == null
            ? {}
            : object(response.metadata, "metadata");
      const cause =
        metadata.prsm_cause === undefined
          ? undefined
          : string(metadata.prsm_cause, "prsm_cause");
      const parent = optionalString(
        metadata.prsm_parent_response_id,
        "prsm_parent_response_id",
      );
      payload = {
        kind: "response.started",
        response: ref(response.id),
        cause:
          cause === undefined
            ? "speech"
            : cause === "user_text"
              ? "text"
              : cause === "tool_continuation"
                ? "tools"
                : cause === "proactive"
                  ? "proactive"
                  : "unknown",
        parent: parent === null ? null : ref(parent),
        inputItemId: optionalString(
          metadata.prsm_input_item_id,
          "prsm_input_item_id",
        ),
      };
      break;
    }
    case "response.done": {
      const response = object(event.response, "response"),
        metadata =
          response.metadata == null
            ? {}
            : object(response.metadata, "metadata");
      const details =
        response.status_details == null
          ? {}
          : object(response.status_details, "status_details");
      const status = string(response.status, "response.status");
      payload = {
        kind: "response.ended",
        response: ref(response.id),
        status:
          status === "completed" ||
          status === "cancelled" ||
          status === "failed"
            ? status
            : "unknown",
        reason: optionalString(details.reason, "reason"),
        drainToken: optionalString(
          metadata.prsm_drain_token,
          "prsm_drain_token",
        ),
      };
      break;
    }
    case "response.function_call_arguments.done":
      payload = {
        kind: "tool.call",
        call: Object.freeze({
          ...ref(event.response_id),
          callId: id(event.call_id, "call_id"),
        }),
        itemId: id(event.item_id, "item_id"),
        name: string(event.name, "name"),
        arguments: argumentsObject(event.arguments),
      };
      break;
    case "response.output_item.added":
    case "response.text.delta":
    case "response.text.done":
    case "response.audio.delta":
    case "response.audio.done": {
      const item =
        kind === "response.output_item.added"
          ? object(event.item, "item")
          : null;
      if (item && string(item.type, "item.type") !== "message") return null;
      const contentIndex =
        kind === "response.output_item.added" &&
        event.content_index === undefined
          ? 0
          : integer(event.content_index, "content_index");
      if (contentIndex !== 0)
        throw new RimeStreamError(
          "Unsupported Prism contract: content_index must be zero",
        );
      const output = Object.freeze({
        response: ref(event.response_id),
        itemId: id(item ? item.id : event.item_id, "item_id"),
        outputIndex: integer(event.output_index, "output_index"),
        contentIndex,
      });
      if (kind === "response.output_item.added")
        payload = { kind: "message.started", output };
      else if (kind === "response.text.delta")
        payload = {
          kind: "text.delta",
          output,
          delta: string(event.delta, "delta"),
        };
      else if (kind === "response.text.done")
        payload = {
          kind: "text.done",
          output,
          text: string(event.text, "text"),
        };
      else if (kind === "response.audio.delta")
        payload = { kind: "audio.delta", output, audio: audio(event.delta) };
      else payload = { kind: "audio.done", output };
      break;
    }
    case "error":
      payload = { kind: "error", error: fault(event.error) };
      break;
    default:
      return null;
  }
  return {
    eventId:
      event.event_id === undefined ? "" : string(event.event_id, "event_id"),
    requestId: optionalString(
      event.prsm_request_event_id,
      "prsm_request_event_id",
    ),
    payload,
  };
}
