import type { PCMFormat } from "../pcm.js";
export type { PCMFormat } from "../pcm.js";
import { RimeError, RimeTimeoutError } from "../errors.js";

export type JsonValue =
  null | boolean | number | string | JsonValue[] | JsonObject;
export interface JsonObject {
  [key: string]: JsonValue;
}

/** All durations are positive, finite seconds. */
export interface RealtimeTimeouts {
  readonly connectS?: number;
  readonly readyS?: number;
  readonly requestS?: number;
}
export interface RealtimeOperationOptions {
  readonly signal?: AbortSignal;
}
export interface ToolDefinition {
  readonly name: string;
  readonly parameters: JsonObject;
  readonly description?: string;
}
export interface RealtimeConnectOptions extends RealtimeOperationOptions {
  readonly endpoint: string;
  readonly model?: "prism";
  readonly voice?: string | null;
  readonly instructions?: string | null;
  readonly tools?: readonly ToolDefinition[];
  readonly interruptOnSpeech?: boolean;
  readonly timeouts?: RealtimeTimeouts;
}
export interface ReplyOptions extends RealtimeOperationOptions {
  readonly instruction?: string | null;
  readonly toolCall?: ToolCallRef | null;
}
export interface AudioChunk {
  readonly data: Uint8Array;
  readonly format?: PCMFormat;
}
export interface ResponseRef {
  readonly sessionId: string;
  readonly responseId: string;
}
export interface ItemRef {
  readonly sessionId: string;
  readonly itemId: string;
}
export interface ToolCallRef extends ResponseRef {
  readonly callId: string;
}
export interface OutputRef {
  readonly response: ResponseRef;
  readonly itemId: string;
  readonly outputIndex: number;
  readonly contentIndex: number;
}
export interface SessionInfo {
  readonly sessionId: string;
  readonly model: "prism";
  readonly voice: string;
  readonly interruptOnSpeech: boolean;
  readonly toolResultTimeoutS: number | null;
  readonly toolContinuationTimeoutS: number | null;
}
export interface RealtimeFault {
  readonly code: string;
  readonly message: string;
  readonly scope:
    | "event"
    | "utterance"
    | "response"
    | "tool_roundtrip"
    | "session"
    | "unknown";
  readonly requestId: string | null;
  readonly responseId: string | null;
  readonly itemId: string | null;
  readonly callIds: readonly string[];
  readonly correlationId: string | null;
  readonly parameter: string | null;
}
export class RealtimeAdmissionTimeout extends RimeTimeoutError {}
export class RimeRealtimeError extends RimeError {
  constructor(readonly fault: RealtimeFault) {
    super(`${fault.code}: ${fault.message}`, fault.requestId);
  }
}
export interface InputReady {
  readonly kind: "input.ready";
}
export interface SpeechStarted {
  readonly kind: "speech.started";
  readonly itemId: string;
  readonly audioStartMs: number;
}
export interface SpeechStopped {
  readonly kind: "speech.stopped";
  readonly itemId: string;
  readonly audioEndMs: number;
}
export interface InputCommitted {
  readonly kind: "input.committed";
  readonly itemId: string;
}
export interface TranscriptDelta {
  readonly kind: "transcript.delta";
  readonly itemId: string;
  readonly delta: string;
}
export interface TranscriptFinal {
  readonly kind: "transcript.final";
  readonly itemId: string;
  readonly text: string;
}
export interface TranscriptFailed {
  readonly kind: "transcript.failed";
  readonly itemId: string;
  readonly error: RealtimeFault;
}
export interface ResponseStarted {
  readonly kind: "response.started";
  readonly response: ResponseRef;
  readonly cause: "speech" | "text" | "proactive" | "tools" | "unknown";
  readonly parent: ResponseRef | null;
  readonly inputItemId: string | null;
}
export interface MessageStarted {
  readonly kind: "message.started";
  readonly output: OutputRef;
}
export interface TextDelta {
  readonly kind: "text.delta";
  readonly output: OutputRef;
  readonly delta: string;
}
export interface TextDone {
  readonly kind: "text.done";
  readonly output: OutputRef;
  readonly text: string;
}
export interface AudioDelta {
  readonly kind: "audio.delta";
  readonly output: OutputRef;
  readonly audio: {
    readonly data: Uint8Array;
    readonly format: Required<PCMFormat>;
  };
}
export interface AudioDone {
  readonly kind: "audio.done";
  readonly output: OutputRef;
}
export interface ToolCall {
  readonly kind: "tool.call";
  readonly call: ToolCallRef;
  readonly itemId: string;
  readonly name: string;
  readonly arguments: JsonObject;
}
export interface ResponseAbandoned {
  readonly kind: "response.abandoned";
  readonly response: ResponseRef;
}
export interface ResponseEnded {
  readonly kind: "response.ended";
  readonly response: ResponseRef;
  readonly status: "completed" | "cancelled" | "failed" | "unknown";
  readonly reason: string | null;
}
export interface FaultEvent {
  readonly kind: "error";
  readonly error: RealtimeFault;
}
export type InputEvent =
  | InputReady
  | SpeechStarted
  | SpeechStopped
  | InputCommitted
  | TranscriptDelta
  | TranscriptFinal
  | TranscriptFailed;
export type ResponseEvent =
  | ResponseStarted
  | ResponseAbandoned
  | MessageStarted
  | TextDelta
  | TextDone
  | AudioDelta
  | AudioDone
  | ToolCall
  | ResponseEnded;
export interface SessionEvent {
  readonly sessionId: string;
  readonly eventId: string;
  readonly requestId: string | null;
  readonly payload: InputEvent | ResponseEvent | FaultEvent;
}
export interface PlaybackInterrupted {
  readonly kind: "interrupted";
  readonly output: OutputRef;
  readonly audioEndMs: number;
}
export interface PlaybackFinished {
  readonly kind: "finished";
  readonly response: ResponseRef;
  readonly playedMs?: number | null;
  readonly audibleTailMs?: number | null;
}
export type PlaybackReport = PlaybackInterrupted | PlaybackFinished;
