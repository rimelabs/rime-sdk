import type { PCMFormat } from "../pcm.js";
export type { PCMFormat } from "../pcm.js";
import { RimeError, RimeTimeoutError } from "../errors.js";
import { normalizeFault } from "./faults.js";

export type JsonValue =
  null | boolean | number | string | JsonValue[] | JsonObject;
/** Your labels for replies, keys to strings, such as `{ step: "3" }`. Prism
 * takes at most 16, keys up to 64 characters and values up to 512, and
 * refuses keys starting with `prism_`. */
export type Labels = Readonly<Record<string, string>>;
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
/** Speech while a tool runs. Text and instructions must be nonblank and at
 * most 500 Unicode scalar values. Invalid UTF-8 is rejected.
 * Omit the setting to use the server default. */
export type PrismFiller =
  | {
      readonly type: "none";
      readonly text?: never;
      readonly instructions?: never;
    }
  | {
      readonly type: "text";
      readonly text: string;
      readonly instructions?: never;
    }
  | {
      readonly type: "auto";
      readonly instructions?: string;
      readonly text?: never;
    };

export interface ToolDefinition {
  readonly name: string;
  readonly parameters: JsonObject;
  readonly description?: string;
  readonly prismFiller?: PrismFiller;
}
export type TurnEagerness = "low" | "medium" | "high" | "auto";
/** Omitted fields use the server's turn timing defaults. */
export interface TurnDetection {
  /** How readily Prism decides that the user has finished speaking. */
  readonly eagerness?: TurnEagerness;
  /** Maximum silence before ending a turn: an integer from 1000 to 10000 ms. */
  readonly endOfTurnTimeoutMs?: number;
}
/** Effective server settings. Null means the server did not publish the field. */
export interface TurnDetectionInfo {
  readonly eagerness: TurnEagerness | "unknown" | null;
  readonly endOfTurnTimeoutMs: number | null;
}
export interface RealtimeConnectOptions extends RealtimeOperationOptions {
  /** Defaults to wss://prism.api.rime.ai/v1/realtime. */
  readonly endpoint?: string;
  readonly model?: "prism";
  readonly voice?: string | null;
  readonly instructions?: string | null;
  readonly tools?: readonly ToolDefinition[];
  readonly interruptOnSpeech?: boolean;
  readonly turnDetection?: TurnDetection;
  readonly timeouts?: RealtimeTimeouts;
  /** Sent as the upgrade's `x-request-id`. It must be nonblank printable ASCII, or
   * `connect` throws `RimeInputError`. Prism replaces IDs longer than 128 characters.
   * A gateway can replace shorter IDs too. `info.requestId` reports the ID Prism
   * actually used.
   * Omitted or `null` sends none. */
  readonly requestId?: string | null;
  /** Labels every reply's `response.started` carries in `metadata`, such as
   * `{ step: "3" }`; change them with `session.setResponseMetadata`. Prism
   * refuses keys starting with `prism_`. Omitted or `null` sets none. */
  readonly responseMetadata?: Labels | null;
}
/** Omitted fields keep their values. Empty instructions or tools clear them. */
export interface RealtimeUpdateOptions extends RealtimeOperationOptions {
  readonly instructions?: string;
  readonly tools?: readonly ToolDefinition[];
  readonly voice?: string;
  readonly interruptOnSpeech?: boolean;
}
export interface ReplyOptions extends RealtimeOperationOptions {
  readonly instruction?: string | null;
  readonly toolCall?: ToolCallRef | null;
}
export interface AudioChunk {
  readonly data: Uint8Array;
  readonly format?: PCMFormat;
}
/** An item without audio. IDs are optional on input and present in history results. */
export type HistoryItem =
  | {
      readonly type: "message";
      readonly id?: string;
      readonly role: "user" | "assistant" | "system";
      readonly text: string;
    }
  | {
      readonly type: "function_call";
      readonly id?: string;
      readonly callId: string;
      readonly name: string;
      readonly arguments: string;
    }
  | {
      readonly type: "function_call_output";
      readonly id?: string;
      readonly callId: string;
      readonly output: string;
    };
export interface HistoryCreateOptions extends RealtimeOperationOptions {
  /** Omit to append, root to insert first, or an item ID to follow its whole turn. */
  readonly previousItemId?: string;
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
  /** Settings revision published by the server; null when unavailable.
   * Optional on caller-created values for compatibility. */
  readonly settingsRevision?: number | null;
  readonly sessionId: string;
  /** The `x-request-id` Prism's logs name this session by. Null when the
   * server does not publish it. */
  readonly requestId: string | null;
  /** The labels replies carry, as Prism last confirmed them. Null when the
   * server does not publish them. */
  readonly responseMetadata: Labels | null;
  readonly model: "prism";
  readonly voice: string;
  readonly interruptOnSpeech: boolean;
  readonly toolResultTimeoutS: number | null;
  readonly toolContinuationTimeoutS: number | null;
  readonly turnDetection: TurnDetectionInfo;
}
export interface RealtimeFault {
  readonly code: string;
  /** Optional for compatibility with caller-created faults. The SDK derives
   * its output classification from the code and ignores supplied values. */
  readonly retryable?: boolean;
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
/** An immutable fault produced by the SDK. */
export interface NormalizedRealtimeFault extends RealtimeFault {
  /** Whether the same request, unchanged, may succeed on another attempt.
   * True only for capacity_exceeded, unavailable, and response_timeout.
   * Unknown codes return false. The application owns retry timing, limits,
   * and recovery after partial output; this value does not replay a request. */
  readonly retryable: boolean;
}
export class RealtimeAdmissionTimeout extends RimeTimeoutError {}
export class RimeRealtimeError extends RimeError {
  readonly fault: NormalizedRealtimeFault;
  constructor(fault: RealtimeFault) {
    const normalized = normalizeFault(fault);
    super(`${normalized.code}: ${normalized.message}`, normalized.requestId);
    this.fault = normalized;
  }
}
export interface InputReady {
  readonly kind: "input.ready";
}
/** The user may have started speaking over a playing reply. Prism sends it
 * before `speech.started` when it may interrupt the reply; it can be a false
 * alarm, so lower playback on it rather than stopping. */
export interface SpeechHint {
  readonly kind: "speech.hint";
  readonly audioStartMs: number;
}
export interface SpeechStarted {
  readonly kind: "speech.started";
  readonly itemId: string;
  readonly audioStartMs: number;
}
/** What ended the user's turn:
 * - `semantic`: Prism judged from the user's speech that they had finished;
 *   `eagerness` sets how readily it does.
 * - `timeout`: the user was silent for `endOfTurnTimeoutMs`, and the timeout
 *   ended the turn before Prism judged that they had finished.
 * - `silence`: Prism's backup turn detection ended the turn at a short pause;
 *   it does not apply `eagerness` or `endOfTurnTimeoutMs`.
 * - `max_duration`: the user spoke for the longest turn Prism takes; what
 *   they say next starts a new turn.
 * - `detector_changed`: Prism switched between its main and backup turn
 *   detection while the user spoke; nothing the user did ended the turn. */
export type SpeechEndReason =
  "semantic" | "timeout" | "silence" | "max_duration" | "detector_changed";
export interface SpeechStopped {
  readonly kind: "speech.stopped";
  readonly itemId: string;
  readonly audioEndMs: number;
  /** Null means the server did not publish the field. */
  readonly endReason: SpeechEndReason | "unknown" | null;
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
export interface TranscriptFailed<Fault extends RealtimeFault = RealtimeFault> {
  readonly kind: "transcript.failed";
  readonly itemId: string;
  readonly error: Fault;
}
export interface ResponseStarted {
  /** Settings revision published by the server; null when unavailable.
   * Optional on caller-created values for compatibility. */
  readonly settingsRevision?: number | null;
  readonly kind: "response.started";
  readonly response: ResponseRef;
  readonly cause: "speech" | "text" | "proactive" | "tools" | "unknown";
  readonly parent: ResponseRef | null;
  readonly inputItemId: string | null;
  /** The session's labels when the reply began. Prism's `prism_` keys are
   * left out. */
  readonly metadata: Labels;
  /** The user items the reply answers: a spoken reply's committed turns, or
   * a typed turn's item. Empty when the server names none. */
  readonly inputItemIds: readonly string[];
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
  /** Settings revision published by the server; null when unavailable.
   * Optional on caller-created values for compatibility. */
  readonly settingsRevision?: number | null;
  readonly kind: "response.ended";
  readonly response: ResponseRef;
  readonly status: "completed" | "cancelled" | "failed" | "unknown";
  readonly reason: string | null;
}
export interface FaultEvent<Fault extends RealtimeFault = RealtimeFault> {
  readonly kind: "error";
  readonly error: Fault;
}
export type InputEvent<Fault extends RealtimeFault = RealtimeFault> =
  | InputReady
  | SpeechHint
  | SpeechStarted
  | SpeechStopped
  | InputCommitted
  | TranscriptDelta
  | TranscriptFinal
  | TranscriptFailed<Fault>;
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
export interface SessionEvent<Fault extends RealtimeFault = RealtimeFault> {
  readonly sessionId: string;
  readonly eventId: string;
  readonly requestId: string | null;
  readonly payload: InputEvent<Fault> | ResponseEvent | FaultEvent<Fault>;
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
