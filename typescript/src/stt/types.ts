import type { PCMFormat } from "../pcm.js";

/** Formatting intent, for example "flight 247" or "flight two four seven". */
export type TranscriptionMode = "written" | "verbatim";
/** One caller-ended utterance of headerless signed PCM16 little-endian audio. */
export type AudioSource = AsyncIterable<Uint8Array>;

export interface TranscriptionOptions {
  /** Required BCP-47 tag, passed unchanged to the service (for example "en"). */
  language: string;
  /** Formatting intent; defaults to written and does not guarantee exact tokens. */
  mode?: TranscriptionMode;
  /** Recognition hints passed unchanged and in order, for example ["Super-G"]. */
  contextTerms?: readonly string[];
  /** Defaults to PCM16 little-endian mono 16 kHz; fixed for the utterance. */
  inputFormat?: PCMFormat;
  /** Optional overall deadline in seconds, including source and consumer pauses. */
  timeout?: number | null;
  /** Cancels only this utterance, including a pending connection. */
  signal?: AbortSignal;
}

export interface TranscriptionPartial {
  /** A revisable replacement snapshot. */
  readonly kind: "partial";
  /** Complete current text; replace previous text instead of appending it. */
  readonly text: string;
}

export interface TranscriptionFinal {
  /** Successful protocol and transport completion. */
  readonly kind: "final";
  /** Complete final text; silence can produce an empty string. */
  readonly text: string;
  /** Canonical BCP-47 tag selected by the service, for example "en". */
  readonly language: string;
}

export type TranscriptionUpdate = TranscriptionPartial | TranscriptionFinal;
