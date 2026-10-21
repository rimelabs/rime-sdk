import * as schema from "@rimelabs/api";
import { RimeStreamError, RimeResourceLimitError } from "../errors.js";
import type {
  TranscriptionPartial,
  TranscriptionDelta,
  TranscriptionFinal,
  TranscriptionOutputContract,
} from "./types.js";

/** Validate the wire sequence before exposing any final result. */
export class TranscriptState {
  language: string | null = null;
  private revision = 0n;
  private text = "";
  private final: TranscriptionFinal | null = null;
  constructor(
    private readonly textLimit: number,
    private readonly outputContract: TranscriptionOutputContract = "revised_hypotheses",
  ) {}
  accept(
    message: schema.StreamingTranscriptionResponse,
    inputDone: boolean,
  ): TranscriptionPartial | TranscriptionDelta | null {
    const payload = message.payload;
    if (payload.case === undefined) return null;
    if (this.final)
      throw new RimeStreamError(
        "Received a message after transcription completion",
      );
    if (payload.case === "accepted") {
      const accepted = payload.value;
      if (this.language !== null)
        throw new RimeStreamError(
          "Received duplicate transcription acceptance",
        );
      if (
        accepted.outputContract !==
        (this.outputContract === "append_fragments"
          ? schema.StreamingOutputContract.APPEND_FRAGMENTS
          : schema.StreamingOutputContract.REVISED_HYPOTHESES)
      )
        throw new RimeStreamError(
          "The service accepted a different transcript contract",
        );
      if (!accepted.language?.tag || !accepted.language.source)
        throw new RimeStreamError(
          "The service did not confirm a resolved language",
        );
      this.language = accepted.language.tag;
      return null;
    }
    if (this.language === null)
      throw new RimeStreamError("Received a transcript before acceptance");
    const append = this.outputContract === "append_fragments";
    if (payload.case === "delta" && append) {
      const text = payload.value.text;
      if (
        Buffer.byteLength(this.text) + Buffer.byteLength(text) >
        this.textLimit
      )
        throw new RimeResourceLimitError(
          "The transcript exceeds the SDK text limit",
        );
      this.text += text;
      return { kind: "delta", text };
    }
    if (payload.case !== "hypothesis" && payload.case !== "done")
      throw new RimeStreamError(
        "Received an unexpected transcription response",
      );
    if (payload.case === "hypothesis" && append)
      throw new RimeStreamError(
        "Received an unexpected transcription response",
      );
    const value = payload.value;
    if (Buffer.byteLength(value.text, "utf8") > this.textLimit)
      throw new RimeResourceLimitError(
        "The transcript exceeds the SDK text limit",
      );
    if (payload.case === "hypothesis") {
      if (
        value.revision <= this.revision ||
        (this.revision === 0n && value.revision !== 1n)
      )
        throw new RimeStreamError(
          "Transcript revisions are not increasing from one",
        );
      this.revision = value.revision;
      this.text = value.text;
      return { kind: "partial", text: value.text };
    }
    if (!inputDone)
      throw new RimeStreamError("The service completed before input finished");
    if (value.revision !== this.revision || value.text !== this.text)
      throw new RimeStreamError(
        "Final transcript does not match prior updates",
      );
    const language = payload.value.language;
    if (language?.tag !== this.language || !language.source)
      throw new RimeStreamError(
        "Final language does not match the accepted language",
      );
    this.final = { kind: "final", text: value.text, language: language.tag };
    return null;
  }
  finish(): TranscriptionFinal {
    if (!this.final)
      throw new RimeStreamError("The service ended without a final transcript");
    return this.final;
  }
}
