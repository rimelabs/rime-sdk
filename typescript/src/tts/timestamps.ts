import type { SynthesisResponseTrailer, Timestamps } from "@rimelabs/api";
import type { Duration } from "@bufbuild/protobuf/wkt";
import { RimeStreamError } from "../errors.js";

/** A spoken, normalized word with start/end seconds from synthesis start. */
export interface WordTimestamp {
  readonly text: string;
  readonly start: number;
  readonly end: number;
}

/** Alignment's google.rpc.Code (0 means OK) and service explanation. */
export interface TimestampStatus {
  readonly code: number;
  readonly message: string;
}

/** Final alignment status and words. Non-OK statuses carry no spans. */
export interface TimestampResult {
  readonly status: TimestampStatus;
  readonly spans: readonly WordTimestamp[];
}

export class TimestampTrailer {
  seen = false;
  invalid = false;
  private value: Timestamps | undefined;

  accept(trailer: SynthesisResponseTrailer) {
    if (this.seen) this.invalid = true;
    else {
      this.seen = true;
      this.value = trailer.timestamps;
    }
  }

  result(requestId: string | null): TimestampResult {
    const malformed = () =>
      new RimeStreamError("Invalid timestamp trailer", requestId);
    if (this.invalid) throw malformed();
    const value = this.value;
    if (!value)
      throw new RimeStreamError(
        "The service did not return requested timestamps",
        requestId,
      );
    if (!value.status || (value.status.code !== 0 && value.spans.length))
      throw malformed();
    const seconds = (duration: Duration | undefined): number => {
      if (
        !duration ||
        duration.seconds < 0n ||
        duration.seconds > 315576000000n ||
        duration.nanos < 0 ||
        duration.nanos >= 1000000000
      )
        throw malformed();
      return Number(duration.seconds) + duration.nanos / 1e9;
    };
    const spans = value.spans.map((span) => {
      const start = seconds(span.start),
        end = seconds(span.end);
      if (!span.text.trim() || end < start) throw malformed();
      return Object.freeze({ text: span.text, start, end });
    });
    return Object.freeze({
      status: Object.freeze({
        code: value.status.code,
        message: value.status.message,
      }),
      spans: Object.freeze(spans),
    });
  }
}
