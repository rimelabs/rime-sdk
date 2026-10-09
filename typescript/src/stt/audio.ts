import { RimeAudioFormatError, RimeInputError } from "../errors.js";
import { InputConverter, resolvePCMFormat, type PCMFormat } from "../pcm.js";

// Bound conversion work and temporary allocations for large source chunks.
const sourceBytes = 16384;
// Maximum decoded STT audio payload, excluding the protobuf envelope.
const messageBytes = 65536;

/** One utterance of headerless signed PCM16 little-endian mono 16 kHz output.
 * Fully consume each feed iterator before feeding the next chunk. Conversion
 * advances one bounded source portion at a time so callers can apply backpressure.
 */
export class InputAudio {
  private readonly converter: InputConverter;
  private readonly frameBytes: number;
  private pending = Buffer.alloc(0);
  private finished = false;

  constructor(format?: PCMFormat) {
    const resolved = resolvePCMFormat(format);
    this.converter = new InputConverter(resolved);
    this.frameBytes = 2 * resolved.channels;
  }

  *feed(data: Uint8Array): Generator<Buffer> {
    if (this.finished) throw new RimeInputError("The audio input is finished");
    if (!(data instanceof Uint8Array))
      throw new RimeAudioFormatError(
        "The audio source must yield PCM16 Uint8Array chunks",
      );
    for (let offset = 0; offset < data.length; offset += sourceBytes) {
      const part = Buffer.concat([
        this.pending,
        data.subarray(offset, offset + sourceBytes),
      ]);
      const complete = part.length - (part.length % this.frameBytes);
      this.pending = Buffer.from(part.subarray(complete));
      if (complete) {
        const converted = this.converter.process(part.subarray(0, complete));
        for (let start = 0; start < converted.length; start += messageBytes)
          yield converted.subarray(start, start + messageBytes);
      }
    }
  }

  /** Check EOF before committing; never silently pad or truncate a final frame. */
  finish(): void {
    this.finished = true;
    if (this.pending.length)
      throw new RimeAudioFormatError(
        "The audio source ended with an incomplete PCM16 frame",
      );
  }
}
