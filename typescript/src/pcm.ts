import { RimeAudioFormatError } from "./errors.js";

/** Headerless signed little-endian PCM16; stereo samples are interleaved. */
export interface PCMFormat {
  /** Input frames per second; omitted means 16000. */
  readonly sampleRate?: 8000 | 16000 | 24000 | 48000;
  /** Samples per frame; omitted means one (mono). */
  readonly channels?: 1 | 2;
  /** Two-byte signed samples with no file header. */
  readonly encoding?: "pcm_s16le";
}

export function resolvePCMFormat(input?: PCMFormat): Required<PCMFormat> {
  if (
    input !== undefined &&
    (input === null || typeof input !== "object" || Array.isArray(input))
  )
    throw new RimeAudioFormatError("Input format must be a PCM format object");
  const format = {
    sampleRate: input?.sampleRate ?? 16000,
    channels: input?.channels ?? 1,
    encoding: input?.encoding ?? "pcm_s16le",
  };
  if (
    ![8000, 16000, 24000, 48000].includes(format.sampleRate) ||
    ![1, 2].includes(format.channels) ||
    format.encoding !== "pcm_s16le"
  )
    throw new RimeAudioFormatError(
      "Use PCM16 at 8, 16, 24 or 48 kHz, with one or two channels",
    );
  return format;
}

/** Streaming linear interpolation, matching Python audioop.ratecv with no filter.
 * Integer phase remains bounded and preserves samples across chunk boundaries.
 */
export class InputConverter {
  private phase = -16000;
  private previous = 0;
  constructor(readonly format: Required<PCMFormat>) {}
  clone() {
    const copy = new InputConverter(this.format);
    copy.phase = this.phase;
    copy.previous = this.previous;
    return copy;
  }
  process(input: Uint8Array): Buffer {
    const data = Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    const samples: number[] = [];
    for (
      let offset = 0;
      offset < data.length;
      offset += this.format.channels * 2
    ) {
      const current =
        this.format.channels === 1
          ? data.readInt16LE(offset)
          : Math.floor(
              (data.readInt16LE(offset) + data.readInt16LE(offset + 2)) / 2,
            );
      this.phase += 16000;
      while (this.phase >= 0) {
        samples.push(
          Math.floor(
            (this.previous * this.phase + current * (16000 - this.phase)) /
              16000,
          ),
        );
        this.phase -= this.format.sampleRate;
      }
      this.previous = current;
    }
    const output = Buffer.alloc(samples.length * 2);
    samples.forEach((sample, index) => output.writeInt16LE(sample, index * 2));
    return output;
  }
}
