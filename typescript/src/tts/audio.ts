import { RimeAudioFormatError, RimeInputError } from "../errors.js";
export class AudioFormat {
  static readonly PCM = Object.freeze(new AudioFormat("pcm_s16le", 0, 1));
  static readonly WAV = Object.freeze(new AudioFormat("wav", 0, 1));
  static readonly MP3 = Object.freeze(new AudioFormat("mp3", 0, 1));
  static readonly PCM_24000 = Object.freeze(
    new AudioFormat("pcm_s16le", 24000, 1),
  );
  static readonly MULAW_8000 = Object.freeze(new AudioFormat("mulaw", 8000, 1));
  private constructor(
    readonly encoding: string,
    /** Fixed preset rate, or zero when selected per request. See AudioStream.sampleRate. */
    readonly sampleRate: number,
    readonly channels: number,
  ) {}
}
export function wireFormat(format: AudioFormat): string {
  if (format === AudioFormat.WAV) return "audio/wav";
  if (format === AudioFormat.MP3) return "audio/mpeg";
  return "audio/pcm";
}
export function resolveAudio(
  format: AudioFormat | null | undefined,
  sampleRate: number | null | undefined,
  timeScaleFactor: number | null | undefined,
) {
  format ??= AudioFormat.PCM;
  if (
    ![
      AudioFormat.PCM,
      AudioFormat.PCM_24000,
      AudioFormat.MULAW_8000,
      AudioFormat.WAV,
      AudioFormat.MP3,
    ].includes(format)
  )
    throw new RimeAudioFormatError("Select a named AudioFormat");
  sampleRate ??= format.sampleRate || 24000;
  if (![8000, 16000, 24000].includes(sampleRate))
    throw new RimeAudioFormatError("sampleRate must be 8000, 16000, or 24000");
  if (format.sampleRate && sampleRate !== format.sampleRate)
    throw new RimeAudioFormatError(
      "sampleRate conflicts with the fixed AudioFormat preset",
    );
  if (
    timeScaleFactor != null &&
    (!Number.isFinite(timeScaleFactor) ||
      timeScaleFactor < 0.4 ||
      timeScaleFactor > 2.5)
  )
    throw new RimeInputError(
      "timeScaleFactor must be a finite number from 0.4 to 2.5",
    );
  if (format === AudioFormat.PCM && sampleRate === 24000)
    format = AudioFormat.PCM_24000;
  return { format, sampleRate };
}
const raw = Array.from(
  { length: 63 },
  (_, i) =>
    (i === 31
      ? (2 * 3400) / 24000
      : Math.sin(((2 * Math.PI * 3400) / 24000) * (i - 31)) /
        (Math.PI * (i - 31))) *
    (0.54 - 0.46 * Math.cos((2 * Math.PI * i) / 62)),
);
const total = raw.reduce((a, b) => a + b, 0);
const coefficients = raw.map((x) => x / total);
function mulaw(sample: number): number {
  sample = Math.max(-32768, Math.min(32767, sample));
  // G.711 uses one's complement for negative PCM before quantization.
  const sign = sample < 0 ? 128 : 0,
    magnitude = Math.min(sample < 0 ? ~sample : sample, 32635) + 132;
  const exponent = Math.max(0, Math.floor(Math.log2(magnitude)) - 7);
  return ~(sign | (exponent << 4) | ((magnitude >> (exponent + 3)) & 15)) & 255;
}
export class Converter {
  private tail = Buffer.alloc(0);
  private samples = Array<number>(63).fill(0);
  private seen = 0;
  private emitted = 0;
  private inputSamples = 0;
  constructor(private profile: AudioFormat) {}
  private sample(sample: number): number | null {
    this.samples.pop();
    this.samples.unshift(sample);
    const position = this.seen++ - 31;
    if (position >= 0 && position % 3 === 0) {
      const value = coefficients.reduce(
        (sum, c, i) => sum + c * this.samples[i]!,
        0,
      );
      this.emitted++;
      return mulaw(Math.floor(value + 0.5));
    }
    return null;
  }
  process(input: Uint8Array, final = false): Buffer {
    if (this.profile === AudioFormat.WAV || this.profile === AudioFormat.MP3)
      return Buffer.from(input);
    const data = Buffer.concat([this.tail, input]);
    const size = Math.floor(data.length / 2) * 2;
    this.tail = Buffer.from(data.subarray(size));
    if (final && this.tail.length)
      throw new RimeAudioFormatError("Incomplete final PCM sample frame");
    if (
      this.profile === AudioFormat.PCM ||
      this.profile === AudioFormat.PCM_24000
    )
      return data.subarray(0, size);
    const result: number[] = [];
    for (let i = 0; i < size; i += 2) {
      this.inputSamples++;
      const value = this.sample(data.readInt16LE(i));
      if (value !== null) result.push(value);
    }
    if (final) {
      const target = Math.ceil(this.inputSamples / 3);
      while (this.emitted < target) {
        const value = this.sample(0);
        if (value !== null) result.push(value);
      }
    }
    return Buffer.from(result);
  }
}
