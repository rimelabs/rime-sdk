import { native, call } from "./native.js";
export class AudioFormat {
  static readonly PCM_24000 = Object.freeze(
    new AudioFormat("pcm_s16le", 24000, 1),
  );
  static readonly MULAW_8000 = Object.freeze(new AudioFormat("mulaw", 8000, 1));
  private constructor(
    readonly encoding: string,
    readonly sampleRate: number,
    readonly channels: number,
  ) {}
}
export class Converter {
  private inner: InstanceType<typeof native.Converter>;
  constructor(profile: AudioFormat) {
    this.inner = call(
      () =>
        new native.Converter(
          profile === AudioFormat.PCM_24000 ? "PCM_24000" : "MULAW_8000",
        ),
    );
  }
  process(input: Uint8Array, final = false): Buffer {
    return call(() => this.inner.process(Buffer.from(input), final));
  }
}
