// This processor sends 40 ms PCM16 frames. Its output remains silent.
class Capture extends AudioWorkletProcessor {
  constructor() {
    super();
    this.frame = new Int16Array(1920);
    this.offset = 0;
  }
  process(inputs) {
    const input = inputs[0]?.[0];
    if (input)
      for (const value of input) {
        this.frame[this.offset++] = Math.round(
          Math.max(-1, Math.min(1, value)) * 32767,
        );
        if (this.offset === this.frame.length) {
          this.port.postMessage(this.frame.buffer, [this.frame.buffer]);
          this.frame = new Int16Array(1920);
          this.offset = 0;
        }
      }
    return true;
  }
}
registerProcessor("capture", Capture);
