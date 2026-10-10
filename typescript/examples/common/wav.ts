import { openSync, writeSync, closeSync, readFileSync } from "node:fs";

/** Mono PCM16 only. The header is finalized even if synthesis fails. */
export class WavWriter {
  private file: number;
  private bytes = 0;
  constructor(
    path: string,
    private rate = 24000,
  ) {
    this.file = openSync(path, "w");
    writeSync(this.file, Buffer.alloc(44));
  }
  write(data: Uint8Array) {
    if (data.length % 2) throw new Error("Incomplete PCM16 sample");
    if (this.bytes + data.length > 0xffffffff - 36)
      throw new Error("WAV is too large");
    let offset = 0;
    while (offset < data.length) offset += writeSync(this.file, data, offset);
    this.bytes += data.length;
  }
  close() {
    const header = Buffer.alloc(44);
    header.write("RIFF");
    header.writeUInt32LE(36 + this.bytes, 4);
    header.write("WAVEfmt ", 8);
    header.writeUInt32LE(16, 16);
    header.writeUInt16LE(1, 20);
    header.writeUInt16LE(1, 22);
    header.writeUInt32LE(this.rate, 24);
    header.writeUInt32LE(this.rate * 2, 28);
    header.writeUInt16LE(2, 32);
    header.writeUInt16LE(16, 34);
    header.write("data", 36);
    header.writeUInt32LE(this.bytes, 40);
    try {
      writeSync(this.file, header, 0, header.length, 0);
    } finally {
      closeSync(this.file);
    }
  }
}

export function readSpeech(path: string | URL): Buffer {
  const file = readFileSync(path);
  if (
    file.toString("ascii", 0, 4) !== "RIFF" ||
    file.toString("ascii", 8, 12) !== "WAVE"
  )
    throw new Error("Input must be a WAV file");
  let valid = false;
  let data: Buffer | undefined;
  for (let offset = 12; offset + 8 <= file.length;) {
    const name = file.toString("ascii", offset, offset + 4);
    const length = file.readUInt32LE(offset + 4);
    const start = offset + 8;
    if (start + length > file.length) throw new Error("Truncated WAV file");
    if (name === "fmt " && length >= 16) {
      valid =
        file.readUInt16LE(start) === 1 &&
        file.readUInt16LE(start + 2) === 1 &&
        file.readUInt32LE(start + 4) === 16000 &&
        file.readUInt16LE(start + 14) === 16;
    }
    if (name === "data") data = file.subarray(start, start + length);
    offset = start + length + (length % 2);
  }
  if (!valid || !data?.length || data.length % 2)
    throw new Error("Input must be a nonempty 16 kHz mono PCM16 WAV file");
  return data;
}
