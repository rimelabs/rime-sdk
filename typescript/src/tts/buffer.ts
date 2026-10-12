import { RimeAudioFormatError } from "../errors.js";

export const maxBufferBytes = 64 * 1024 * 1024;

/** Validate mono PCM16 WAV and replace unknown streaming chunk lengths. */
export function finalizeWav(
  data: Buffer,
  sampleRate: number,
  requestId: string | null,
): void {
  const invalid = () =>
    new RimeAudioFormatError(
      "Expected a complete mono PCM16 WAV at the requested sample rate",
      requestId,
    );
  if (
    data.length < 12 ||
    data.toString("latin1", 0, 4) !== "RIFF" ||
    data.toString("latin1", 8, 12) !== "WAVE"
  )
    throw invalid();
  if (data.length - 8 > 0xffffffff)
    throw new RimeAudioFormatError(
      "Audio exceeds the WAV size limit",
      requestId,
    );
  if (![0xffffffff, data.length - 8].includes(data.readUInt32LE(4)))
    throw invalid();
  let offset = 12;
  let foundFormat = false;
  let dataSizeOffset: number | undefined;
  let dataSize = 0;
  while (offset < data.length) {
    if (data.length - offset < 8) throw invalid();
    const name = data.toString("latin1", offset, offset + 4);
    let size = data.readUInt32LE(offset + 4);
    const start = offset + 8;
    if (name === "data" && size === 0xffffffff) size = data.length - start;
    const end = start + size;
    if (end > data.length) throw invalid();
    if (name === "fmt ") {
      if (
        foundFormat ||
        size < 16 ||
        data.readUInt16LE(start) !== 1 ||
        data.readUInt16LE(start + 2) !== 1 ||
        data.readUInt32LE(start + 4) !== sampleRate ||
        data.readUInt32LE(start + 8) !== sampleRate * 2 ||
        data.readUInt16LE(start + 12) !== 2 ||
        data.readUInt16LE(start + 14) !== 16
      )
        throw invalid();
      foundFormat = true;
    } else if (name === "data") {
      if (!foundFormat || dataSizeOffset !== undefined || size % 2)
        throw invalid();
      dataSizeOffset = offset + 4;
      dataSize = size;
    }
    offset = end + (size % 2);
  }
  if (offset !== data.length || dataSizeOffset === undefined) throw invalid();
  data.writeUInt32LE(data.length - 8, 4);
  data.writeUInt32LE(dataSize, dataSizeOffset);
}
