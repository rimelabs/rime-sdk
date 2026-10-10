import { RimeAudioFormatError } from "../errors.js";
import { resolvePCMFormat } from "../pcm.js";
import type { AudioChunk, PCMFormat } from "./types.js";

export function formatOf(chunk: AudioChunk): Required<PCMFormat> {
  const format = resolvePCMFormat(chunk.format);
  if (
    !(chunk.data instanceof Uint8Array) ||
    chunk.data.length % (2 * format.channels)
  )
    throw new RimeAudioFormatError(
      "Audio must contain complete PCM16 frames as Uint8Array",
    );
  if (chunk.data.length > 192000)
    throw new RimeAudioFormatError(
      "Send audio in chunks of at most 192000 bytes",
    );
  return format;
}
