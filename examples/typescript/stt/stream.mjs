import { createReadStream } from "node:fs";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { Rime } from "@rimelabs/sdk";

async function* audioChunks(path) {
  yield* createReadStream(path, { highWaterMark: 3200 });
}

/** Transcribe one headerless signed PCM16 little-endian mono 16 kHz file. */
export async function main(
  path,
  language,
  mode = "written",
  client = new Rime(),
) {
  try {
    const stream = client.stt.stream(audioChunks(path), {
      language,
      mode,
      timeout: 120,
    });
    for await (const update of stream)
      console.log(`${update.kind}: ${update.text}`);
    console.log(`request_id: ${stream.requestId}`);
  } finally {
    await client.close();
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const { positionals, values } = parseArgs({
    allowPositionals: true,
    options: {
      language: { type: "string" },
      mode: { type: "string", default: "written" },
    },
  });
  if (positionals.length !== 1 || !values.language)
    throw new Error(
      "Usage: stream.mjs audio.pcm --language en [--mode written|verbatim]",
    );
  await main(positionals[0], values.language, values.mode);
}
