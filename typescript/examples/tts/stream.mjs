// From the repository root: npm --prefix typescript/examples run tts
import { writeFile } from "node:fs/promises";
import { Rime } from "@rimelabs/sdk";

async function* text() {
  yield "Hello. ";
  yield "This text arrives in separate chunks. ";
  yield "Rime automatically combines text fragments into sentences for speech.";
}

const client = new Rime();
try {
  const audio = client.tts.stream(text());
  await writeFile("speech.pcm", audio);
  console.log(
    `Wrote mono 24 kHz signed 16-bit PCM; requestId=${audio.requestId}`,
  );
} finally {
  await client.close();
}
