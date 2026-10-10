import { openSync, writeSync, closeSync } from "node:fs";
import { Rime, RimeRealtimeError } from "@rimelabs/sdk";

async function saveReply(session) {
  const output = openSync("reply.pcm", "w");
  try {
    for await (const { payload } of session.events) {
      switch (payload.kind) {
        case "text.delta":
          process.stdout.write(payload.delta);
          break;
        case "audio.delta":
          writeSync(output, payload.audio.data);
          break;
        case "error":
          throw new RimeRealtimeError(payload.error);
        case "response.ended":
          if (payload.status !== "completed")
            throw new Error(`Response ${payload.status}: ${payload.reason}`);
          console.log(
            "\nSaved reply.pcm: mono 24 kHz signed little-endian PCM16.",
          );
          return;
      }
    }
    throw new Error("Session closed before the response ended");
  } finally {
    closeSync(output);
  }
}

const client = new Rime();
// Bound the whole example, including generation after response creation.
const deadline = setTimeout(() => void client.close(), 60_000);
let reading;
try {
  const session = await client.realtime.connect({
    endpoint: process.env.PRISM_URL,
    voice: process.env.PRISM_VOICE,
    instructions: "Give short, clear answers.",
  });
  reading = saveReply(session);
  await Promise.all([
    reading,
    session.sendText("Hello! What can you help me with?"),
  ]);
} finally {
  clearTimeout(deadline);
  await client.close();
  await reading?.catch(() => {});
}
