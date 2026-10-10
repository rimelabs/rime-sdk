import { parseArgs } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import { Rime, RimeRealtimeError } from "@rimelabs/sdk";
import { readSpeech, WavWriter } from "../common/wav.js";

const { values } = parseArgs({
  options: {
    input: { type: "string" },
    output: { type: "string", default: "reply.wav" },
  },
});
const data = readSpeech(
  values.input ??
    new URL("../../../fixtures/audio/france.wav", import.meta.url),
);
const endpoint = process.env.PRISM_URL;
if (!endpoint)
  throw new Error(
    "Set PRISM_URL to your deployment's /v1/realtime WebSocket URL",
  );
const client = new Rime();
const stop = new AbortController();
const deadline = setTimeout(() => {
  stop.abort();
  void client.close();
}, 60_000);
const output = new WavWriter(values.output!);
let sending: Promise<void> | undefined;
let reading: Promise<void> | undefined;
try {
  const session = await client.realtime.connect({
    endpoint,
    voice: process.env.PRISM_VOICE,
    instructions: "Give short, clear answers.",
  });
  sending = (async () => {
    // Silence after the recording lets Prism detect the end of the spoken turn.
    for (let offset = 0; ; offset += 1280) {
      const frame = Buffer.alloc(1280);
      if (offset < data.length)
        data.copy(frame, 0, offset, Math.min(offset + 1280, data.length));
      await session.sendAudio({ data: frame });
      await sleep(40, undefined, { signal: stop.signal });
    }
  })();
  reading = (async () => {
    for await (const { payload } of session.events) {
      switch (payload.kind) {
        case "audio.delta":
          output.write(payload.audio.data);
          break;
        case "transcript.final":
          console.log(`You: ${payload.text}`);
          break;
        case "text.done":
          console.log(`Prism: ${payload.text}`);
          break;
        case "error":
          throw new RimeRealtimeError(payload.error);
        case "response.ended":
          if (payload.status !== "completed")
            throw new Error(`Response ${payload.status}: ${payload.reason}`);
          return;
      }
    }
    throw new Error("Session closed before the reply ended");
  })();
  await Promise.race([sending, reading]);
  console.log(`Saved ${values.output}`);
} finally {
  stop.abort();
  clearTimeout(deadline);
  await client.close();
  await Promise.allSettled([sending, reading]);
  output.close();
}
