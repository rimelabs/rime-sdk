import { setTimeout as sleep } from "node:timers/promises";
import { Rime } from "@rimelabs/sdk";
import { serve } from "../common/browser-audio.js";

async function* text() {
  // Replace this generator with text deltas from your application's LLM.
  for (const sentence of [
    "Your appointment is confirmed. ",
    "It is tomorrow at ten in the morning. ",
    "Please arrive fifteen minutes early.",
  ]) {
    yield sentence;
    await sleep(500);
  }
}

serve("tts", async (audio) => {
  const client = new Rime({ timeout: 60 });
  const disconnected = (async () => {
    for await (const _ of audio.frames()) {
      /* no microphone in TTS mode */
    }
  })();
  const playing = (async () => {
    audio.send({ type: "ready" });
    audio.begin("tts");
    for await (const chunk of client.tts.stream(text()))
      audio.write("tts", chunk);
    await audio.finish("tts");
  })();
  try {
    await Promise.race([playing, disconnected]);
  } finally {
    audio.close();
    await client.close();
    await Promise.allSettled([playing, disconnected]);
  }
});
