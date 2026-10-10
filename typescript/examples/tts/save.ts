import { Rime } from "@rimelabs/sdk";
import { WavWriter } from "../common/wav.js";

const client = new Rime({ timeout: 60 });
const output = new WavWriter("speech.wav");
try {
  const audio = client.tts.synthesize(
    "Your appointment is confirmed for tomorrow at ten in the morning.",
  );
  for await (const chunk of audio) output.write(chunk);
  console.log(`Saved speech.wav; requestId=${audio.requestId}`);
} finally {
  output.close();
  await client.close();
}
