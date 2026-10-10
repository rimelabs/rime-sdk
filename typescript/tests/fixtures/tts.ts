import { Rime, AudioStream } from "../../dist/index.js";

const client = new Rime({ apiKey: "test" });
async function* text() {
  yield "Hello.";
}

const complete: AudioStream = client.tts.synthesize("Hello.");
const incremental: AudioStream = client.tts.stream(text());
void complete;
void incremental;

// @ts-expect-error Complete strings belong to synthesize().
client.tts.stream("Hello.");
// @ts-expect-error Incremental sources belong to stream().
client.tts.synthesize(text());
// @ts-expect-error The source must be asynchronous.
client.tts.stream(["Hello."]);
