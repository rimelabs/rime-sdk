import { Rime, AudioStream, AudioFormat } from "../../dist/index.js";

const client = new Rime({ apiKey: "test" });
async function* text() {
  yield "Hello.";
}

const complete: AudioStream = client.tts.synthesize("Hello.");
const incremental: AudioStream = client.tts.stream(text());
void complete;
void incremental;
const buffered: Promise<Buffer> = client.tts.synthesizeToBuffer("Hello.", {
  audioFormat: AudioFormat.WAV,
  sampleRate: 16000,
  timeScaleFactor: 0.9,
});
const rate: number = complete.sampleRate;
void buffered;
void rate;

// @ts-expect-error Complete strings belong to synthesize().
client.tts.stream("Hello.");
// @ts-expect-error Incremental sources belong to stream().
client.tts.synthesize(text());
// @ts-expect-error The source must be asynchronous.
client.tts.stream(["Hello."]);
// @ts-expect-error Buffered synthesis requires a complete string.
client.tts.synthesizeToBuffer(text());
// @ts-expect-error Sample rates are numeric.
client.tts.synthesize("Hello.", { sampleRate: "16000" });
// @ts-expect-error Audio metadata is read-only.
complete.sampleRate = 8000;
