import {
  Rime,
  type PCMFormat,
  type TranscriptionUpdate,
  type TranscriptionFinal,
  type TranscriptionOptions,
} from "../../dist/index.js";

const format: PCMFormat = { sampleRate: 48000, channels: 2 };
const options: TranscriptionOptions = {
  language: "en",
  inputFormat: format,
  mode: "verbatim",
  contextTerms: ["Rime"],
};
const client = new Rime({ apiKey: "test", sttEndpoint: "stt.example:443" });
async function* audio() {
  yield new Uint8Array(4);
}
const stream = client.stt.stream(audio(), options);
function consume(update: TranscriptionUpdate): string {
  if (update.kind === "final") {
    const final: TranscriptionFinal = update;
    return final.language;
  }
  // @ts-expect-error Partial transcripts do not advertise a final language.
  update.language;
  return update.text;
}
async function run() {
  for await (const update of stream) consume(update);
  await stream.cancel();
  await client.close();
}
void run;
// @ts-expect-error Spoken language is required.
client.stt.stream(audio(), {});
client.stt.stream(audio(), {
  language: "en",
  // @ts-expect-error Only declared PCM sample rates are supported.
  inputFormat: { sampleRate: 44100 },
});
