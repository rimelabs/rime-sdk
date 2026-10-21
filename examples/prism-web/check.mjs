// Check your setup without a microphone: send a recorded question to Prism, print the
// reply text, and save the reply audio to reply.wav.
// Run: RIME_API_KEY=... node check.mjs
import { readFileSync, writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { Rime, RimeRealtimeError } from "@rimelabs/sdk";

const ENDPOINT = process.env.PRISM_URL ?? "wss://prism.api.rime.ai/v1/realtime";
const INSTRUCTIONS =
  "You answer the phone for Juniper Cafe. It's open from 8 a.m. to 4 p.m. " +
  "every day. Answer in one short sentence.";
const FRAME = 1280; // 40 ms of 16 kHz mono PCM16

// The samples of a 16 kHz mono 16-bit PCM WAV file. Reads the "fmt " and "data" chunks.
function readWav(path) {
  const file = readFileSync(path);
  const fail = (why) => new Error(`${path}: ${why}`);
  if (
    file.toString("ascii", 0, 4) !== "RIFF" ||
    file.toString("ascii", 8, 12) !== "WAVE"
  )
    throw fail("not a WAV file");
  let format;
  for (let at = 12; at + 8 <= file.length;) {
    const id = file.toString("ascii", at, at + 4);
    const size = file.readUInt32LE(at + 4);
    const body = at + 8;
    if (body + size > file.length) throw fail("the file is cut short");
    if (id === "fmt ")
      format = {
        pcm: file.readUInt16LE(body) === 1,
        channels: file.readUInt16LE(body + 2),
        rate: file.readUInt32LE(body + 4),
        bits: file.readUInt16LE(body + 14),
      };
    if (id === "data") {
      if (
        !format?.pcm ||
        format.channels !== 1 ||
        format.rate !== 16000 ||
        format.bits !== 16
      )
        throw fail("must be 16 kHz mono 16-bit PCM");
      if (size === 0) throw fail("has no audio");
      return file.subarray(body, body + size);
    }
    at = body + size + (size % 2); // chunks are padded to an even length
  }
  throw fail("has no audio data");
}

async function sendCallAudio(session, question, signal) {
  // Send the question at speaking pace, then silence, as a live call would.
  for (let offset = 0; ; offset += FRAME) {
    const frame = Buffer.alloc(FRAME);
    question.copy(frame, 0, Math.min(offset, question.length), offset + FRAME);
    await session.sendAudio({ data: frame });
    await sleep(40, undefined, { signal });
  }
}

async function readReply(session) {
  const audio = [];
  for await (const { payload } of session.events) {
    switch (payload.kind) {
      case "speech.started":
        console.log("Caller started speaking");
        break;
      case "speech.stopped":
        console.log("Caller stopped speaking");
        break;
      case "response.started":
        process.stdout.write("Prism: ");
        break;
      case "text.delta":
        process.stdout.write(payload.delta);
        break;
      case "audio.delta":
        audio.push(payload.audio.data);
        break;
      case "error":
        throw new RimeRealtimeError(payload.error);
      case "response.ended":
        console.log();
        if (payload.status !== "completed")
          throw new Error(`Reply ${payload.status}: ${payload.reason}`);
        return Buffer.concat(audio);
    }
  }
  throw new Error("The session closed before the reply ended");
}

// Prism replies in 24 kHz mono PCM16. A 44-byte header makes it a WAV file.
function toWav(pcm) {
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVEfmt ", 8);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(24000, 24); // samples per second
  header.writeUInt32LE(48000, 28); // bytes per second
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

const question = readWav(process.argv[2] ?? "question.wav");
const client = new Rime();
const stop = new AbortController();
const deadline = setTimeout(() => void client.close(), 60_000);
let sending;
try {
  const session = await client.realtime.connect({
    endpoint: ENDPOINT,
    voice: "astra",
    instructions: INSTRUCTIONS,
  });
  sending = sendCallAudio(session, question, stop.signal);
  const reply = await Promise.race([readReply(session), sending]);
  writeFileSync("reply.wav", toWav(reply));
  console.log(
    `Saved ${(reply.length / 48000).toFixed(1)} seconds of audio to reply.wav`,
  );
} finally {
  stop.abort();
  clearTimeout(deadline);
  await client.close();
  await sending?.catch(() => {});
}
