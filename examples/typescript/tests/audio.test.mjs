import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EventEmitter } from "node:events";
import { WavWriter, readSpeech } from "../common/wav.ts";
import { BrowserAudio } from "../common/browser-audio.ts";
import { Player } from "../browser/player.js";

test("WAV header preserves PCM and the included fixture is valid speech input", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "rime-wav-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, "reply.wav");
  const writer = new WavWriter(path, 16000);
  writer.write(Buffer.from([1, 0, 2, 0]));
  writer.close();
  assert.deepEqual(readSpeech(path), Buffer.from([1, 0, 2, 0]));
  assert.equal((await readFile(path)).readUInt32LE(4), 40);
  const fixture = readSpeech(
    new URL("../../audio/france.wav", import.meta.url),
  );
  assert.ok(fixture.length > 16000 && fixture.some((byte) => byte !== 0));
});

class Context {
  currentTime = 0;
  outputTime = 0;
  sources = [];
  getOutputTimestamp() {
    return { contextTime: this.outputTime };
  }
  createBuffer(_, frames, rate) {
    return {
      duration: frames / rate,
      getChannelData: () => new Float32Array(frames),
    };
  }
  createBufferSource() {
    const source = {
      connect() {},
      start(at) {
        this.startAt = at;
      },
      stop(at) {
        this.stopAt = at;
      },
    };
    this.sources.push(source);
    return source;
  }
}

test("playback waits for the device clock, including buffered output after interruption", () => {
  const context = new Context();
  const reports = [];
  const player = new Player(context, (report) => reports.push(report));
  player.begin("first");
  player.write("first", new Uint8Array(48000));
  player.end("first");
  context.currentTime = 1.1;
  context.outputTime = 0.9;
  player.poll();
  assert.equal(reports.length, 0);
  context.outputTime = 1.1;
  player.poll();
  assert.equal(reports[0].playedMs, 1000);
  player.begin("cut");
  player.write("cut", new Uint8Array(48000));
  context.currentTime = 1.62;
  context.outputTime = 1.52;
  player.interrupt();
  player.poll();
  assert.equal(reports.length, 1);
  assert.equal(context.sources[1].stopAt, 1.62);
  player.write("cut", new Uint8Array(48000)); // late audio must not play
  assert.equal(context.sources.length, 2);
  context.outputTime = 1.63;
  player.poll();
  player.end("cut");
  assert.equal(reports[1].interrupted, true);
  assert.ok(Math.abs(reports[1].playedMs - 500) < 0.01);
  player.begin("next");
  player.write("next", new Uint8Array(960));
  player.end("next");
  context.outputTime = 2;
  player.poll();
  assert.equal(reports[2].id, "next");
  assert.equal(reports[2].interrupted, false);
});

test("empty and repeated interruptions produce only one report", () => {
  const context = new Context();
  const reports = [];
  const player = new Player(context, (report) => reports.push(report));
  player.begin("silent");
  player.interrupt();
  player.interrupt();
  player.poll();
  player.end("silent");
  player.poll();
  assert.deepEqual(reports, [
    { type: "played", id: "silent", playedMs: 0, interrupted: true },
  ]);
  assert.equal(player.replies.size, 0);
});

export class Socket extends EventEmitter {
  bufferedAmount = 0;
  sent = [];
  send(value) {
    this.sent.push(JSON.parse(value));
    this.emit("sent", JSON.parse(value));
  }
  close() {
    this.emit("close");
  }
  report(value) {
    this.emit("message", Buffer.from(JSON.stringify(value)), false);
  }
}

test("browser disconnect releases waiting capture and playback; input is bounded", async () => {
  const socket = new Socket();
  const audio = new BrowserAudio(socket);
  audio.begin("first");
  const playback = assert.rejects(
    audio.finish("first"),
    /Browser disconnected/,
  );
  const capture = audio.frames().next();
  audio.close();
  assert.equal((await capture).done, true);
  await playback;
  const second = new Socket();
  const bounded = new BrowserAudio(second);
  for (let i = 0; i < 101; i++)
    second.emit("message", Buffer.alloc(3840), true);
  await assert.rejects(bounded.frames().next(), /queue exceeded/);
});
