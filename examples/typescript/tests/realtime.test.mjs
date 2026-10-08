import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Rime } from "@rimelabs/sdk";
import { Peer } from "../../../typescript/tests/realtime-peer.mjs";
import { BrowserAudio } from "../common/browser-audio.ts";
import { converse } from "../realtime/conversation.ts";

const exec = promisify(execFile);

test("recorded example sends audio and creates a playable reply without devices", async (t) => {
  const peer = await new Peer().start();
  const directory = await mkdtemp(join(tmpdir(), "rime-recorded-"));
  t.after(async () => {
    await peer.close();
    await rm(directory, { recursive: true, force: true });
  });
  let replied = false;
  peer.onRequest = (request) => {
    assert.equal(request.type, "input_audio_buffer.append");
    if (replied) return;
    replied = true;
    peer.emit("response.created", { response: { id: "speech" } });
    peer.message("speech");
    peer.emit("response.audio.delta", {
      response_id: "speech",
      item_id: "message-1",
      output_index: 0,
      content_index: 0,
      delta: Buffer.from([1, 0, 2, 0]).toString("base64"),
    });
    peer.ended("speech");
  };
  await exec(
    process.execPath,
    [
      "--import",
      fileURLToPath(
        new URL("../node_modules/tsx/dist/loader.mjs", import.meta.url),
      ),
      fileURLToPath(new URL("../realtime/recorded.ts", import.meta.url)),
    ],
    {
      cwd: directory,
      timeout: 10000,
      env: {
        ...process.env,
        RIME_API_KEY: "test",
        PRISM_URL: peer.endpoint,
        PRISM_VOICE: "test",
      },
    },
  );
  const wav = await readFile(join(directory, "reply.wav"));
  assert.equal(wav.toString("ascii", 0, 4), "RIFF");
  assert.equal(wav.readUInt32LE(24), 24000);
  assert.deepEqual(wav.subarray(44), Buffer.from([1, 0, 2, 0]));
  assert.ok(replied);
});

test("voice interruption reports the cut and continues into a completed next turn", async (t) => {
  const peer = await new Peer().start();
  peer.autoInitialize = false;
  peer.onRequest = (request) => {
    if (request.type === "session.update")
      peer.emit("session.updated", {
        prsm_request_event_id: request.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prsm_effective_interrupt_response: true,
        },
      });
    else if (peer.waiters.length) peer.waiters.shift()(request);
    else peer.queue.push(request);
  };
  const client = new Rime({ apiKey: "test" });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  class Socket extends EventEmitter {
    bufferedAmount = 0;
    send(raw) {
      const message = JSON.parse(raw);
      if (message.type === "end")
        queueMicrotask(() =>
          this.emit(
            "message",
            Buffer.from(
              JSON.stringify({
                type: "played",
                id: message.id,
                interrupted: message.id === "first",
                playedMs: message.id === "first" ? 20 : 40,
              }),
            ),
            false,
          ),
        );
    }
    close() {
      this.emit("close");
    }
  }
  const audio = new BrowserAudio(new Socket());
  const running = converse(session, audio);
  t.after(async () => {
    audio.close();
    await client.close();
    await running.catch(() => {});
    await peer.close();
  });
  function reply(id) {
    peer.emit("response.created", { response: { id } });
    peer.message(id, id);
    peer.emit("response.audio.delta", {
      response_id: id,
      item_id: id,
      output_index: 0,
      content_index: 0,
      delta: Buffer.alloc(1920).toString("base64"),
    });
  }
  reply("first");
  peer.emit("input_audio_buffer.speech_started", {
    item_id: "user",
    audio_start_ms: 0,
  });
  peer.ended("first", "cancelled");
  const cut = await peer.next("conversation.item.truncate");
  assert.equal(cut.item_id, "first");
  assert.equal(cut.audio_end_ms, 20);
  peer.emit("conversation.item.truncated", {
    item_id: "first",
    content_index: 0,
    audio_end_ms: 20,
    prsm_request_event_id: cut.event_id,
  });
  reply("next");
  peer.ended("next");
  const drained = await peer.next("prsm.playback.drained");
  assert.equal(drained.response_id, "next");
  assert.equal(drained.played_ms, 40);
  audio.close();
  await running;
});

for (const interrupted of [false, true]) {
  test(
    `disconnect does not send a playback measurement: interrupted=${interrupted}`,
    { timeout: 3000 },
    async (t) => {
      const peer = await new Peer().start();
      peer.autoInitialize = false;
      peer.onRequest = (request) => {
        if (request.type === "session.update")
          peer.emit("session.updated", {
            prsm_request_event_id: request.event_id,
            session: {
              id: "session-1",
              voice: "test",
              prsm_effective_interrupt_response: true,
            },
          });
      };
      const client = new Rime({ apiKey: "test" });
      const session = await client.realtime.connect({
        endpoint: peer.endpoint,
      });
      // Keep capture busy during disconnect so it cannot win the shutdown race.
      let release;
      session.sendAudio = () =>
        new Promise((resolve) => {
          release = resolve;
        });
      const close = session.close.bind(session);
      session.close = async () => {
        release?.();
        await close();
      };
      let finish;
      const finishing = new Promise((resolve) => {
        finish = resolve;
      });
      class Socket extends EventEmitter {
        bufferedAmount = 0;
        send(raw) {
          if (JSON.parse(raw).type === "end") finish();
        }
        close() {
          this.emit("close");
        }
      }
      const socket = new Socket();
      const audio = new BrowserAudio(socket);
      const running = converse(session, audio);
      const closed = assert.rejects(running, /Browser disconnected/);
      const remoteClosed = new Promise((resolve) =>
        peer.socket.once("close", resolve),
      );
      t.after(async () => {
        audio.close();
        await client.close();
        await running.catch(() => {});
        await peer.close();
      });
      socket.emit("message", Buffer.alloc(3840), true);
      peer.emit("response.created", { response: { id: "reply" } });
      peer.message("reply", "reply");
      if (interrupted)
        peer.emit("input_audio_buffer.speech_started", {
          item_id: "user",
          audio_start_ms: 0,
        });
      peer.ended("reply", interrupted ? "cancelled" : "completed");
      await finishing;
      socket.close();
      await closed;
      await remoteClosed;
      assert.deepEqual(
        peer.requests.filter((request) =>
          ["conversation.item.truncate", "prsm.playback.drained"].includes(
            request.type,
          ),
        ),
        [],
      );
    },
  );
}
