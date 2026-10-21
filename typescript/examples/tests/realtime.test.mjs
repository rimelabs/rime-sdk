import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Rime, runConversation } from "@rimelabs/sdk";
import { Peer } from "../../tests/realtime-peer.mjs";
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
      import.meta.resolve("tsx"),
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
        prism_request_event_id: request.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prism_effective_interrupt_response: true,
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
                interrupted: JSON.parse(message.id)[0] === "first",
                playedMs: JSON.parse(message.id)[0] === "first" ? 20 : 40,
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
    prism_request_event_id: cut.event_id,
  });
  reply("next");
  peer.ended("next");
  const drained = await peer.next("prism.playback.drained");
  assert.equal(drained.response_id, "next");
  assert.equal(drained.played_ms, 40);
  audio.close();
  await assert.rejects(
    running,
    /Browser disconnected before shutdown confirmation/,
  );
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
            prism_request_event_id: request.event_id,
            session: {
              id: "session-1",
              voice: "test",
              prism_effective_interrupt_response: true,
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
          ["conversation.item.truncate", "prism.playback.drained"].includes(
            request.type,
          ),
        ),
        [],
      );
    },
  );
}

for (const queue of ["input", "output"]) {
  test(
    `browser ${queue} queue limit fails the public runner`,
    { timeout: 3000 },
    async (t) => {
      const peer = await new Peer().start();
      const client = new Rime({ apiKey: "test" });
      const session = await client.realtime.connect({
        endpoint: peer.endpoint,
      });
      let closed = false;
      class Socket extends EventEmitter {
        bufferedAmount = 0;
        send(raw) {
          if (queue === "output" && JSON.parse(raw).type === "begin")
            this.bufferedAmount = 3 * 1024 * 1024;
        }
        close() {
          closed = true;
          this.emit("close");
        }
      }
      const socket = new Socket();
      const audio = new BrowserAudio(socket);
      const running = runConversation(session, {
        source: audio,
        playback: audio,
      });
      const failed = assert.rejects(running, /queue exceeded|queue is full/);
      t.after(async () => {
        audio.close();
        await client.close();
        await running.catch(() => {});
        await peer.close();
      });
      if (queue === "input") {
        for (let i = 0; i < 101; i++)
          socket.emit("message", Buffer.alloc(3840), true);
      } else {
        peer.emit("response.created", { response: { id: "reply" } });
        peer.message("reply", "reply");
        peer.emit("response.audio.delta", {
          response_id: "reply",
          item_id: "reply",
          output_index: 0,
          content_index: 0,
          delta: Buffer.alloc(960).toString("base64"),
        });
      }
      await failed;
      // Invalid incoming audio closes the transport; failed stop() keeps ownership with the caller.
      assert.equal(closed, queue === "input");
      assert.deepEqual(
        peer.requests.filter(
          (request) => request.type === "prism.playback.drained",
        ),
        [],
      );
    },
  );
}

for (const reason of ["eof", "closed", "cancelled"]) {
  test(
    `public runner waits for remote playback shutdown: ${reason}`,
    { timeout: 4000 },
    async (t) => {
      const peer = await new Peer().start();
      const client = new Rime({ apiKey: "test" });
      const session = await client.realtime.connect({
        endpoint: peer.endpoint,
      });
      const controller = new AbortController();
      const started = Promise.withResolvers(),
        stopping = Promise.withResolvers();
      let finishInput, receiptSignal;
      class Socket extends EventEmitter {
        bufferedAmount = 0;
        playing = false;
        closed = false;
        outputId;
        send(raw) {
          const message = JSON.parse(raw);
          if (message.type === "begin") this.outputId = message.id;
          if (message.type === "audio") {
            this.playing = true;
            started.resolve();
          }
          if (message.type === "stop") stopping.resolve(message);
        }
        report(value) {
          this.emit("message", Buffer.from(JSON.stringify(value)), false);
        }
        close() {
          this.closed = true;
          this.emit("close");
        }
      }
      const socket = new Socket();
      const audio = new BrowserAudio(socket);
      const open = audio.open.bind(audio);
      audio.open = (ref) => {
        const output = open(ref),
          completion = output.completion;
        output.completion = (signal) => {
          receiptSignal = signal;
          return completion(signal);
        };
        return output;
      };
      const source = {
        read(signal) {
          return new Promise((resolve, reject) => {
            const abort = () => reject(signal.reason);
            signal.addEventListener("abort", abort, { once: true });
            finishInput = () => {
              signal.removeEventListener("abort", abort);
              resolve(null);
            };
          });
        },
      };
      const running = runConversation(session, {
        source,
        playback: audio,
        signal: controller.signal,
      });
      let settled = false;
      void running.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      t.after(async () => {
        audio.close();
        controller.abort();
        await running.catch(() => {});
        await client.close();
        await peer.close();
      });
      peer.emit("response.created", { response: { id: "reply" } });
      peer.message("reply", "reply");
      peer.emit("response.audio.delta", {
        response_id: "reply",
        item_id: "reply",
        output_index: 0,
        content_index: 0,
        delta: Buffer.alloc(960).toString("base64"),
      });
      await started.promise;
      if (reason === "eof") finishInput();
      else if (reason === "closed") await session.close();
      else controller.abort();
      const stop = await stopping.promise;
      assert.equal(socket.playing, true);
      assert.equal(settled, false);
      assert.equal(receiptSignal.aborted, false);
      socket.report({ type: "stopped", id: "wrong-request" });
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(settled, false);
      socket.playing = false;
      socket.report({
        type: "played",
        id: socket.outputId,
        interrupted: true,
        playedMs: 3,
      });
      socket.report({ type: "stopped", id: stop.id });
      if (reason === "cancelled")
        await assert.rejects(
          running,
          (error) => error === controller.signal.reason,
        );
      else await running;
      assert.equal(socket.playing, false);
      assert.equal(
        socket.closed,
        false,
        "Caller still owns the adapter resource",
      );
    },
  );
}
