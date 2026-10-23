import { test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Rime,
  runConversation,
  RimeInputError,
  RimeResourceLimitError,
  RimeStreamError,
} from "../dist/index.js";
import { Peer } from "./realtime-peer.mjs";

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  void promise.catch(() => {});
  return { promise, resolve, reject };
}
function wait(promise, signal) {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}
class Source {
  next = deferred();
  waiting = 0;
  async read(signal) {
    this.waiting++;
    try {
      const value = await wait(this.next.promise, signal);
      this.next = deferred();
      return value;
    } finally {
      this.waiting--;
      if (this.releaseError) throw this.releaseError;
    }
  }
}
class Output {
  done = deferred();
  writes = [];
  ended = false;
  interrupted = false;
  waiting = 0;
  write(chunk) {
    if (this.writeError) throw this.writeError;
    assert.equal(this.ended || this.interrupted, false);
    this.writes.push(chunk);
  }
  end() {
    this.ended = true;
  }
  interrupt() {
    this.interrupted = true;
    this.writes = [];
  }
  async completion(signal) {
    this.waiting++;
    try {
      return await wait(this.done.promise, signal);
    } finally {
      this.waiting--;
    }
  }
}
class Playback {
  outputs = new Map();
  stopped = false;
  batches = [];
  open(ref) {
    const output = new Output();
    this.outputs.set(ref.itemId, output);
    return output;
  }
  interrupt(outputs) {
    this.batches.push(outputs);
    for (const ref of outputs) this.outputs.get(ref.itemId).interrupt();
  }
  async stop() {
    this.stopped = true;
    for (const output of this.outputs.values()) output.interrupt();
    if (this.stopError) throw this.stopError;
  }
}

for (const trigger of ["speech", "abandoned"]) {
  test(
    `interruption batches dependencies and preserves other turns: ${trigger}`,
    { timeout: 4000 },
    async () => {
      const refs = Object.fromEntries(
        ["root", "child", "other", "grandchild", "late", "new"].map(
          (responseId) => [responseId, { sessionId: "s", responseId }],
        ),
      );
      const source = new Source(),
        playback = new Playback(),
        consumed = deferred(),
        closed = deferred();
      const session = {
        info: { interruptOnSpeech: true },
        async close() {
          closed.resolve();
        },
        events: (async function* () {
          for (const [name, parent] of [
            ["root"],
            ["child", "root"],
            ["other"],
            ["grandchild", "child"],
          ]) {
            yield {
              payload: {
                kind: "response.started",
                response: refs[name],
                parent: refs[parent],
              },
            };
            yield {
              payload: {
                kind: "message.started",
                output: {
                  response: refs[name],
                  itemId: name,
                  outputIndex: 0,
                  contentIndex: 0,
                },
              },
            };
          }
          yield {
            payload:
              trigger === "speech"
                ? { kind: "speech.started" }
                : { kind: "response.abandoned", response: refs.root },
          };
          for (const [name, parent] of [["late", "grandchild"], ["new"]]) {
            yield {
              payload: {
                kind: "response.started",
                response: refs[name],
                parent: refs[parent],
              },
            };
            const output = {
              response: refs[name],
              itemId: name,
              outputIndex: 0,
              contentIndex: 0,
            };
            yield { payload: { kind: "message.started", output } };
            yield {
              payload: {
                kind: "audio.delta",
                output,
                audio: { data: new Uint8Array(960) },
              },
            };
          }
          consumed.resolve();
          await closed.promise;
        })(),
      };
      const running = runConversation(session, { source, playback });
      try {
        await consumed.promise;
        const expected =
          trigger === "speech"
            ? ["root", "child", "other", "grandchild"]
            : ["root", "child", "grandchild"];
        assert.deepEqual(
          playback.batches.map((batch) => batch.map((ref) => ref.itemId)),
          [expected, ["late"]],
        );
        assert.equal(
          playback.outputs.get("other").interrupted,
          trigger === "speech",
        );
        assert.equal(playback.outputs.get("late").writes.length, 0);
        assert.equal(playback.outputs.get("new").writes.length, 1);
      } finally {
        source.next.resolve(null);
        await running;
      }
    },
  );
}
async function until(condition) {
  const deadline = Date.now() + 1500;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("Condition did not become true");
    await sleep(1);
  }
}
async function setup(
  t,
  { enabled = true, callback, revision, beforeRun } = {},
) {
  const peer = await new Peer().start();
  peer.autoInitialize = false;
  peer.onRequest = (request) => {
    if (request.type === "session.update") {
      peer.emit("session.updated", {
        prism_request_event_id: request.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prism_effective_interrupt_response: enabled,
          ...(revision === undefined
            ? {}
            : { prism_settings_revision: revision }),
        },
      });
    } else if (peer.waiters.length) peer.waiters.shift()(request);
    else peer.queue.push(request);
  };
  const client = new Rime({ apiKey: "test" });
  const session = await client.realtime.connect({ endpoint: peer.endpoint });
  await beforeRun?.(session, peer);
  const source = new Source(),
    playback = new Playback(),
    controller = new AbortController();
  const events = [];
  const running = runConversation(session, {
    source,
    playback,
    signal: controller.signal,
    onEvent(event) {
      events.push(event.payload);
      return callback?.(event);
    },
  });
  void running.catch(() => {});
  t.after(async () => {
    controller.abort();
    await running.catch(() => {});
    assert.equal(source.waiting, 0);
    assert.equal(playback.stopped, true);
    assert.ok(
      [...playback.outputs.values()].every((output) => output.waiting === 0),
    );
    // Buffered events can remain, but the iterator must terminate after cleanup.
    try {
      for await (const event of session.events) {
        assert.ok(event.payload);
      }
    } catch {
      /* A failed session retains its terminal error. */
    }
    await client.close();
    await peer.close();
  });
  return { peer, session, source, playback, controller, events, running };
}
function audio(peer, response = "first", item = response, index = 0) {
  peer.emit("response.audio.delta", {
    response_id: response,
    item_id: item,
    output_index: index,
    content_index: 0,
    delta: Buffer.alloc(960).toString("base64"),
  });
}
function begin(peer, id = "first", withAudio = true) {
  peer.emit("response.created", { response: { id } });
  peer.message(id, id);
  if (withAudio) audio(peer, id);
}
function speech(peer) {
  peer.emit("input_audio_buffer.speech_started", {
    item_id: "user",
    audio_start_ms: 0,
  });
}
function ack(peer, request) {
  peer.emit("conversation.item.truncated", {
    item_id: request.item_id,
    content_index: 0,
    audio_end_ms: request.audio_end_ms,
    prism_request_event_id: request.event_id,
  });
}
function reports(peer) {
  return peer.requests.filter((r) =>
    ["prism.playback.drained", "conversation.item.truncate"].includes(r.type),
  );
}

test(
  "input and transcripts continue during measured output drain",
  { timeout: 4000 },
  async (t) => {
    const { peer, source, playback, events, running } = await setup(t);
    begin(peer);
    peer.ended("first");
    await until(() => playback.outputs.get("first")?.ended);
    assert.deepEqual(reports(peer), []);
    source.next.resolve({
      data: Buffer.alloc(3840),
      format: { sampleRate: 48000, channels: 1 },
    });
    assert.equal(
      Buffer.from(
        (await peer.next("input_audio_buffer.append")).audio,
        "base64",
      ).length,
      1280,
    );
    peer.emit("conversation.item.input_audio_transcription.completed", {
      item_id: "user",
      transcript: "hello",
    });
    await until(() => events.some((e) => e.kind === "transcript.final"));
    assert.deepEqual(reports(peer), []);
    playback.outputs.get("first").done.resolve(13.5);
    const report = await peer.next("prism.playback.drained");
    assert.equal(report.played_ms, 13.5);
    source.next.resolve(null);
    await running;
  },
);

for (const stage of ["before_message", "before_audio", "draining"]) {
  test(
    `interruption at ${stage}; late audio and old receipts cannot stop new output`,
    { timeout: 4000 },
    async (t) => {
      const { peer, playback, events } = await setup(t);
      peer.emit("response.created", { response: { id: "first" } });
      if (stage !== "before_message") peer.message("first", "first");
      if (stage === "draining") {
        audio(peer);
        peer.ended("first");
      }
      speech(peer);
      if (stage === "before_message") peer.message("first", "first");
      audio(peer);
      if (stage !== "draining") peer.ended("first", "cancelled");
      begin(peer, "second");
      peer.ended("second");
      await until(() => playback.outputs.get("second")?.ended);
      const first = playback.outputs.get("first"),
        second = playback.outputs.get("second");
      assert.ok(first.interrupted);
      assert.deepEqual(first.writes, []);
      assert.equal(second.interrupted, false);
      assert.equal(second.writes.length, 1);
      second.done.resolve(19);
      await sleep(10);
      assert.deepEqual(reports(peer), []);
      first.done.resolve(stage === "draining" ? 7 : 0);
      await until(() => reports(peer).length === 2);
      const report = reports(peer).find(
        (value) => value.type === "prism.playback.drained",
      );
      assert.equal(report.response_id, "second");
      assert.equal(report.played_ms, 19);
      const cut = reports(peer).find(
        (value) => value.type === "conversation.item.truncate",
      );
      assert.equal(cut.item_id, "first");
      assert.equal(cut.audio_end_ms, stage === "draining" ? 7 : 0);
      ack(peer, cut);
      audio(peer);
      await until(
        () =>
          events.filter((e) => e.kind === "audio.delta").length ===
          (stage === "draining" ? 4 : 3),
      );
      assert.equal(second.interrupted, false);
    },
  );
}

for (const [enabled, revision] of [
  [true, undefined],
  [false, undefined],
  [true, 1],
  [false, 1],
]) {
  test(
    `effective server interruption setting ${enabled}, revision ${revision}; hints do not interrupt`,
    { timeout: 4000 },
    async (t) => {
      const { session, peer, playback, events } = await setup(t, {
        enabled,
        revision,
      });
      assert.equal(session.info.interruptOnSpeech, enabled);
      begin(peer);
      peer.emit("prism.input_audio_buffer.speech_hint", { audio_start_ms: 0 });
      await until(() => events.some((e) => e.kind === "speech.hint"));
      const output = playback.outputs.get("first");
      assert.equal(output.interrupted, false);
      speech(peer);
      await until(() => events.some((e) => e.kind === "speech.started"));
      assert.equal(output.interrupted, enabled);
      peer.ended("first");
      output.done.resolve(17);
      const report = await peer.next(
        enabled ? "conversation.item.truncate" : "prism.playback.drained",
      );
      if (enabled) ack(peer, report);
      else assert.equal(report.played_ms, 17);
    },
  );
}

test(
  "separate output receipts are summed only after all outputs drain",
  { timeout: 4000 },
  async (t) => {
    const { peer, playback } = await setup(t);
    begin(peer);
    peer.message("first", "another", 1);
    audio(peer, "first", "another", 1);
    peer.ended("first");
    await until(() => playback.outputs.has("another"));
    playback.outputs.get("first").done.resolve(5);
    await sleep(0);
    assert.deepEqual(reports(peer), []);
    playback.outputs.get("another").done.resolve(8);
    assert.equal((await peer.next("prism.playback.drained")).played_ms, 13);
  },
);

for (const stage of ["input", "output", "drain", "report"]) {
  test(
    `abort during ${stage} settles all runner operations`,
    { timeout: 4000 },
    async (t) => {
      const { peer, source, playback, controller, running } = await setup(t);
      await until(() => source.waiting > 0);
      if (stage !== "input") {
        begin(peer);
        await until(() => playback.outputs.has("first"));
        if (stage === "drain") {
          peer.ended("first");
          await until(() => playback.outputs.get("first").ended);
        }
        if (stage === "report") {
          speech(peer);
          playback.outputs.get("first").done.resolve(3);
          await peer.next("conversation.item.truncate");
        }
      }
      const reason = new Error("cancelled by caller");
      controller.abort(reason);
      await assert.rejects(running, (error) => error === reason);
    },
  );
}
for (const mode of ["eof", "local_close", "remote_close"]) {
  test(
    `${mode} stops pending input and playback`,
    { timeout: 4000 },
    async (t) => {
      const { session, peer, source, playback, running } = await setup(t);
      begin(peer);
      await until(() => playback.outputs.has("first"));
      if (mode === "eof") source.next.resolve(null);
      else if (mode === "local_close") await session.close();
      else peer.socket.close();
      if (mode === "remote_close") await assert.rejects(running, /closed/);
      else await running;
    },
  );
}

for (const mode of [
  "input",
  "write",
  "receipt",
  "invalid",
  "callback",
  "overflow",
]) {
  test(
    `${mode} failure reaches caller even if cleanup fails`,
    { timeout: 4000 },
    async (t) => {
      const original = new Error("original failure");
      const { peer, source, playback, running } = await setup(t, {
        callback() {
          if (mode === "callback") throw original;
        },
      });
      playback.stopError = new Error("cleanup failure");
      if (mode === "input") source.next.reject(original);
      else if (mode === "overflow") {
        for (let i = 0; i < 65; i++)
          peer.emit("response.created", { response: { id: String(i) } });
      } else {
        begin(peer, "first", false);
        if (mode !== "callback") {
          await until(() => playback.outputs.has("first"));
          const output = playback.outputs.get("first");
          if (mode === "write") {
            output.writeError = original;
            audio(peer);
          } else if (mode === "receipt") output.done.reject(original);
          else output.done.resolve(NaN);
        }
      }
      await assert.rejects(running, (error) =>
        mode === "invalid"
          ? error instanceof RimeInputError
          : mode === "overflow"
            ? error instanceof RimeResourceLimitError
            : error === original,
      );
      assert.deepEqual(reports(peer), []);
    },
  );
}

test(
  "forwarded tools can use existing session methods",
  { timeout: 4000 },
  async (t) => {
    const { session, peer, events } = await setup(t);
    peer.emit("response.created", { response: { id: "tool" } });
    peer.tool("tool");
    peer.ended("tool");
    await peer.next("prism.playback.drained");
    const call = events.find((e) => e.kind === "tool.call");
    const submitting = session.submitToolResult(call.call, '{"ok":true}');
    peer.ack(await peer.next("conversation.item.create"));
    await submitting;
    const continuing = session.continueReply(call.call);
    peer.accepted(await peer.next("response.create"), "continued");
    await continuing;
  },
);

test(
  "abort while the public audio send is pending",
  { timeout: 4000 },
  async (t) => {
    const { session, source, controller, running } = await setup(t);
    const gate = deferred();
    const sending = deferred();
    let waiting = false;
    const sendAudio = session.sendAudio.bind(session);
    session.sendAudio = async (chunk, { signal }) => {
      waiting = true;
      sending.resolve();
      try {
        await wait(gate.promise, signal);
        await sendAudio(chunk, { signal });
      } finally {
        waiting = false;
      }
    };
    source.next.resolve({ data: Buffer.alloc(1280) });
    await sending.promise;
    controller.abort();
    await assert.rejects(running);
    assert.equal(waiting, false);
  },
);

test(
  "audio.done seals playback; generation still gates the finished report",
  { timeout: 4000 },
  async (t) => {
    const { peer, playback, events } = await setup(t);
    begin(peer);
    peer.emit("response.audio.done", {
      response_id: "first",
      item_id: "first",
      output_index: 0,
      content_index: 0,
    });
    await until(() => playback.outputs.get("first")?.ended);
    playback.outputs.get("first").done.resolve(4);
    peer.emit("conversation.item.input_audio_transcription.completed", {
      item_id: "user",
      transcript: "yes",
    });
    await until(() => events.some((e) => e.kind === "transcript.final"));
    assert.deepEqual(reports(peer), []);
    peer.ended("first");
    assert.equal((await peer.next("prism.playback.drained")).played_ms, 4);
  },
);

test(
  "stale interruption refusal is forwarded without stopping the next response",
  { timeout: 4000 },
  async (t) => {
    const { peer, playback, events } = await setup(t);
    begin(peer);
    speech(peer);
    await until(() => playback.outputs.get("first")?.interrupted);
    playback.outputs.get("first").done.resolve(0);
    peer.fault(
      await peer.next("conversation.item.truncate"),
      "truncate_not_current",
    );
    peer.ended("first", "cancelled");
    begin(peer, "second");
    peer.ended("second");
    await until(() => playback.outputs.get("second")?.ended);
    playback.outputs.get("second").done.resolve(9);
    assert.equal(
      (await peer.next("prism.playback.drained")).response_id,
      "second",
    );
    assert.ok(
      events.some(
        (e) => e.kind === "error" && e.error.code === "truncate_not_current",
      ),
    );
  },
);

test("async event callbacks are rejected", { timeout: 4000 }, async (t) => {
  const { peer, running } = await setup(t, { callback: async () => {} });
  begin(peer);
  await assert.rejects(running, /synchronous/);
});

test(
  "receipt failure wins over simultaneous input EOF",
  { timeout: 4000 },
  async (t) => {
    const { peer, source, playback, running } = await setup(t);
    begin(peer);
    await until(() => playback.outputs.has("first"));
    const failure = new Error("Playback disconnected");
    source.next.resolve(null);
    playback.outputs.get("first").done.reject(failure);
    await assert.rejects(running, (error) => error === failure);
  },
);

for (const earlierEnded of [false, true]) {
  test(
    `newer drain waits for earlier playback, not later playback; earlier ended=${earlierEnded}`,
    { timeout: 4000 },
    async (t) => {
      const { peer, source, playback, events } = await setup(t, {
        enabled: false,
      });
      begin(peer, "first");
      if (earlierEnded) peer.ended("first");
      begin(peer, "second");
      peer.ended("second");
      begin(peer, "third");
      await until(() => playback.outputs.has("third"));
      playback.outputs.get("second").done.resolve(8);
      await until(() => playback.outputs.get("second").waiting === 0);
      peer.emit("conversation.item.input_audio_transcription.completed", {
        item_id: "user",
        transcript: "yes",
      });
      await until(() => events.some((e) => e.kind === "transcript.final"));
      source.next.resolve({ data: Buffer.alloc(1280) });
      await peer.next("input_audio_buffer.append");
      assert.deepEqual(reports(peer), []);
      if (!earlierEnded) peer.ended("first");
      playback.outputs.get("first").done.resolve(12);
      const values = [
        await peer.next("prism.playback.drained"),
        await peer.next("prism.playback.drained"),
      ];
      assert.deepEqual(
        Object.fromEntries(
          values.map((value) => [value.response_id, value.played_ms]),
        ),
        { first: 12, second: 8 },
      );
      assert.equal(playback.outputs.get("third").waiting, 1);
    },
  );
}

for (const originalFailure of [false, true]) {
  test(
    `input release errors reach the caller; original failure=${originalFailure}`,
    { timeout: 4000 },
    async (t) => {
      const original = new Error("callback failed");
      const { session, peer, source, running } = await setup(t, {
        callback() {
          if (originalFailure) throw original;
        },
      });
      await until(() => source.waiting === 1);
      const released = new Error("microphone release failed");
      source.releaseError = released;
      if (originalFailure) begin(peer);
      else await session.close();
      await assert.rejects(
        running,
        (error) => error === (originalFailure ? original : released),
      );
    },
  );
}

for (const reason of ["eof", "closed", "cancelled"]) {
  test(
    `shutdown confirms the adapter stop before cancelling receipts: ${reason}`,
    { timeout: 4000 },
    async (t) => {
      const { session, peer, source, playback, controller, running } =
        await setup(t);
      begin(peer);
      await until(() => playback.outputs.get("first")?.waiting === 1);
      const stopping = deferred(),
        confirmed = deferred();
      const stop = playback.stop.bind(playback);
      playback.stop = async (signal) => {
        stopping.resolve();
        await wait(confirmed.promise, signal);
        await stop();
      };
      if (reason === "eof") source.next.resolve(null);
      else if (reason === "closed") await session.close();
      else controller.abort();
      let settled = false;
      void running.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      try {
        await stopping.promise;
        assert.equal(settled, false);
        assert.equal(playback.stopped, false);
        assert.equal(playback.outputs.get("first").waiting, 1);
        await until(() => source.waiting === 0);
      } finally {
        confirmed.resolve();
      }
      if (reason === "cancelled")
        await assert.rejects(
          running,
          (error) => error === controller.signal.reason,
        );
      else await running;
      assert.equal(playback.stopped, true);
      assert.equal(playback.outputs.get("first").waiting, 0);
    },
  );
}

for (const settlement of ["reject", "resolve"]) {
  test(
    `shutdown awaits cancellation cleanup after its deadline: ${settlement}`,
    { timeout: 7000 },
    async (t) => {
      const { peer, source, playback, running } = await setup(t);
      begin(peer);
      await until(() => playback.outputs.has("first"));
      let released = false;
      const stop = playback.stop.bind(playback);
      playback.stop = async (signal) => {
        try {
          await wait(new Promise(() => {}), signal);
        } catch (error) {
          if (settlement === "reject") throw error;
        } finally {
          await sleep(50);
          assert.equal(playback.outputs.get("first").waiting, 1);
          await stop();
          released = true;
        }
      };
      source.next.resolve(null);
      await assert.rejects(running, /Playback shutdown confirmation timed out/);
      assert.equal(released, true);
    },
  );
}

for (const reason of ["eof", "closed", "failure"]) {
  test(
    `caller abort during shutdown is retained: ${reason}`,
    { timeout: 4000 },
    async (t) => {
      const original = new Error("callback failed");
      const { session, peer, source, playback, controller, running } =
        await setup(t, {
          callback(event) {
            if (
              reason === "failure" &&
              event.payload.kind === "transcript.final"
            )
              throw original;
          },
        });
      begin(peer);
      await until(() => playback.outputs.get("first")?.waiting === 1);
      const stopping = deferred(),
        confirmed = deferred();
      const stop = playback.stop.bind(playback);
      playback.stop = async (signal) => {
        stopping.resolve();
        await wait(confirmed.promise, signal);
        await stop();
      };
      if (reason === "eof") source.next.resolve(null);
      else if (reason === "closed") await session.close();
      else
        peer.emit("conversation.item.input_audio_transcription.completed", {
          item_id: "user",
          transcript: "hello",
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
      const aborted = new Error("tool failed during shutdown");
      try {
        await stopping.promise;
        controller.abort(aborted);
        await sleep(0);
        assert.equal(settled, false);
        assert.equal(playback.stopped, false);
        assert.equal(playback.outputs.get("first").waiting, 1);
      } finally {
        confirmed.resolve();
      }
      await assert.rejects(
        running,
        (error) => error === (reason === "failure" ? original : aborted),
      );
      assert.equal(playback.stopped, true);
      assert.equal(playback.outputs.get("first").waiting, 0);
    },
  );
}

for (const reason of ["local_close", "remote_close", "refused"]) {
  test(
    `closure with pending interruption report: ${reason}`,
    { timeout: 4000 },
    async (t) => {
      const { session, peer, playback, running } = await setup(t);
      begin(peer);
      await until(() => playback.outputs.has("first"));
      speech(peer);
      playback.outputs.get("first").done.resolve(3);
      const request = await peer.next("conversation.item.truncate");
      if (reason === "local_close") {
        await session.close();
        await running;
      } else if (reason === "remote_close") {
        peer.socket.close();
        await assert.rejects(running, RimeStreamError);
      } else {
        peer.fault(request, "invalid_request");
        await assert.rejects(running, /invalid_request/);
      }
    },
  );
}

test(
  "unrelated stream error during close is preserved",
  { timeout: 4000 },
  async (t) => {
    const { session, peer, playback, running } = await setup(t);
    begin(peer);
    await until(() => playback.outputs.has("first"));
    const error = new RimeStreamError("Realtime session closed");
    playback.stopError = error;
    await session.close();
    await assert.rejects(running, (value) => value === error);
  },
);

test("close concurrent with playback completion ends normally", async (t) => {
  const { peer, session, playback, running } = await setup(t);
  begin(peer);
  peer.ended("first");
  await until(() => playback.outputs.get("first")?.ended);
  const closing = session.close();
  playback.outputs.get("first").done.resolve(8);
  await closing;
  await running;
});

test("unrelated input error during close is preserved", async (t) => {
  const { session, source, running } = await setup(t);
  await until(() => source.waiting);
  const error = new RimeInputError("The realtime session is closed");
  source.releaseError = error;
  await session.close();
  await assert.rejects(running, (value) => value === error);
});

test(
  "receipt abort without runner cancellation fails the run",
  { timeout: 4000 },
  async (t) => {
    const { peer, playback, controller, running } = await setup(t);
    begin(peer);
    peer.ended("first");
    await until(() => playback.outputs.get("first")?.ended);
    const error = new DOMException("Playback receipt cancelled", "AbortError");
    playback.outputs.get("first").done.reject(error);
    await assert.rejects(running, (value) => value === error);
    assert.equal(controller.signal.aborted, false);
    assert.equal(playback.stopped, true);
    assert.deepEqual(reports(peer), []);
  },
);

for (const ended of [false, true]) {
  for (const [initial, updated, late, replyRevision] of [
    [true, false, false, 1],
    [false, true, false, 1],
    [true, false, true, 1],
    [false, true, true, 1],
    [true, false, true, 2],
    [false, true, true, 2],
    [false, false, true, 1],
  ]) {
    test(`reply policy: ${initial} -> ${updated}, late=${late}, revision=${replyRevision}, ended=${ended}`, async (t) => {
      const { session, peer, playback, events } = await setup(t, {
        enabled: initial,
        revision: 1,
      });
      const beginReply = () => {
        peer.emit("response.created", {
          response: { id: "first", prism_settings_revision: replyRevision },
        });
        peer.message("first", "first");
        audio(peer);
      };
      if (!late) {
        beginReply();
        await until(() => playback.outputs.has("first"));
      }
      peer.onRequest = null;
      const update = session.update({ interruptOnSpeech: updated });
      const q = await peer.next("session.update");
      peer.emit("session.updated", {
        prism_request_event_id: q.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prism_effective_interrupt_response: updated,
          prism_settings_revision: 2,
        },
      });
      await update;
      if (late) {
        beginReply();
        await until(() => playback.outputs.has("first"));
      }
      if (ended) {
        // The device still plays after the server's estimate expires.
        peer.ended("first");
        await until(() => events.some((e) => e.kind === "response.ended"));
      }
      speech(peer);
      await until(() => events.some((e) => e.kind === "speech.started"));
      const expected = replyRevision === 1 ? initial : updated;
      const output = playback.outputs.get("first");
      assert.equal(output.interrupted, expected);
      if (!ended) peer.ended("first");
      output.done.resolve(17);
      const report = await peer.next(
        expected ? "conversation.item.truncate" : "prism.playback.drained",
      );
      if (expected) ack(peer, report);
    });
  }
}

for (const change of [false, true]) {
  test(`interruption history retains policy changes only: change=${change}`, async (t) => {
    const peer = await new Peer().start();
    const client = new Rime({ apiKey: "test" });
    t.after(async () => {
      await client.close();
      await peer.close();
    });
    const session = await client.realtime.connect({ endpoint: peer.endpoint });
    peer.autoInitialize = false;
    for (let revision = 1; revision <= 130; revision++) {
      const enabled = change ? Boolean(revision % 2) : false;
      const update = session.update({ interruptOnSpeech: enabled });
      const q = await peer.next("session.update");
      peer.emit("session.updated", {
        prism_request_event_id: q.event_id,
        session: {
          id: "session-1",
          voice: "test",
          prism_effective_interrupt_response: enabled,
          prism_settings_revision: revision,
        },
      });
      await update;
    }
    peer.emit("response.created", {
      response: { id: "old-guess", prism_settings_revision: 1 },
    });
    const events = session.events;
    if (change) await assert.rejects(events.next(), RimeResourceLimitError);
    else assert.equal((await events.next()).value.payload.settingsRevision, 1);
    await events.return();
  });
}

test("queued reply retains its captured policy after revision history eviction", async (t) => {
  const { session, peer, playback, events } = await setup(t, {
    revision: 1,
    async beforeRun(session, peer) {
      peer.onRequest = null;
      peer.emit("response.created", {
        response: { id: "first", prism_settings_revision: 1 },
      });
      for (let revision = 2; revision <= 130; revision++) {
        const enabled = Boolean(revision % 2);
        const update = session.update({ interruptOnSpeech: enabled });
        const q = await peer.next("session.update");
        peer.emit("session.updated", {
          prism_request_event_id: q.event_id,
          session: {
            id: "session-1",
            voice: "test",
            prism_effective_interrupt_response: enabled,
            prism_settings_revision: revision,
          },
        });
        await update;
      }
      peer.message("first", "first");
      audio(peer);
    },
  });
  assert.equal(session.info.interruptOnSpeech, false);
  await until(() => playback.outputs.has("first"));
  speech(peer);
  await until(() => events.some((e) => e.kind === "speech.started"));
  assert.equal(playback.outputs.get("first").interrupted, true);
  peer.ended("first");
  playback.outputs.get("first").done.resolve(17);
  ack(peer, await peer.next("conversation.item.truncate"));
});

for (const newer of [false, true]) {
  test(`speech preserves a pending finished report; newer reply=${newer}`, async (t) => {
    const { session, peer, playback, events } = await setup(t);
    const started = deferred();
    const release = deferred();
    const report = session.reportPlayback.bind(session);
    t.mock.method(session, "reportPlayback", async (value, options) => {
      if (value.kind === "finished" && value.response.responseId === "first") {
        started.resolve();
        await wait(release.promise, options.signal);
      }
      return report(value, options);
    });
    begin(peer);
    peer.ended("first");
    await until(() => playback.outputs.get("first")?.ended);
    playback.outputs.get("first").done.resolve(15);
    await started.promise;
    if (newer) {
      begin(peer, "second");
      await until(() => playback.outputs.has("second"));
    }
    speech(peer);
    await until(() => events.some((e) => e.kind === "speech.started"));
    assert.equal(playback.outputs.get("first").interrupted, false);
    assert.deepEqual(
      playback.batches.map((batch) => batch.map((ref) => ref.itemId)),
      newer ? [["second"]] : [],
    );
    if (newer) {
      playback.outputs.get("second").done.resolve(4);
      const cut = await peer.next("conversation.item.truncate");
      assert.equal(cut.item_id, "second");
      ack(peer, cut);
    }
    release.resolve();
    const finished = await peer.next("prism.playback.drained");
    assert.equal(finished.response_id, "first");
    assert.equal(finished.played_ms, 15);
    assert.equal(
      reports(peer).filter(
        (r) => r.type === "conversation.item.truncate" && r.item_id === "first",
      ).length,
      0,
    );
  });
}
