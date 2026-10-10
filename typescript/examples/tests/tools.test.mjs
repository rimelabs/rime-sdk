import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import { RimeRealtimeError } from "@rimelabs/sdk";
import { BrowserAudio } from "../common/browser-audio.ts";
import { converse } from "../realtime/conversation.ts";

for (const scenario of ["completed", "superseded", "refused"]) {
  test(`tool continuation: ${scenario}`, { timeout: 3000 }, async () => {
    const superseded = scenario === "superseded";
    const events = [];
    let wake,
      ended = false;
    const emit = (payload) => {
      events.push({ payload });
      wake?.();
    };
    const results = [],
      continuations = [];
    let nextPlayed;
    const nextPlayback = new Promise((resolve) => {
      nextPlayed = resolve;
    });
    const session = {
      info: { interruptOnSpeech: true },
      events: (async function* () {
        while (!ended) {
          if (events.length) yield events.shift();
          else
            await new Promise((resolve) => {
              wake = resolve;
            });
        }
      })(),
      async sendAudio() {},
      async reportPlayback(report) {
        if (report.response?.responseId === "next") nextPlayed();
      },
      async submitToolResult(call, output) {
        results.push({ call, output });
      },
      async continueReply(ref) {
        continuations.push(ref);
        if (scenario === "refused") {
          // Speech arrives while the continuation request is still pending.
          emit({ kind: "speech.started", itemId: "user", audioStartMs: 0 });
          await sleep(0);
          throw new RimeRealtimeError({
            scope: "event",
            code: "tool_continuation_unavailable",
            message: "The user started another turn",
          });
        }
      },
      async close() {
        ended = true;
        wake?.();
      },
    };
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
                  interrupted: false,
                  playedMs: 0,
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
    let release, started;
    const waiting = new Promise((resolve) => {
      started = resolve;
    });
    const tool = async () => {
      started();
      await new Promise((resolve) => {
        release = resolve;
      });
      return '{"status":"shipped"}';
    };
    const running = converse(session, audio, tool);
    try {
      const response = { sessionId: "s", responseId: "tool" };
      emit({ kind: "response.started", response });
      emit({
        kind: "tool.call",
        call: { ...response, callId: "c" },
        name: "lookup_order",
        arguments: { order_id: "demo-123" },
      });
      emit({
        kind: "response.ended",
        response,
        status: "completed",
        reason: "stop",
      });
      await waiting;
      if (superseded) {
        emit({ kind: "speech.started", itemId: "user", audioStartMs: 0 });
        await sleep(0);
      }
      release();
      await sleep(0);
      assert.equal(results.length, 1);
      assert.equal(continuations.length, superseded ? 0 : 1);
      const next = { sessionId: "s", responseId: "next" };
      emit({ kind: "response.started", response: next });
      emit({
        kind: "response.ended",
        response: next,
        status: "completed",
        reason: "stop",
      });
      await Promise.race([nextPlayback, running]);
      assert.equal(ended, false);
      assert.equal(continuations.length, superseded ? 0 : 1);
    } finally {
      audio.close();
      await running;
    }
  });
}
