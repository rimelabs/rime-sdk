import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { AudioStream, constructionKey } from "../../dist/stream.js";
import { AudioFormat } from "../../dist/audio.js";

const count = 20000;
const results = [];
const processed = Promise.withResolvers();
const stopped = Promise.withResolvers();
const pendingRequest = Promise.withResolvers();
const pendingRead = Promise.withResolvers();
let request = 0;
const native = {
  sourceChars: 1024,
  cleanupTimeout: 0.1,
  start() {},
  waitStopped: () => stopped.promise,
  async inputRequest() {
    if (request++ === count * 2) {
      processed.resolve();
      return pendingRequest.promise;
    }
    return (request - 1) % 2;
  },
  inputReply() {},
  failSource() {
    processed.reject(new Error("Unexpected source failure"));
  },
  read: () => pendingRead.promise,
  acceptRead() {},
  cancel() {
    stopped.resolve();
    pendingRequest.resolve(null);
    pendingRead.resolve({ data: null, ticket: 0 });
  },
};
const source = {
  [Symbol.asyncIterator]() {
    return {
      async next() {
        const result = { value: `text ${results.length}`, done: false };
        results.push(new WeakRef(result));
        return result;
      },
    };
  },
};
const stream = new AudioStream(
  constructionKey,
  { forget() {} },
  native,
  source,
  AudioFormat.PCM_24000,
);
const read = stream.next();
try {
  await processed.promise;
  // Leave the WeakRef creation job and keep the stop promise unresolved.
  for (let i = 0; i < 3; i++) {
    await setImmediate();
    global.gc();
  }
  const retained = results.filter(
    (result) => result.deref() !== undefined,
  ).length;
  assert.ok(
    retained <= 1,
    `${retained} of ${count} completed results retained`,
  );
} finally {
  await stream.cancel();
  await read;
}
