import test from "node:test";
import assert from "node:assert/strict";
import { ByteQueue } from "../dist/tts/queue.js";

for (const [limit, chunkSize] of [
  [6, 2],
  [5, 3],
  [3, 8],
]) {
  test(
    `large input is bounded and split in order / ${limit}, ${chunkSize}`,
    { timeout: 1000 },
    async () => {
      const queue = new ByteQueue(limit, chunkSize);
      const data = Buffer.from(Array.from({ length: 23 }, (_, i) => i));
      const writer = queue.put(data).then(() => queue.finish());
      const chunks = [];
      try {
        for (;;) {
          const result = await queue.get();
          if (result.done) break;
          chunks.push(result.value);
          assert.ok(queue.size <= limit);
        }
        await writer;
      } finally {
        queue.finish();
        await writer;
      }
      assert.deepEqual(Buffer.concat(chunks), data);
      assert.ok(
        chunks.every(
          (part) =>
            part.length > 0 && part.length <= Math.min(limit, chunkSize),
        ),
      );
      assert.equal(queue.hasPendingOutput, false);
    },
  );
}

test(
  "pending output includes a writer waiting to resume",
  { timeout: 1000 },
  async () => {
    const queue = new ByteQueue(4, 4);
    const writer = queue.put(Buffer.from("abcdefgh"));
    try {
      assert.equal(queue.size, 4);
      // get() removes the bytes synchronously, before the producer resumes.
      const first = queue.get();
      assert.equal(queue.size, 0);
      assert.equal(queue.hasPendingOutput, true);
      assert.deepEqual((await first).value, Buffer.from("abcd"));
      await writer;
      assert.deepEqual((await queue.get()).value, Buffer.from("efgh"));
      assert.equal(queue.hasPendingOutput, false);
    } finally {
      queue.finish();
      await writer;
    }
  },
);

test(
  "empty input does not yield an empty chunk",
  { timeout: 1000 },
  async () => {
    const queue = new ByteQueue(4, 2);
    await queue.put(Buffer.alloc(0));
    assert.equal(queue.size, 0);
    assert.equal(queue.hasPendingOutput, false);
    let settled = false;
    const reader = queue.get().then((result) => {
      settled = true;
      return result;
    });
    await Promise.resolve();
    assert.equal(settled, false);
    queue.finish();
    assert.equal((await reader).done, true);
  },
);

test("finish drains and ignores further output", async () => {
  const queue = new ByteQueue(4, 2);
  await queue.put(Buffer.from("abcd"));
  queue.finish();
  queue.finish();
  await queue.put(Buffer.from("ignored"));
  assert.deepEqual((await queue.get()).value, Buffer.from("ab"));
  assert.deepEqual((await queue.get()).value, Buffer.from("cd"));
  assert.equal((await queue.get()).done, true);
});

for (const finishFirst of [false, true]) {
  test(`failure discards output and cannot be overwritten / finish first=${finishFirst}`, async () => {
    const queue = new ByteQueue(4, 2);
    await queue.put(Buffer.from("abcd"));
    if (finishFirst) queue.finish();
    const error = new Error("output failed");
    queue.fail(error);
    queue.finish();
    queue.fail(new Error("later failure"));
    await queue.put(Buffer.from("ignored"));
    assert.equal(queue.size, 0);
    assert.equal(queue.hasPendingOutput, false);
    for (let i = 0; i < 2; i++)
      await assert.rejects(queue.get(), (caught) => caught === error);
  });
}

for (const fail of [false, true]) {
  test(
    `termination wakes blocked writer / failure=${fail}`,
    { timeout: 1000 },
    async () => {
      const queue = new ByteQueue(4, 4);
      const writer = queue.put(Buffer.from("abcdefgh"));
      assert.equal(queue.size, 4);
      if (fail) queue.fail(new Error("cancelled"));
      else queue.finish();
      await writer;
      if (fail) {
        assert.equal(queue.size, 0);
        await assert.rejects(queue.get(), /cancelled/);
      } else {
        assert.deepEqual((await queue.get()).value, Buffer.from("abcd"));
        assert.equal((await queue.get()).done, true);
      }
      assert.equal(queue.hasPendingOutput, false);
    },
  );
}

test("failure wakes blocked reader", { timeout: 1000 }, async () => {
  const queue = new ByteQueue(4, 2);
  const reader = queue.get();
  const error = new Error("cancelled");
  queue.fail(error);
  await assert.rejects(reader, (caught) => caught === error);
});

for (const [limit, chunkSize] of [
  [0, 1],
  [1, 0],
  [-1, 1],
  [1, -1],
  [Infinity, 1],
  [1, 1.5],
]) {
  test(`invalid limits fail at construction / ${limit}, ${chunkSize}`, () => {
    assert.throws(() => new ByteQueue(limit, chunkSize), RangeError);
  });
}
