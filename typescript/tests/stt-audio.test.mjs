import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { RimeAudioFormatError, RimeInputError } from "../dist/index.js";
import { InputConverter, resolvePCMFormat } from "../dist/pcm.js";
import { InputAudio } from "../dist/stt/audio.js";

const vectors = JSON.parse(
  readFileSync(
    new URL("../../conformance/pcm-input.json", import.meta.url),
    "utf8",
  ),
).vectors;

function pcm(samples) {
  const bytes = Buffer.alloc(samples.length * 2);
  samples.forEach((sample, index) => bytes.writeInt16LE(sample, index * 2));
  return bytes;
}

for (const vector of vectors) {
  const label = `${vector.sampleRate} Hz / ${vector.channels} channels`;
  const format = { sampleRate: vector.sampleRate, channels: vector.channels };
  for (const split of [null, 1, 3, 7]) {
    test(`STT reference PCM ${label} / byte split ${split}`, () => {
      const audio = new InputAudio(format);
      const source = pcm(vector.input);
      const chunks = [];
      const step = split ?? source.length;
      for (let offset = 0; offset < source.length; offset += step)
        chunks.push(...audio.feed(source.subarray(offset, offset + step)));
      audio.finish();
      assert.deepEqual(Buffer.concat(chunks), pcm(vector.output));
      assert.ok(
        chunks.every(
          (chunk) =>
            chunk.length > 0 && chunk.length <= 65536 && chunk.length % 2 === 0,
        ),
      );
    });
  }

  test(`independent interleaved STT input state: ${label}`, () => {
    const first = new InputAudio(format),
      second = new InputAudio(format);
    const source = pcm(vector.input);
    const firstOutput = [],
      secondOutput = [];
    for (let offset = 0; offset < source.length; offset += 3) {
      const part = source.subarray(offset, offset + 3);
      firstOutput.push(...first.feed(part));
      secondOutput.push(...second.feed(Buffer.alloc(part.length)));
    }
    first.finish();
    second.finish();
    assert.deepEqual(Buffer.concat(firstOutput), pcm(vector.output));
    assert.deepEqual(
      Buffer.concat(secondOutput),
      Buffer.alloc(vector.output.length * 2),
    );
  });

  test(`converter snapshot preserves previous state: ${label}`, () => {
    const source = pcm(vector.input);
    const boundary = 7 * 2 * format.channels;
    const previous = new InputConverter(resolvePCMFormat(format));
    const prefix = previous.process(source.subarray(0, boundary));
    const attempted = previous.clone();
    const tail = attempted.process(source.subarray(boundary));
    assert.deepEqual(previous.process(source.subarray(boundary)), tail);
    assert.deepEqual(Buffer.concat([prefix, tail]), pcm(vector.output));
  });

  test(`large STT source has bounded messages: ${label}`, () => {
    const audio = new InputAudio(format);
    const frames = 480000;
    const chunks = [
      ...audio.feed(pcm(Array(frames * format.channels).fill(1234))),
    ];
    audio.finish();
    const outputFrames =
      Math.floor(((frames - 1) * 16000) / format.sampleRate) + 1;
    assert.deepEqual(
      Buffer.concat(chunks),
      pcm(Array(outputFrames).fill(1234)),
    );
    assert.ok(
      chunks.every(
        (chunk) =>
          chunk.length > 0 && chunk.length <= 65536 && chunk.length % 2 === 0,
      ),
    );
  });
}

for (const channels of [1, 2]) {
  test(`carry incomplete frames but refuse truncated EOF: ${channels} channels`, () => {
    for (let length = 1; length < 2 * channels; length++) {
      const audio = new InputAudio({ channels });
      assert.deepEqual([...audio.feed(Buffer.alloc(length))], []);
      assert.throws(() => audio.finish(), RimeAudioFormatError);
      assert.throws(() => [...audio.feed(Buffer.alloc(1))], RimeInputError);
    }
    const audio = new InputAudio({ channels });
    assert.deepEqual([...audio.feed(Buffer.alloc(1))], []);
    assert.deepEqual(
      [...audio.feed(Buffer.alloc(2 * channels - 1))],
      [Buffer.alloc(2)],
    );
    audio.finish();
  });
}

test("refuse non-byte chunks and unsupported formats", () => {
  for (const value of [null, "audio", [0, 0], new Int16Array(2)])
    assert.throws(
      () => [...new InputAudio().feed(value)],
      RimeAudioFormatError,
    );
  for (const format of [
    null,
    "pcm_s16le",
    16000,
    true,
    [],
    { sampleRate: 44100 },
    { channels: 0 },
    { channels: true },
    { channels: 1.5 },
    { encoding: "mulaw" },
  ])
    assert.throws(() => new InputAudio(format), RimeAudioFormatError);
});

test("omitted and empty PCM format objects retain default input conversion", () => {
  for (const format of [undefined, {}]) {
    const audio = new InputAudio(format);
    assert.deepEqual([...audio.feed(pcm([1234, -4321]))], [pcm([1234, -4321])]);
    audio.finish();
  }
});

test("empty chunks do not flush pending bytes or add samples", () => {
  const audio = new InputAudio();
  assert.deepEqual([...audio.feed(Buffer.alloc(0))], []);
  assert.deepEqual([...audio.feed(Buffer.from([1]))], []);
  assert.deepEqual([...audio.feed(Buffer.alloc(0))], []);
  assert.deepEqual([...audio.feed(Buffer.from([0]))], [Buffer.from([1, 0])]);
  audio.finish();
  audio.finish();
  assert.throws(() => [...audio.feed(Buffer.alloc(0))], RimeInputError);
  new InputAudio().finish();
});

test("subarray offsets and pending bytes do not retain caller mutations", () => {
  const audio = new InputAudio();
  const source = Uint8Array.from([99, 1, 0, 2, 99]);
  assert.deepEqual(
    [...audio.feed(source.subarray(1, 4))],
    [Buffer.from([1, 0])],
  );
  source.fill(88);
  assert.deepEqual(
    [...audio.feed(Uint8Array.from([0]))],
    [Buffer.from([2, 0])],
  );
  audio.finish();
});

test("preparation is lazy and bounds each conversion", (t) => {
  const original = InputConverter.prototype.process;
  const convertedBytes = [];
  t.mock.method(InputConverter.prototype, "process", function (data) {
    convertedBytes.push(data.length);
    return original.call(this, data);
  });
  const audio = new InputAudio();
  const source = Buffer.alloc(1000000);
  const output = audio.feed(source);
  assert.deepEqual(convertedBytes, []);
  assert.ok(output.next().value.length);
  assert.ok(
    convertedBytes.reduce((sum, value) => sum + value, 0) < source.length,
  );
  for (const _ of output) {
    /* Drain without retaining output. */
  }
  audio.finish();
  assert.equal(
    convertedBytes.reduce((sum, value) => sum + value, 0),
    source.length,
  );
  assert.ok(Math.max(...convertedBytes) <= 16384);
});
