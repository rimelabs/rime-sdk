import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { AudioFormat, Converter } from "../dist/audio.js";

const fixture = JSON.parse(
  fs.readFileSync(new URL("../../conformance/mulaw.json", import.meta.url)),
);

test("mu-law streaming constant samples match ITU quantization boundaries", () => {
  for (const { pcm, mulaw } of fixture.samples) {
    const converter = new Converter(AudioFormat.MULAW_8000);
    const input = Buffer.alloc(96 * 2);
    for (let i = 0; i < 96; i++) input.writeInt16LE(pcm, i * 2);
    const encoded = Buffer.concat([
      converter.process(input),
      converter.process(Buffer.alloc(0), true),
    ]);
    assert.equal(encoded.length, 32);
    // These output samples have all 63 FIR taps inside the constant input.
    assert.deepEqual(
      encoded.subarray(11, 22),
      Buffer.alloc(11, mulaw),
      `PCM ${pcm}`,
    );
  }
});
