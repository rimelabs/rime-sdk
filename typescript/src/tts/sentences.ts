import { createRequire } from "node:module";
import { RimeInputError, RimeResourceLimitError } from "../errors.js";
const require = createRequire(import.meta.url);
interface BlingFire {
  ready: Promise<BlingFire>;
  _malloc(n: number): number;
  _free(p: number): void;
  _TextToSentences(
    input: number,
    length: number,
    output: number,
    capacity: number,
  ): number;
  HEAPU8: Uint8Array;
}
const module = require("../../vendor/blingfire.cjs") as BlingFire;
export const ready = module.ready;
// These marks schedule scans. Only BlingFire decides sentence boundaries.
// Keep the set and code-point counts in sync with Python's _sentences.py.
const scanTriggers = new Set(
  "\r\n.!?\u01c3\u06d4\u061f\u2024\u2026\u203c\u203d\u2048\u2049" +
    "\u2404\ufe52\uff0e\uff61\u3002\uff1f\uff01\u2028\u2029\u00bf\u00a1",
);
const lookahead = 16,
  scanInterval = 1024,
  contextSize = 128;
function sentenceEnds(text: string): number[] {
  if (!text.trim()) return [];
  const input = Buffer.from(text),
    capacity = input.length * 3 + 16;
  const source = module._malloc(input.length + 1),
    output = module._malloc(capacity);
  let normalized: string;
  try {
    module.HEAPU8.set(input, source);
    module.HEAPU8[source + input.length] = 0;
    const length = module._TextToSentences(
      source,
      input.length,
      output,
      capacity,
    );
    if (length < 0 || length > capacity)
      throw new RimeInputError("Sentence detection failed");
    normalized = Buffer.from(module.HEAPU8.subarray(output, output + length))
      .toString("utf8")
      .replace(/\0+$/, "");
  } finally {
    module._free(source);
    module._free(output);
  }
  let position = 0;
  const ends: number[] = [];
  for (const sentence of normalized.split("\n")) {
    if (!sentence.trim()) continue;
    for (const char of sentence) {
      if (/\s/u.test(char)) continue;
      // BlingFire removes zero-width spaces, direction marks, and BOM at
      // boundaries. Match first to preserve marks retained inside a sentence.
      while (
        position < text.length &&
        text[position] !== char &&
        /[\s\u200b\u200e\u200f\ufeff]/u.test(text[position]!)
      )
        position++;
      const original = String.fromCodePoint(text.codePointAt(position) ?? 0);
      if (original !== char)
        throw new RimeInputError(
          "Sentence detector could not preserve source offsets",
        );
      position += char.length;
    }
    ends.push(position);
  }
  return ends;
}
export class SentenceBuffer {
  private pending = "";
  private committed = 0;
  private parts: string[] = [];
  private untilScan = scanInterval;
  private sincePunctuation = lookahead;
  private highSurrogate = "";
  constructor(private limit: number) {}
  *feed(fragment: string, final = false): Generator<string> {
    fragment = this.highSurrogate + fragment;
    this.highSurrogate = "";
    if (!final && /[\uD800-\uDBFF]$/.test(fragment)) {
      this.highSurrogate = fragment.slice(-1);
      fragment = fragment.slice(0, -1);
    }
    for (const char of fragment) {
      this.parts.push(char);
      this.untilScan--;
      this.sincePunctuation = Math.min(this.sincePunctuation + 1, lookahead);
      if (scanTriggers.has(char)) {
        this.sincePunctuation = 0;
        this.untilScan = Math.min(this.untilScan, lookahead);
      }
      if (this.untilScan === 0) yield* this.scan(false);
    }
    if (final) yield* this.scan(true);
  }
  private *scan(final: boolean): Generator<string> {
    this.pending += this.parts.join("");
    this.parts = [];
    if (!this.pending) return;
    const ends = sentenceEnds(this.pending);
    if (final && ends.length) ends[ends.length - 1] = this.pending.length;
    // Retained lookahead can contain multiple separately limited sentences.
    let start = this.committed;
    for (const end of [...ends, this.pending.length]) {
      if (end <= start) continue;
      if (Buffer.byteLength(this.pending.slice(start, end)) > this.limit)
        throw new RimeResourceLimitError(
          "Sentence exceeds the supported byte limit",
        );
      start = end;
    }
    const committedEnds = final
      ? ends
      : ends
          .slice(0, -1)
          .filter(
            (end) => [...this.pending.slice(end).trim()].length >= lookahead,
          );
    for (const end of committedEnds) {
      if (end <= this.committed) continue;
      const sentence = this.pending.slice(this.committed, end);
      this.committed = end;
      if (sentence.trim()) yield sentence;
    }
    if (final) {
      const residual = this.pending.slice(this.committed);
      if (residual.trim()) yield residual;
      this.pending = "";
      this.committed = 0;
      this.untilScan = scanInterval;
      this.sincePunctuation = lookahead;
      return;
    }
    this.untilScan =
      this.sincePunctuation < lookahead ? lookahead : scanInterval;
    for (const end of ends) {
      if (this.committed < end && end < this.pending.length) {
        const needed = Math.max(
          1,
          lookahead - [...this.pending.slice(end).trimStart()].length,
        );
        this.untilScan = Math.min(this.untilScan, needed);
        break;
      }
    }
    // Bound history by code points, without cutting a UTF-16 surrogate pair.
    const context = [...this.pending.slice(0, this.committed)]
      .slice(-contextSize)
      .join("");
    this.pending = context + this.pending.slice(this.committed);
    this.committed = context.length;
  }
}
