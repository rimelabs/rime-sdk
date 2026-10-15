import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import Ajv from "ajv";

const read = (name) =>
  JSON.parse(
    readFileSync(
      new URL(`../../conformance/tts-compat/${name}`, import.meta.url),
    ),
  );
const contract = read("contract.json");
const schema = new Ajv({ allErrors: true }).compile(read("schema.json"));
const languages = ["go", "python", "rust", "typescript"];
const families = Array.from({ length: 14 }, (_, i) =>
  `C${i + 1}`.replace(/^C(\d)$/, "C0$1"),
);

function validatePresence(value) {
  if (!value || typeof value !== "object") return;
  if (Object.hasOwn(value, "state")) {
    assert.ok(["omitted", "value", "clear"].includes(value.state));
    assert.equal(Object.hasOwn(value, "value"), value.state === "value");
  }
  for (const child of Object.values(value)) validatePresence(child);
}

function validate(value) {
  assert.ok(schema(value), JSON.stringify(schema.errors));
  assert.deepEqual([...value.languages].sort(), languages);
  assert.deepEqual(Object.keys(value.families).sort(), families);
  const inventory = new Map(value.inventory.map((item) => [item.id, item]));
  assert.equal(inventory.size, value.inventory.length);
  for (const item of inventory.values()) {
    assert.ok(Object.hasOwn(value.sources, item.source));
  }
  const ids = new Set();
  const covered = new Set();
  const seenFamilies = new Set();
  for (const entry of value.cases) {
    assert.ok(!ids.has(entry.id), "duplicate case ID");
    ids.add(entry.id);
    assert.ok(Object.hasOwn(value.baselines, entry.baseline));
    for (const family of entry.families) {
      assert.ok(families.includes(family));
      seenFamilies.add(family);
    }
    for (const source of entry.sources) {
      assert.ok(Object.hasOwn(value.sources, source));
    }
    for (const gate of entry.gates) {
      assert.ok(Object.hasOwn(value.gates, gate));
    }
    for (const id of entry.covers) {
      assert.ok(inventory.has(id), "unknown inventory reference");
      assert.equal(inventory.get(id).baseline, entry.baseline);
      covered.add(id);
    }
    if (entry.status === "unresolved") {
      assert.ok(
        entry.gates.some((gate) => value.gates[gate].status === "open"),
      );
      assert.ok(entry.expected.every((item) => item.op === "pending"));
    } else {
      assert.ok(entry.expected.every((item) => item.op !== "pending"));
    }
    validatePresence(entry.given);
  }
  assert.deepEqual([...covered].sort(), [...inventory.keys()].sort());
  assert.deepEqual([...seenFamilies].sort(), families);
}

test("T0 compatibility inventory is complete without claiming SDK conformance", () => {
  validate(contract);
});

for (const mutation of read("invalid.json")) {
  test(`T0 rejects ${mutation.id}`, () => {
    const invalid = structuredClone(contract);
    const path = mutation.path.split("/").slice(1);
    let parent = invalid;
    for (const key of path.slice(0, -1)) parent = parent[key];
    parent[path.at(-1)] = mutation.value;
    assert.throws(() => validate(invalid));
  });
}
