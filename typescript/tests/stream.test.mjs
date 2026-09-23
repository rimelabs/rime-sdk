import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

test("completed source reads are released before the stream stops", async () => {
  const result = await promisify(execFile)(
    process.execPath,
    [
      "--expose-gc",
      fileURLToPath(
        new URL("./fixtures/source-retention.mjs", import.meta.url),
      ),
    ],
    { timeout: 10000 },
  );
  assert.equal(result.stderr, "");
});
