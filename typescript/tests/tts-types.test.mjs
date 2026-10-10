import { test } from "node:test";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

test("published TTS declarations distinguish complete and incremental text", () => {
  execFileSync(
    process.execPath,
    [
      fileURLToPath(import.meta.resolve("typescript/bin/tsc")),
      "--strict",
      "--noEmit",
      "--skipLibCheck",
      "--target",
      "ES2022",
      "--module",
      "NodeNext",
      fileURLToPath(new URL("fixtures/tts.ts", import.meta.url)),
    ],
    { stdio: "inherit" },
  );
});
