import { test } from "node:test";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

test("published Realtime declarations support typed consumers", () => {
  execFileSync(
    process.execPath,
    [
      fileURLToPath(
        new URL("../node_modules/typescript/bin/tsc", import.meta.url),
      ),
      "--strict",
      "--noEmit",
      "--skipLibCheck",
      "--target",
      "ES2022",
      "--module",
      "NodeNext",
      fileURLToPath(new URL("fixtures/realtime.ts", import.meta.url)),
    ],
    { stdio: "inherit" },
  );
});
