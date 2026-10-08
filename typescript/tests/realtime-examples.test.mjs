import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Peer } from "./realtime-peer.mjs";

const exec = promisify(execFile);
const scripts = new URL("../../examples/typescript/realtime/", import.meta.url);

async function run(t, file, respond, source) {
  // Node resolves the unchanged @rimelabs/sdk import through package self-reference.
  // This also works in CI without installing the separate examples package.
  const directory = await mkdtemp(
    join(dirname(fileURLToPath(import.meta.url)), ".realtime-example-"),
  );
  const peer = await new Peer().start();
  t.after(async () => {
    await peer.close();
    await rm(directory, { recursive: true, force: true });
  });
  const script = join(directory, "example.mjs");
  await writeFile(
    script,
    source ?? (await readFile(new URL(file, scripts), "utf8")),
  );
  peer.server.on("connection", (socket) => {
    socket.on("message", (raw) => {
      if (JSON.parse(raw.toString()).type === "session.update")
        peer.emit("prsm.typed_input.ready");
    });
  });
  peer.onRequest = (request) => respond(peer, request);
  const result = await exec(process.execPath, [script], {
    cwd: directory,
    timeout: 5000,
    env: {
      ...process.env,
      RIME_API_KEY: "test",
      PRISM_URL: peer.endpoint,
      PRISM_VOICE: "test",
    },
  });
  return { ...result, directory };
}
function reply(peer, request, id = "reply-1", status = "completed") {
  peer.accepted(request, id);
  peer.message(id);
  const coordinates = {
    response_id: id,
    item_id: "message-1",
    output_index: 0,
    content_index: 0,
  };
  peer.emit("response.text.delta", { ...coordinates, delta: "Hello." });
  peer.emit("response.audio.delta", {
    ...coordinates,
    delta: Buffer.from([1, 0, 2, 0]).toString("base64"),
  });
  peer.ended(id, status);
}

test("typed-turn example prints text and saves PCM through the public package", async (t) => {
  const result = await run(t, "typed-turn.mjs", (peer, request) =>
    reply(peer, request),
  );
  assert.match(result.stdout, /Hello\./);
  assert.deepEqual(
    await readFile(join(result.directory, "reply.pcm")),
    Buffer.from([1, 0, 2, 0]),
  );
});

test("typed-turn example rejects failed generation", async (t) => {
  await assert.rejects(
    run(t, "typed-turn.mjs", (peer, request) =>
      reply(peer, request, "reply-1", "failed"),
    ),
    /Response failed/,
  );
});

for (const calls of [0, 1, 2]) {
  test(`tool example handles ${calls} calls and waits for acknowledgments`, async (t) => {
    let completed = 0,
      continuation = false;
    const result = await run(t, "tools.mjs", (peer, request) => {
      if (request.type === "conversation.item.create") {
        assert.equal(JSON.parse(request.item.output).status, "shipped");
        peer.ack(request);
        completed++;
      } else if (request.response.metadata.prsm_cause === "tool_continuation") {
        continuation = true;
        assert.equal(completed, calls);
        reply(peer, request, "reply-2");
      } else if (calls) {
        peer.accepted(request);
        for (let i = 0; i < calls; i++) {
          peer.tool("reply-1", `call-${i}`);
          peer.tool("reply-1", `call-${i}`);
        }
        peer.ended();
      } else reply(peer, request);
    });
    assert.match(result.stdout, /Hello\./);
    assert.equal(completed, calls);
    assert.equal(continuation, calls > 0);
  });
}
