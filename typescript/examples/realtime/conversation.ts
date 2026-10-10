import {
  Rime,
  RimeInputError,
  RimeRealtimeError,
  type RealtimeSession,
  type OutputRef,
  type ResponseEnded,
  type ToolCall,
  type ToolDefinition,
} from "@rimelabs/sdk";
import { BrowserAudio, serve } from "../common/browser-audio.js";

interface Reply {
  output?: OutputRef;
  interrupted: boolean;
  calls: ToolCall[];
}
type ExecuteTool = (call: ToolCall) => Promise<string>;

export async function converse(
  session: RealtimeSession,
  audio: BrowserAudio,
  executeTool?: ExecuteTool,
) {
  const replies = new Map<string, Reply>();
  const tasks = new Set<Promise<void>>();
  let fail!: (error: unknown) => void;
  const failure = new Promise<never>((_, reject) => {
    fail = reject;
  });
  function start(task: Promise<void>) {
    tasks.add(task);
    task.then(
      () => tasks.delete(task),
      (error) => {
        tasks.delete(task);
        fail(error);
      },
    );
  }
  async function finish(event: ResponseEnded) {
    const id = event.response.responseId;
    const reply = replies.get(id)!;
    const played = await audio.finish(id);
    if (reply.interrupted && played.interrupted && reply.output) {
      try {
        await session.reportPlayback({
          kind: "interrupted",
          output: reply.output,
          audioEndMs: Math.floor(played.playedMs),
        });
      } catch (error) {
        if (
          !(error instanceof RimeRealtimeError) ||
          error.fault.code !== "truncate_not_current"
        )
          throw error;
      }
    } else {
      await session.reportPlayback({
        kind: "finished",
        response: event.response,
        playedMs: played.playedMs,
      });
    }
    if (event.status === "failed" || event.status === "unknown")
      throw new Error(`Response ${event.status}: ${event.reason}`);
    if (reply.calls.length && event.status === "completed") {
      for (const call of reply.calls) {
        if (!executeTool) throw new Error("The example has no tool handler");
        await session.submitToolResult(call.call, await executeTool(call));
      }
      if (!reply.interrupted) {
        try {
          await session.continueReply(event.response);
        } catch (error) {
          if (
            !(error instanceof RimeInputError && reply.interrupted) &&
            (!(error instanceof RimeRealtimeError) ||
              error.fault.scope !== "event" ||
              error.fault.code !== "tool_continuation_unavailable")
          )
            throw error;
        }
      }
    }
    replies.delete(id);
  }
  const sending = (async () => {
    for await (const data of audio.frames())
      await session.sendAudio({
        data,
        format: { sampleRate: 48000, channels: 1 },
      });
  })();
  const reading = (async () => {
    for await (const { payload } of session.events) {
      switch (payload.kind) {
        case "response.started":
          replies.set(payload.response.responseId, {
            interrupted: false,
            calls: [],
          });
          audio.begin(payload.response.responseId);
          break;
        case "message.started":
          replies.get(payload.output.response.responseId)!.output =
            payload.output;
          break;
        case "text.done":
          audio.send({ type: "text", role: "Prism", text: payload.text });
          break;
        case "transcript.final":
          audio.send({ type: "text", role: "You", text: payload.text });
          break;
        case "audio.delta":
          if (!replies.get(payload.output.response.responseId)!.interrupted)
            audio.write(payload.output.response.responseId, payload.audio.data);
          break;
        case "tool.call":
          replies.get(payload.call.responseId)!.calls.push(payload);
          break;
        case "speech.started":
          if (session.info.interruptOnSpeech) {
            for (const reply of replies.values()) reply.interrupted = true;
            audio.interrupt();
          }
          break;
        case "response.ended":
          start(finish(payload));
          break;
        case "error":
        case "transcript.failed":
          if (payload.error.scope === "session")
            throw new RimeRealtimeError(payload.error);
          audio.send({
            type: "text",
            role: "Notice",
            text: payload.error.message,
          });
          break;
      }
    }
    throw new Error("Prism closed the event stream");
  })();
  try {
    await Promise.race([sending, reading, failure]);
  } finally {
    audio.close();
    await session.close();
    await Promise.allSettled([sending, reading, ...tasks]);
  }
}

export function voice(tools: ToolDefinition[] = [], executeTool?: ExecuteTool) {
  const endpoint = process.env.PRISM_URL;
  if (!endpoint)
    throw new Error(
      "Set PRISM_URL to your deployment's /v1/realtime WebSocket URL",
    );
  return serve("prism", async (audio) => {
    const client = new Rime();
    try {
      const session = await client.realtime.connect({
        endpoint,
        voice: process.env.PRISM_VOICE,
        tools,
        instructions: tools.length
          ? "Give short answers in English. Use lookup_order for order questions."
          : "Give short answers in English.",
      });
      audio.send({ type: "ready" });
      await converse(session, audio, executeTool);
    } finally {
      await client.close();
    }
  });
}
