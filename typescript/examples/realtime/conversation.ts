import {
  Rime,
  runConversation,
  type RealtimeSession,
  type ToolCall,
  type ToolDefinition,
} from "@rimelabs/sdk";
import { BrowserAudio, serve } from "../common/browser-audio.js";
import { ManualTools } from "./manual-tools.js";

type ExecuteTool = (call: ToolCall, signal: AbortSignal) => Promise<string>;

export async function converse(
  session: RealtimeSession,
  audio: BrowserAudio,
  executeTool?: ExecuteTool,
) {
  const controller = new AbortController();
  const tools = new ManualTools(session, controller, executeTool);
  try {
    await runConversation(session, {
      source: audio,
      playback: audio,
      signal: controller.signal,
      onEvent(event) {
        const payload = event.payload;
        if (payload.kind === "text.done")
          audio.send({ type: "text", role: "Prism", text: payload.text });
        else if (payload.kind === "transcript.final")
          audio.send({ type: "text", role: "You", text: payload.text });
        else if (
          payload.kind === "error" ||
          payload.kind === "transcript.failed"
        )
          audio.send({
            type: "text",
            role: "Notice",
            text: payload.error.message,
          });
        else if (
          payload.kind === "response.ended" &&
          ["failed", "unknown"].includes(payload.status)
        )
          throw new Error(`Response ${payload.status}: ${payload.reason}`);
        tools.onEvent(event);
      },
    });
  } finally {
    controller.abort();
    await tools.settled();
  }
}

export function voice(tools: ToolDefinition[] = [], executeTool?: ExecuteTool) {
  const endpoint = process.env.PRISM_URL;
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
