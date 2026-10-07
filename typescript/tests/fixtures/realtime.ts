import {
  Rime,
  type RealtimeConnectOptions,
  type RealtimeSession,
  type ResponseRef,
  type ToolDefinition,
  type AudioChunk,
  type PlaybackReport,
  type SessionEvent,
} from "../../dist/index.js";

const tool: ToolDefinition = { name: "lookup", parameters: { type: "object" } };
const options: RealtimeConnectOptions = {
  endpoint: "wss://example.com/v1/realtime",
  tools: [tool],
  timeouts: { readyS: 10 },
};
async function useSession(session: RealtimeSession) {
  const ref: ResponseRef = await session.sendText("Hello");
  const input: AudioChunk = { data: new Uint8Array(2) };
  await session.sendAudio(input);
  const report: PlaybackReport = {
    kind: "finished",
    response: ref,
    playedMs: 0,
  };
  await session.reportPlayback(report);
  for await (const event of session.events) {
    const envelope: SessionEvent = event;
    const payload = envelope.payload;
    if (payload.kind === "audio.delta") {
      const rate: 8000 | 16000 | 24000 | 48000 =
        payload.audio.format.sampleRate;
      void rate;
    }
    if (payload.kind === "tool.call") {
      await session.submitToolResult(
        payload.call,
        JSON.stringify({ ok: true }),
      );
      await session.continueReply(payload.call);
    }
    // @ts-expect-error Not every event contains audio.
    void payload.audio;
  }
  // @ts-expect-error No manual commit or raw wire operations are exposed.
  await session.commitAudio();
  await session.sendAudio({
    data: new Uint8Array(),
    // @ts-expect-error Input formats are restricted.
    format: { sampleRate: 44100 },
  });
  // @ts-expect-error Playback interruption needs output identity and duration.
  await session.reportPlayback({ kind: "interrupted", response: ref });
}
const client = new Rime({ apiKey: "test" });
const connected: Promise<RealtimeSession> = client.realtime.connect(options);
void connected;
void useSession;
// @ts-expect-error Endpoint is required.
client.realtime.connect({});

// Internal request descriptors must retain their specific result types.
import { requests, type Acknowledgment } from "../../dist/realtime/protocol.js";
function checkAcknowledgmentTypes(ack: Acknowledgment) {
  const created: ResponseRef = requests.create.result(ack).response;
  const item: string = requests.item.result(ack).itemId;
  const call: string = requests.toolResult.result(ack).callId;
  const voice: string = requests.update.result(ack).session.voice;
  // @ts-expect-error A response acknowledgment cannot return a session view.
  requests.create.result(ack).session;
  // @ts-expect-error A tool result acknowledgment need not contain an item ID.
  requests.toolResult.result(ack).itemId;
  void [created, item, call, voice];
}
void checkAcknowledgmentTypes;
