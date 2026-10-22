import {
  Rime,
  type RealtimeConnectOptions,
  type RealtimeSession,
  type ResponseRef,
  type ToolDefinition,
  type PrismFiller,
  type AudioChunk,
  type PlaybackReport,
  type SessionEvent,
  type TurnDetection,
  type TurnDetectionInfo,
} from "../../dist/index.js";

const tool: ToolDefinition = { name: "lookup", parameters: { type: "object" } };
const fillers: PrismFiller[] = [
  { type: "none" },
  { type: "text", text: "One moment." },
  { type: "auto" },
  { type: "auto", instructions: "Say you are checking." },
  // @ts-expect-error Fixed filler requires text.
  { type: "text" },
  // @ts-expect-error Silent filler does not accept text.
  { type: "none", text: "Hi" },
  // @ts-expect-error Auto filler does not accept fixed text.
  { type: "auto", text: "Hi" },
  // @ts-expect-error Filler modes form a closed set.
  { type: "future" },
];
const configuredTools: ToolDefinition[] = fillers.map((prismFiller) => ({
  ...tool,
  prismFiller,
}));
void configuredTools;
const options: RealtimeConnectOptions = {
  tools: [tool],
  timeouts: { readyS: 10 },
  turnDetection: { eagerness: "low", endOfTurnTimeoutMs: 2000 },
};
async function useSession(session: RealtimeSession) {
  const timing: TurnDetectionInfo = session.info.turnDetection;
  void timing;
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
client.realtime.connect({});
client.realtime.connect();
const invalidTiming: TurnDetection = {
  // @ts-expect-error Eagerness is a closed set of input values.
  eagerness: "future",
};
void invalidTiming;

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
