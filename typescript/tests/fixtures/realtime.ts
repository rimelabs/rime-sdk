import {
  Rime,
  RimeRealtimeError,
  type RealtimeFault,
  type TranscriptFailed,
  type FaultEvent,
  type InputEvent,
  type ConversationOptions,
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

const previousFault: RealtimeFault = {
  code: "unavailable",
  message: "Service unavailable",
  scope: "response",
  requestId: "request-1",
  responseId: "response-1",
  itemId: null,
  callIds: [],
  correlationId: null,
  parameter: null,
};
const constructedError = new RimeRealtimeError(previousFault);
const retryable: boolean = constructedError.fault.retryable;
void retryable;
// @ts-expect-error Retry classification is read-only.
constructedError.fault.retryable = false;
new RimeRealtimeError({ ...previousFault, retryable: false });

const previousTranscript: TranscriptFailed = {
  kind: "transcript.failed",
  itemId: "item-1",
  error: previousFault,
};
const previousError: FaultEvent = { kind: "error", error: previousFault };
const previousInput: InputEvent = previousTranscript;
const previousEvents: SessionEvent[] = [previousTranscript, previousError].map(
  (payload) => ({
    sessionId: "session-1",
    eventId: "event-1",
    requestId: null,
    payload,
  }),
);
void [previousInput, previousEvents];

const onEvent: NonNullable<ConversationOptions["onEvent"]> = (event) => {
  const payload = event.payload;
  if (payload.kind === "error" || payload.kind === "transcript.failed") {
    const retryable: boolean = payload.error.retryable;
    void retryable;
  }
};
// Existing callbacks can continue to accept the general event type.
const previousCallback: typeof onEvent = (event: SessionEvent) => {
  void event;
};
void [onEvent, previousCallback];

const tool: ToolDefinition = { name: "lookup", parameters: { type: "object" } };
const fillers: PrismFiller[] = [
  { type: "none" },
  { type: "text", text: "One moment." },
  { type: "auto" },
  { type: "auto", instructions: "Say you are checking." },
  { type: "none", text: undefined, instructions: undefined },
  { type: "text", text: "Hi", instructions: undefined },
  { type: "auto", text: undefined, instructions: undefined },
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
    void envelope;
    const payload = event.payload;
    if (payload.kind === "error" || payload.kind === "transcript.failed") {
      const retryable: boolean = payload.error.retryable;
      void retryable;
      // @ts-expect-error Retry classification is read-only.
      payload.error.retryable = true;
    }
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
import {
  requests,
  type Acknowledgment,
  type SessionPatch,
  type SessionSettings,
} from "../../dist/realtime/protocol.js";
const truncationSettings: SessionSettings["truncation"][] = [
  "auto",
  "disabled",
  { type: "retention_ratio", retention_ratio: 0.9 },
  // @ts-expect-error Unsupported truncation strings cannot be sent.
  "future",
  // @ts-expect-error Ratio objects must use the known wire discriminator.
  { type: "future", retention_ratio: 0.9 },
];
const invalidTruncationPatch: SessionPatch = {
  // @ts-expect-error Partial updates use the same closed wire type.
  truncation: "future",
};
void [truncationSettings, invalidTruncationPatch];
function checkAcknowledgmentTypes(ack: Acknowledgment) {
  const created: ResponseRef = requests.create.result(ack).response;
  const item: string = requests.item.result(ack).itemId;
  const call: string = requests.toolResult.result(ack).callId;
  const voice: string = requests.update.result(ack).session.voice;
  // @ts-expect-error A response acknowledgment cannot return a session view.
  requests.create.result(ack).session;
  const maybeItemId: string | null = requests.toolResult.result(ack).itemId;
  // @ts-expect-error Older tool result acknowledgments can omit an item ID.
  const requiredItemId: string = requests.toolResult.result(ack).itemId;
  void [maybeItemId, requiredItemId];
  void [created, item, call, voice];
}
void checkAcknowledgmentTypes;
