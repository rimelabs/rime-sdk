export {
  Rime,
  type RimeOptions,
  type SynthesisOptions,
  type DiscoveryOptions,
  type VoiceListOptions,
} from "./client.js";
export { AudioStream } from "./tts/stream.js";
export { AudioFormat } from "./tts/audio.js";
export type { PronunciationEntry } from "./tts/lexicon.js";
export type {
  TimestampResult,
  TimestampStatus,
  WordTimestamp,
} from "./tts/timestamps.js";
export * from "./errors.js";
export { RealtimeSession } from "./realtime/session.js";
export * from "./realtime/types.js";

export { TranscriptStream } from "./stt/stream.js";
export type {
  AudioSource,
  TranscribeOptions,
  Transcription,
  TranscriptionOptions,
  TranscriptionMode,
  TranscriptionPartial,
  TranscriptionDelta,
  TranscriptionOutputContract,
  TranscriptionFinal,
  TranscriptionUpdate,
} from "./stt/types.js";
export { runConversation } from "./realtime/conversation.js";
export type {
  ConversationSource,
  ConversationOutput,
  ConversationPlayback,
  ConversationOptions,
} from "./realtime/conversation.js";
