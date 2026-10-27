//! Asynchronous speech recognition and Coda and Mist v3 text-to-speech.
//!
//! Use [`Client::builder`] to configure credentials. The SDK uses the caller's
//! Tokio runtime with I/O and time enabled. Audio supports mono PCM16 and WAV
//! at 8/16/24 kHz, MP3, and mu-law at 8 kHz.
//! Applications own audio playback. Prism realtime sessions support application-owned audio adapters.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

mod audio;
mod buffer;
mod client;
mod error;
mod lexicon;
mod pcm;
pub mod realtime;
mod stream;
pub mod stt;
mod timestamps;

pub use audio::AudioFormat;
pub use client::{Client, ClientBuilder, Model, SynthesisOptions, Tts};
pub use error::{Error, ErrorKind};
pub use lexicon::PronunciationEntry;
pub use realtime::{
    AudioChunk, ConversationOutput, ConversationPlayback, ConversationRunner, ConversationSource,
    HistoryItem, HistoryRole, ItemRef, OutputRef, PlaybackReport, PrismFiller, Realtime,
    RealtimeConnectOptions, RealtimeEvent, RealtimeEvents, RealtimeFault, RealtimeSession,
    RealtimeTimeouts, RealtimeUpdateOptions, ReplyOptions, ResponseRef, ResponseTruncation,
    SessionEvent, SessionInfo, ToolCallRef, ToolDefinition, Truncation, TurnDetection,
};
pub use stream::AudioStream;
pub use stt::{
    PcmFormat, Stt, TranscribeOptions, TranscriptStream, Transcription, TranscriptionMode,
    TranscriptionOptions, TranscriptionOutputContract, TranscriptionUpdate,
};
pub use timestamps::{TimestampResult, TimestampStatus, WordTimestamp};

#[cfg(test)]
mod tests;
