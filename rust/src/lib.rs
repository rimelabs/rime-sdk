//! Asynchronous speech recognition and Coda and Mist v3 text-to-speech.
//!
//! Use [`Client::builder`] to configure credentials. The SDK uses the caller's
//! Tokio runtime with I/O and time enabled. Audio supports mono PCM16 and WAV
//! at 8/16/24 kHz, MP3, and mu-law at 8 kHz.
//! Applications own audio playback. Realtime Prism is not included.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

mod audio;
mod buffer;
mod client;
mod error;
mod lexicon;
mod stream;
pub mod stt;
mod timestamps;

pub use audio::AudioFormat;
pub use client::{Client, ClientBuilder, Model, SynthesisOptions, Tts};
pub use error::{Error, ErrorKind};
pub use lexicon::PronunciationEntry;
pub use stream::AudioStream;
pub use stt::{
    PcmFormat, Stt, TranscriptStream, TranscriptionMode, TranscriptionOptions, TranscriptionUpdate,
};
pub use timestamps::{TimestampResult, TimestampStatus, WordTimestamp};

#[cfg(test)]
mod tests;
