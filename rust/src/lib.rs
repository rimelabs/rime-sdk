//! Asynchronous Coda and Mist v3 text-to-speech with bounded audio buffering.
//!
//! Use [`Client::builder`] to configure credentials. The SDK uses the caller's
//! Tokio runtime with I/O and time enabled. Audio is raw mono PCM16 at 24 kHz
//! or mu-law at 8 kHz.
//! Applications own audio playback. Realtime Prism is not included.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

mod audio;
mod client;
mod error;
mod sentences;
mod stream;

pub use audio::AudioFormat;
pub use client::{Client, ClientBuilder, Model, SynthesisOptions, Tts};
pub use error::{Error, ErrorKind};
pub use stream::AudioStream;

#[cfg(test)]
mod tests;
