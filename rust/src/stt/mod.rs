//! Streaming speech recognition for one caller-ended utterance.
mod audio;
mod protocol;
mod stream;
#[cfg(test)]
mod tests;

use crate::{client::validate_timeout, Client, Error};
pub use audio::PcmFormat;
use bytes::Bytes;
use futures_core::Stream;
use std::time::Duration;
pub use stream::TranscriptStream;

/// Transcript formatting intent, not a guarantee of exact tokens.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TranscriptionMode {
    /// Normalize speech to written text.
    #[default]
    Written,
    /// Preserve spoken wording.
    Verbatim,
}

/// Options for one utterance. TTS client defaults do not apply.
#[derive(Clone, Debug)]
pub struct TranscriptionOptions {
    language: String,
    mode: TranscriptionMode,
    context_terms: Vec<String>,
    input_format: PcmFormat,
    timeout: Option<Duration>,
}
impl TranscriptionOptions {
    /// Select a required spoken BCP-47 language tag, passed unchanged to the service.
    pub fn new(language: impl Into<String>) -> Self {
        Self {
            language: language.into(),
            mode: TranscriptionMode::Written,
            context_terms: Vec::new(),
            input_format: PcmFormat::default(),
            timeout: None,
        }
    }
    /// Select written or verbatim formatting intent.
    pub fn mode(mut self, mode: TranscriptionMode) -> Self {
        self.mode = mode;
        self
    }
    /// Set recognition hints, preserving their order and contents.
    pub fn context_terms(mut self, terms: Vec<String>) -> Self {
        self.context_terms = terms;
        self
    }
    /// Set the raw PCM16 input format. The default is 16 kHz mono.
    pub fn input_format(mut self, format: PcmFormat) -> Self {
        self.input_format = format;
        self
    }
    /// Set an overall timeout from first poll until final consumption, including pauses.
    /// `None` disables it. Zero is invalid.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
}

/// A complete replacement transcript. Never append partials to previous text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptionUpdate {
    /// A revisable snapshot.
    Partial {
        /// Complete current text.
        text: String,
    },
    /// A confirmed result after successful protocol and gRPC completion.
    Final {
        /// Complete final text; silence can produce an empty string.
        text: String,
        /// Canonical language confirmed by the service.
        language: String,
    },
}

/// Speech-to-text operations backed by a shared client.
pub struct Stt<'a> {
    pub(crate) client: &'a Client,
}
impl Stt<'_> {
    /// Create a lazy transcript stream. Network work starts on the first poll.
    /// Audio is read only after the service accepts the configuration.
    /// The source must yield without blocking Tokio; dropping the result cancels it.
    ///
    /// # Panics
    /// Panics if the current Tokio runtime does not have time enabled.
    pub fn stream<S>(
        &self,
        source: S,
        options: TranscriptionOptions,
    ) -> Result<TranscriptStream, Error>
    where
        S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
    {
        self.client.ensure_open()?;
        crate::client::require_runtime()?;
        options.input_format.validate()?;
        validate_timeout(options.timeout)?;
        stream::start(self.client.inner.clone(), source, options)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub acceptance: Duration,
    pub completion: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            acceptance: Duration::from_secs(10),
            completion: Duration::from_secs(120),
        }
    }
}
