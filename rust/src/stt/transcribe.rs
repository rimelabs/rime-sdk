use super::{Stt, TranscriptionMode};
use crate::{
    client::{require_runtime, validate_timeout},
    error::request_id,
    Error, ErrorKind,
};
use rimelabs_api as pb;
use std::time::Duration;

const MAX_WAV_BYTES: usize = 16 * 1024 * 1024;

/// Options for a complete WAV recording. TTS defaults do not apply.
#[derive(Clone, Debug)]
pub struct TranscribeOptions {
    language: String,
    mode: TranscriptionMode,
    context_terms: Vec<String>,
    timeout: Option<Duration>,
}
impl TranscribeOptions {
    /// Select the spoken BCP-47 language, passed unchanged to the service.
    pub fn new(language: impl Into<String>) -> Self {
        Self {
            language: language.into(),
            mode: TranscriptionMode::Written,
            context_terms: Vec::new(),
            timeout: None,
        }
    }
    /// Select written or verbatim formatting intent.
    pub fn mode(mut self, mode: TranscriptionMode) -> Self {
        self.mode = mode;
        self
    }
    /// Set recognition hints without changing their order or contents.
    pub fn context_terms(mut self, terms: Vec<String>) -> Self {
        self.context_terms = terms;
        self
    }
    /// Set the overall timeout, including connection and upload. None disables it.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
}

/// A completed whole-recording transcription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transcription {
    /// Final text; silence may produce an empty string.
    pub text: String,
    /// Canonical language confirmed by the service.
    pub language: String,
    /// Service request identity, when supplied.
    pub request_id: Option<String>,
}

impl Stt<'_> {
    /// Transcribe complete WAV bytes, including the header, without retries.
    /// PCM16 mono 16 kHz is the portable format. Bytes are sent unchanged, with a
    /// maximum of 16 MiB. Dropping the future or closing the client cancels the RPC.
    pub async fn transcribe(
        &self,
        audio: impl AsRef<[u8]>,
        options: TranscribeOptions,
    ) -> Result<Transcription, Error> {
        self.client.ensure_open()?;
        require_runtime()?;
        validate_timeout(options.timeout)?;
        let audio = audio.as_ref();
        if audio.is_empty() {
            return Err(Error::new(
                ErrorKind::AudioFormat,
                "audio must contain complete WAV bytes",
            ));
        }
        if audio.len() > MAX_WAV_BYTES {
            return Err(Error::new(
                ErrorKind::ResourceLimit,
                "WAV input exceeds 16 MiB",
            ));
        }
        let operation = async {
            let mut stub = self
                .client
                .inner
                .stt_stub()
                .await?
                .max_encoding_message_size(MAX_WAV_BYTES + 65536);
            let request = self.client.inner.request(pb::TranscriptionRequest {
                audio: audio.to_vec(),
                options: Some(pb::TranscriptionOptions {
                    language: Some(options.language),
                    mode: match options.mode {
                        TranscriptionMode::Written => pb::TranscriptionMode::Written as i32,
                        TranscriptionMode::Verbatim => pb::TranscriptionMode::Verbatim as i32,
                    },
                    context_terms: options.context_terms,
                }),
            });
            let response = tokio::time::timeout(
                self.client.inner.stt_limits.completion,
                stub.transcribe(request),
            )
            .await
            .map_err(|_| Error::new(ErrorKind::Timeout, "transcription request timed out"))??;
            let identity = request_id(response.metadata()).map(str::to_owned);
            let result = response.into_inner();
            let language = result
                .language
                .filter(|value| !value.tag.is_empty() && value.source != 0)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Stream,
                        "the service did not confirm a resolved language",
                    )
                    .with_request_id(identity.as_deref())
                })?;
            if result.text.len() > 65536 {
                return Err(Error::new(
                    ErrorKind::ResourceLimit,
                    "the transcript exceeds the SDK text limit",
                )
                .with_request_id(identity.as_deref()));
            }
            Ok(Transcription {
                text: result.text,
                language: language.tag,
                request_id: identity,
            })
        };
        let deadline = async {
            match options.timeout {
                Some(budget) => tokio::time::sleep(budget).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = self.client.inner.cancellation.cancelled() => Err(Error::new(ErrorKind::Cancelled, "client closed")),
            _ = deadline => Err(Error::new(ErrorKind::Timeout, "overall transcription deadline expired")),
            result = operation => result,
        }
    }
}
