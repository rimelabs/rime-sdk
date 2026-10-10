use crate::{AudioFormat, AudioStream, Error, ErrorKind};
use futures_core::Stream;
use rimelabs_api::{
    text_to_speech_client::TextToSpeechClient, GetSupportedLanguagesRequest,
    GetSupportedSpeakersRequest,
};
use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::OnceCell;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};

/// Supported text-to-speech model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Model {
    /// Coda, using its default voice `clementine`.
    #[default]
    Coda,
    /// Mist v3, using its default voice `astra`.
    MistV3,
}

impl Model {
    pub(crate) fn voice(self) -> &'static str {
        match self {
            Self::Coda => "clementine",
            Self::MistV3 => "astra",
        }
    }
    fn endpoint(self) -> &'static str {
        match self {
            Self::Coda => "coda.api.rime.ai:443",
            Self::MistV3 => "mist.api.rime.ai:443",
        }
    }
}

/// Options for a synthesis request. Defaults inherit the client's voice and timeout.
#[derive(Clone, Debug, Default)]
pub struct SynthesisOptions {
    pub(crate) voice: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) format: AudioFormat,
    pub(crate) timeout: Option<Option<Duration>>,
}

impl SynthesisOptions {
    /// Select a voice. Empty or whitespace-only values fail validation.
    pub fn voice(mut self, voice: impl Into<String>) -> Self {
        self.voice = Some(voice.into());
        self
    }
    /// Select a language. The default is `en`.
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }
    /// Select raw PCM24 or mu-law8 output.
    pub fn audio_format(mut self, format: AudioFormat) -> Self {
        self.format = format;
        self
    }
    /// Override the overall timeout. `None` disables it. Zero is invalid.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Client configuration. Construction does not perform network requests.
#[derive(Default)]
pub struct ClientBuilder {
    key: Option<String>,
    model: Model,
    endpoint: Option<String>,
    timeout: Option<Duration>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientBuilder")
            .field("model", &self.model)
            .field("endpoint", &self.endpoint)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl ClientBuilder {
    /// Set the API key. Otherwise the client reads `RIME_API_KEY` at build time.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
    /// Select Coda or Mist v3.
    pub fn model(mut self, model: Model) -> Self {
        self.model = model;
        self
    }
    /// Override the TLS hostname and optional port, without a scheme or path.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    /// Set an overall request timeout. The default is no overall timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Validate configuration and create a client.
    /// Use operations inside a Tokio runtime with I/O and time enabled.
    pub fn build(self) -> Result<Client, Error> {
        validate_timeout(self.timeout)?;
        let key = self
            .key
            .or_else(|| std::env::var("RIME_API_KEY").ok())
            .ok_or_else(|| {
                Error::new(ErrorKind::Authentication, "set an API key or RIME_API_KEY")
            })?;
        if key.is_empty() || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
            return Err(Error::new(
                ErrorKind::Authentication,
                "API key must be printable ASCII without whitespace",
            ));
        }
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {key}")
            .parse()
            .map_err(|_| Error::new(ErrorKind::Authentication, "invalid API key"))?;
        authorization.set_sensitive(true);
        let target = self.endpoint.as_deref().unwrap_or(self.model.endpoint());
        validate_endpoint(target)?;
        let endpoint = Endpoint::from_shared(format!("https://{target}"))
            .map_err(|error| Error::new(ErrorKind::Input, "invalid endpoint").caused_by(error))?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|error| {
                Error::new(ErrorKind::Input, "invalid TLS configuration").caused_by(error)
            })?
            .connect_timeout(Duration::from_secs(10));
        let cancellation = CancellationToken::new();
        Ok(Client {
            lifetime: Arc::new(ClientLifetime(cancellation.clone())),
            inner: Arc::new(Inner {
                endpoint,
                authorization,
                model: self.model,
                timeout: self.timeout,
                channel: OnceCell::new(),
                cancellation,
                tasks: TaskTracker::new(),
                admission: Mutex::new(()),
                first_audio_timeout: Duration::from_secs(30),
                progress_timeout: Duration::from_secs(60),
            }),
        })
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), Error> {
    let invalid = || {
        Error::new(
            ErrorKind::Input,
            "endpoint must be a hostname with an optional port",
        )
    };
    let mut parts = endpoint.split(':');
    let hostname = parts.next().ok_or_else(invalid)?;
    if hostname.len() > 253
        || hostname.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label.as_bytes()[0].is_ascii_alphanumeric()
                || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(invalid());
    }
    if let Some(port) = parts.next() {
        if port.is_empty()
            || port.len() > 5
            || !port.bytes().all(|byte| byte.is_ascii_digit())
            || port.parse::<u16>().ok().filter(|port| *port > 0).is_none()
        {
            return Err(invalid());
        }
    }
    if parts.next().is_some() {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn validate_timeout(timeout: Option<Duration>) -> Result<(), Error> {
    if timeout.is_some_and(|timeout| {
        timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none()
    }) {
        return Err(Error::new(
            ErrorKind::Input,
            "timeout must be positive and representable",
        ));
    }
    Ok(())
}

fn require_runtime() -> Result<(), Error> {
    tokio::runtime::Handle::try_current()
        .map(|_| ())
        .map_err(|_| Error::new(ErrorKind::Input, "operations require a Tokio runtime"))
}

struct ClientLifetime(CancellationToken);
impl Drop for ClientLifetime {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A reusable client. Clones share connections and client-wide cancellation.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
    lifetime: Arc<ClientLifetime>,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("model", &self.inner.model)
            .field("closed", &self.inner.cancellation.is_cancelled())
            .field("handles", &Arc::strong_count(&self.lifetime))
            .finish_non_exhaustive()
    }
}

pub(crate) struct Inner {
    pub(crate) endpoint: Endpoint,
    authorization: MetadataValue<Ascii>,
    pub(crate) model: Model,
    pub(crate) timeout: Option<Duration>,
    channel: OnceCell<Channel>,
    pub(crate) cancellation: CancellationToken,
    tasks: TaskTracker,
    admission: Mutex<()>,
    pub(crate) first_audio_timeout: Duration,
    pub(crate) progress_timeout: Duration,
}

enum DiscoveryRequest {
    Voices { language: Option<String> },
    Languages,
}

impl Inner {
    pub(crate) fn spawn<F>(&self, future: F) -> Result<tokio::task::JoinHandle<F::Output>, Error>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let _admission = self.admission.lock().expect("admission mutex poisoned");
        if self.cancellation.is_cancelled() {
            return Err(Error::new(ErrorKind::Cancelled, "client is closed"));
        }
        Ok(self.tasks.spawn(future))
    }

    fn close(&self) {
        let _admission = self.admission.lock().expect("admission mutex poisoned");
        self.cancellation.cancel();
        self.tasks.close();
    }

    pub(crate) async fn stub(&self) -> Result<TextToSpeechClient<Channel>, Error> {
        let channel = self
            .channel
            .get_or_try_init(|| async {
                self.endpoint.connect().await.map_err(|error| {
                    Error::new(
                        ErrorKind::Unavailable,
                        "could not connect to the speech service",
                    )
                    .caused_by(error)
                })
            })
            .await?;
        Ok(TextToSpeechClient::new(channel.clone())
            .max_decoding_message_size(4 * 1024 * 1024)
            .max_encoding_message_size(131_072))
    }
    pub(crate) fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }
}

impl Client {
    /// Start configuring a client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Start synthesis of complete text. Returns immediately; consume errors from the stream.
    ///
    /// # Panics
    /// Panics if the current Tokio runtime does not have time enabled.
    pub fn synthesize(
        &self,
        text: impl Into<String>,
        options: SynthesisOptions,
    ) -> Result<AudioStream, Error> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(Error::new(ErrorKind::Input, "text must not be blank"));
        }
        self.synthesize_stream(futures_util::stream::once(async { Ok(text) }), options)
    }

    /// Start synthesis from incremental text. The source must yield without blocking Tokio.
    /// Dropping the audio stream cancels the source and the RPC.
    ///
    /// # Panics
    /// Panics if the current Tokio runtime does not have time enabled.
    pub fn synthesize_stream<S>(
        &self,
        source: S,
        options: SynthesisOptions,
    ) -> Result<AudioStream, Error>
    where
        S: Stream<Item = Result<String, Error>> + Send + 'static,
    {
        self.ensure_open()?;
        require_runtime()?;
        crate::stream::start(self.inner.clone(), source, options)
    }

    /// List voices, optionally for one language. Uses a maximum ten-second budget.
    ///
    /// # Panics
    /// Panics if the current Tokio runtime does not have time enabled.
    pub async fn voices(&self, language: Option<&str>) -> Result<Vec<String>, Error> {
        if language.is_some_and(|value| value.trim().is_empty()) {
            return Err(Error::new(ErrorKind::Input, "language must not be blank"));
        }
        self.discover(DiscoveryRequest::Voices {
            language: language.map(str::to_owned),
        })
        .await
    }

    /// List supported languages. Uses a maximum ten-second budget.
    ///
    /// # Panics
    /// Panics if the current Tokio runtime does not have time enabled.
    pub async fn languages(&self) -> Result<Vec<String>, Error> {
        self.discover(DiscoveryRequest::Languages).await
    }

    async fn discover(&self, request: DiscoveryRequest) -> Result<Vec<String>, Error> {
        self.ensure_open()?;
        require_runtime()?;
        let budget = self
            .inner
            .timeout
            .unwrap_or(Duration::from_secs(10))
            .min(Duration::from_secs(10));
        let operation = async {
            let mut stub = self.inner.stub().await?;
            for attempt in 0..3 {
                let result = match &request {
                    DiscoveryRequest::Voices { language } => stub
                        .get_supported_speakers(self.inner.request(GetSupportedSpeakersRequest {
                            language: language.clone(),
                        }))
                        .await
                        .map(|response| response.into_inner().speakers),
                    DiscoveryRequest::Languages => stub
                        .get_supported_languages(
                            self.inner.request(GetSupportedLanguagesRequest {}),
                        )
                        .await
                        .map(|response| response.into_inner().languages),
                };
                match result {
                    Ok(values) => return Ok(values),
                    Err(status) if status.code() == tonic::Code::Unavailable && attempt < 2 => {
                        tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
                    }
                    Err(status) => return Err(Error::from(status)),
                }
            }
            unreachable!("the last discovery attempt always returns")
        };
        tokio::select! {
            biased;
            _ = self.inner.cancellation.cancelled() => Err(Error::new(ErrorKind::Cancelled, "client closed")),
            result = tokio::time::timeout(budget, operation) => result.unwrap_or_else(|_| Err(Error::new(ErrorKind::Timeout, "discovery timed out"))),
        }
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.inner.cancellation.is_cancelled() {
            return Err(Error::new(ErrorKind::Cancelled, "client is closed"));
        }
        Ok(())
    }

    /// Cancel all operations on this client and its clones, then wait for stream workers.
    pub async fn close(&self) {
        self.inner.close();
        self.inner.tasks.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn client() -> Client {
        Client::builder().api_key("test-key").build().unwrap()
    }

    #[tokio::test]
    async fn close_rejects_worker_after_an_earlier_open_check() {
        let client = client();
        // Force the overlap: admission was checked before close drained the tracker,
        // but stream construction has not yet registered its worker.
        client.ensure_open().unwrap();
        client.close().await;
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let source = futures_util::stream::once(async move {
            let _guard = guard;
            std::future::pending::<Result<String, Error>>().await
        });
        let error = crate::stream::start(client.inner.clone(), source, SynthesisOptions::default())
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert!(client.inner.tasks.is_empty());
        assert!(
            dropped.load(Ordering::SeqCst),
            "source must be dropped before return"
        );
    }

    #[test]
    fn operations_without_a_runtime_return_input_errors() {
        let client = client();
        assert_eq!(
            client
                .synthesize("Hello.", SynthesisOptions::default())
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
        assert_eq!(
            client
                .synthesize_stream(futures_util::stream::empty(), SynthesisOptions::default())
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
        assert_eq!(
            client
                .languages()
                .now_or_never()
                .unwrap()
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
        assert_eq!(
            client
                .voices(None)
                .now_or_never()
                .unwrap()
                .unwrap_err()
                .kind(),
            ErrorKind::Input
        );
    }

    #[test]
    fn disabled_time_panics_before_starting_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        let client = client();
        runtime.block_on(async {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.synthesize("Hello.", SynthesisOptions::default())
            }))
            .is_err());
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.synthesize_stream(futures_util::stream::empty(), SynthesisOptions::default())
            }))
            .is_err());
            assert!(std::panic::AssertUnwindSafe(client.languages())
                .catch_unwind()
                .await
                .is_err());
            assert!(std::panic::AssertUnwindSafe(client.voices(None))
                .catch_unwind()
                .await
                .is_err());
            assert!(client.inner.tasks.is_empty());
            assert!(!client.inner.channel.initialized());
            client.close().await;
        });
    }
}
