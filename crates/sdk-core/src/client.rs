use crate::{
    connection::{Channel, ConnectionConfig},
    error::{CoreError, Result},
    policy::{self, Policy},
    stream::{AudioStream, StreamOptions},
    transport::ReceiveLimit,
};
use sdk_protocol::{
    GetSupportedLanguagesRequest, GetSupportedLanguagesResponse, GetSupportedSpeakersRequest,
    GetSupportedSpeakersResponse, text_to_speech_client::TextToSpeechClient,
};
use serde::Deserialize;
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tonic::Request;

#[derive(Deserialize)]
pub struct ClientConfig {
    pub api_key: String,
    #[serde(default = "default_model")]
    pub model: String,
    pub endpoint: Option<String>,
    pub timeout: Option<f64>,
}
fn default_model() -> String {
    "coda".into()
}

pub struct Client {
    pub(crate) key: Mutex<String>,
    pub(crate) target: String,
    pub(crate) policy: Policy,
    pub(crate) default_timeout: Option<f64>,
    pub(crate) stopped: CancellationToken,
    channel: AsyncMutex<Option<Channel>>,
    streams: Mutex<Vec<Weak<AudioStream>>>,
    #[cfg(feature = "test-support")]
    pub(crate) insecure: bool,
    #[cfg(feature = "test-support")]
    test_ca: Option<Vec<u8>>,
}
impl Client {
    pub fn new(config: ClientConfig) -> Result<Arc<Self>> {
        if config.api_key.trim().is_empty() {
            return Err(CoreError::new(
                "Authentication",
                "Provide api_key or set RIME_API_KEY",
            ));
        }
        let (target, _) = policy::endpoint(&config.model, config.endpoint.as_deref())?;
        Ok(Arc::new(Self {
            key: Mutex::new(config.api_key),
            target,
            policy: Policy::default(),
            default_timeout: policy::timeout(config.timeout)?,
            stopped: CancellationToken::new(),
            channel: AsyncMutex::new(None),
            streams: Mutex::new(vec![]),
            #[cfg(feature = "test-support")]
            insecure: false,
            #[cfg(feature = "test-support")]
            test_ca: None,
        }))
    }
    #[cfg(feature = "test-support")]
    pub fn testing(config: ClientConfig, target: String, policy: Policy) -> Result<Arc<Self>> {
        let mut client = Self::new(config)?;
        let inner = Arc::get_mut(&mut client).unwrap();
        inner.target = target;
        inner.policy = policy;
        inner.insecure = true;
        Ok(client)
    }
    #[cfg(feature = "test-support")]
    pub fn testing_tls(config: ClientConfig, ca: Vec<u8>) -> Result<Arc<Self>> {
        let mut client = Self::new(config)?;
        Arc::get_mut(&mut client).unwrap().test_ca = Some(ca);
        Ok(client)
    }
    pub fn ensure_open(&self) -> Result<()> {
        if self.stopped.is_cancelled() {
            Err(CoreError::input("The Rime client is closed"))
        } else {
            Ok(())
        }
    }
    pub fn stream(self: &Arc<Self>, options: StreamOptions) -> Result<Arc<AudioStream>> {
        self.ensure_open()?;
        let stream = AudioStream::new(self.clone(), options)?;
        let mut streams = self.streams.lock().unwrap();
        streams.retain(|s| s.strong_count() > 0);
        streams.push(Arc::downgrade(&stream));
        Ok(stream)
    }
    pub(crate) fn request<T>(&self, message: T) -> Result<Request<T>> {
        self.ensure_open()?;
        let value = format!("Bearer {}", self.key.lock().unwrap());
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            value
                .parse()
                .map_err(|_| CoreError::new("Authentication", "Invalid API key"))?,
        );
        Ok(request)
    }
    pub(crate) async fn connection(&self) -> Result<TextToSpeechClient<ReceiveLimit>> {
        self.ensure_open()?;
        let channel = self.ready_channel().await?;
        Ok(
            TextToSpeechClient::new(ReceiveLimit::new(channel, self.policy.receive_bytes))
                .max_decoding_message_size(self.policy.receive_bytes)
                .max_encoding_message_size(self.policy.sentence_bytes + 65536),
        )
    }

    async fn ready_channel(&self) -> Result<Channel> {
        // One budget covers lock contention, reconnects, and transport readiness.
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs_f64(self.policy.connection_timeout);
        let connect = async {
            let mut channel = self.channel.lock().await;
            if let Some(cached) = channel.as_mut()
                && cached.ready().await.is_ok()
            {
                return Ok(cached.clone());
            }
            channel.take();
            let secure = {
                #[cfg(feature = "test-support")]
                {
                    !self.insecure
                }
                #[cfg(not(feature = "test-support"))]
                {
                    true
                }
            };
            let extra_ca = {
                #[cfg(feature = "test-support")]
                {
                    self.test_ca.as_deref()
                }
                #[cfg(not(feature = "test-support"))]
                {
                    None
                }
            };
            let config = ConnectionConfig::new(&self.target, secure, extra_ca)?;
            loop {
                if let Ok(mut connected) = config.connect().await
                    && connected.ready().await.is_ok()
                {
                    *channel = Some(connected.clone());
                    return Ok::<_, CoreError>(connected);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::select! {
            biased;
            _ = self.stopped.cancelled() => Err(CoreError::cancelled()),
            result = tokio::time::timeout_at(deadline, connect) =>
                result.map_err(|_| CoreError::new("Timeout", "Connection establishment timed out"))?,
        }
    }
    pub async fn discover(
        &self,
        voices: bool,
        language: Option<String>,
        timeout: Option<f64>,
        inherit: bool,
    ) -> Result<Vec<String>> {
        let timeout = if inherit {
            self.default_timeout
        } else {
            timeout
        };
        // Adapters reject new calls after close. A future that reached the core
        // has already started, even if close happened before its first poll.
        if self.stopped.is_cancelled() {
            return Err(CoreError::cancelled());
        }
        policy::timeout(timeout)?;
        let budget = Duration::from_secs_f64(
            timeout
                .unwrap_or(self.policy.discovery_timeout)
                .min(self.policy.discovery_timeout),
        );
        let deadline = tokio::time::Instant::now() + budget;
        let request_id = Mutex::new(None);
        let operation = async {
            let mut previous_id = None;
            for attempt in 0..3 {
                *request_id.lock().unwrap() = previous_id.take();
                let result = if voices {
                    self.unary::<_, GetSupportedSpeakersResponse>(
                        GetSupportedSpeakersRequest {
                            language: language.clone(),
                        },
                        "/rime.TextToSpeech/GetSupportedSpeakers",
                        &request_id,
                    )
                    .await
                    .map(|r| r.speakers)
                } else {
                    self.unary::<_, GetSupportedLanguagesResponse>(
                        GetSupportedLanguagesRequest {},
                        "/rime.TextToSpeech/GetSupportedLanguages",
                        &request_id,
                    )
                    .await
                    .map(|r| r.languages)
                };
                match result {
                    Ok(names) => return Ok(names),
                    Err(error) => {
                        if error.kind != "Unavailable" || attempt == 2 {
                            return Err(error);
                        }
                        previous_id = error.request_id;
                        *request_id.lock().unwrap() = previous_id.clone();
                        tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
                    }
                }
            }
            unreachable!()
        };
        tokio::select! {
            biased;
            _ = self.stopped.cancelled() => Err(CoreError::new("Cancelled","Client closed during discovery")),
            result = tokio::time::timeout_at(deadline,operation) => result.unwrap_or_else(|_| {
                let mut error = CoreError::new("Timeout","Discovery deadline expired");
                error.request_id = request_id.lock().unwrap().clone();
                Err(error)
            }),
        }
    }
    // A unary response uses the same HTTP/2 framing as a one-message stream.
    // Reading headers separately retains the ID if the body stalls past the deadline.
    async fn unary<Req, Res>(
        &self,
        message: Req,
        path: &'static str,
        id: &Mutex<Option<String>>,
    ) -> Result<Res>
    where
        Req: prost::Message + Default + Send + Sync + 'static,
        Res: prost::Message + Default + Send + Sync + 'static,
    {
        let channel = self.ready_channel().await?;
        let mut grpc =
            tonic::client::Grpc::new(ReceiveLimit::new(channel, self.policy.receive_bytes))
                .max_decoding_message_size(self.policy.receive_bytes);
        grpc.ready()
            .await
            .map_err(|_| CoreError::new("Unavailable", "Discovery transport unavailable"))?;
        let result = async {
            *id.lock().unwrap() = None;
            let response = grpc
                .server_streaming(
                    self.request(message)?,
                    tonic::codegen::http::uri::PathAndQuery::from_static(path),
                    tonic_prost::ProstCodec::<Req, Res>::default(),
                )
                .await
                .map_err(CoreError::status)?;
            *id.lock().unwrap() = crate::error::request_id(response.metadata());
            let mut body = response.into_inner();
            let value = body.message().await.map_err(CoreError::status)?;
            if body.message().await.map_err(CoreError::status)?.is_some() {
                return Err(CoreError::new(
                    "Stream",
                    "Discovery returned more than one response",
                ));
            }
            if let Some(trailers) = body.trailers().await.map_err(CoreError::status)? {
                let mut stored = id.lock().unwrap();
                if stored.is_none() {
                    *stored = crate::error::request_id(&trailers);
                }
            }
            value.ok_or_else(|| CoreError::new("Stream", "Discovery returned no response"))
        }
        .await;
        result.map_err(|mut e: CoreError| {
            e.request_id = id.lock().unwrap().clone().or(e.request_id);
            e
        })
    }

    pub fn cancel(&self) {
        self.stopped.cancel();
        for stream in self
            .streams
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
        {
            stream.cancel();
        }
        self.key.lock().unwrap().clear();
    }
    pub async fn close(&self) {
        self.cancel();
        self.channel.lock().await.take();
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.stopped.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn discovery_queued_before_close_returns_cancellation() {
        for voices in [false, true] {
            let client = Client::new(ClientConfig {
                api_key: "test".into(),
                model: "coda".into(),
                endpoint: None,
                timeout: None,
            })
            .unwrap();
            client.ensure_open().unwrap();
            let pending = client.discover(voices, None, None, true);
            client.close().await;
            assert_eq!(pending.await.unwrap_err().kind, "Cancelled");
            assert_eq!(client.ensure_open().unwrap_err().kind, "Input");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn connection_deadline_includes_channel_lock_wait() {
        let client = Client::new(ClientConfig {
            api_key: "test".into(),
            model: "coda".into(),
            endpoint: None,
            timeout: None,
        })
        .unwrap();
        let _lock = client.channel.lock().await;
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(11), client.connection())
            .await
            .expect("connection budget must expire while the lock is held");
        assert_eq!(result.err().unwrap().kind, "Timeout");
        assert_eq!(start.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn client_cancellation_interrupts_channel_lock_wait() {
        let client = Client::new(ClientConfig {
            api_key: "test".into(),
            model: "coda".into(),
            endpoint: None,
            timeout: None,
        })
        .unwrap();
        let _lock = client.channel.lock().await;
        let cancel = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            client.cancel();
        };
        let (result, ()) = tokio::join!(client.connection(), cancel);
        assert_eq!(result.err().unwrap().kind, "Cancelled");
    }

    #[tokio::test]
    async fn discovery_read_after_close_returns_cancellation() {
        let client = Client::new(ClientConfig {
            api_key: "test".into(),
            model: "coda".into(),
            endpoint: None,
            timeout: None,
        })
        .unwrap();
        client.close().await;
        // A ready discovery retry can be polled before the select cancellation arm.
        let error = client
            .unary::<_, GetSupportedLanguagesResponse>(
                GetSupportedLanguagesRequest {},
                "/rime.TextToSpeech/GetSupportedLanguages",
                &Mutex::new(None),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, "Cancelled");
    }
}
