use napi::{Error, bindgen_prelude::*};
use napi_derive::napi;
use sdk_core::{client::Client, error::CoreError, stream::AudioStream};
use std::sync::Arc;
fn error(e: CoreError) -> Error {
    Error::from_reason(e.json())
}
fn parse<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|_| error(CoreError::input("Invalid native options")))
}
#[napi]
pub struct NativeClient {
    inner: Arc<Client>,
}
#[napi]
impl NativeClient {
    #[napi(constructor)]
    pub fn new(config: String) -> Result<Self> {
        Ok(Self {
            inner: Client::new(parse(&config)?).map_err(error)?,
        })
    }
    #[napi]
    pub fn stream(&self, options: String) -> Result<NativeStream> {
        Ok(NativeStream {
            inner: self.inner.stream(parse(&options)?).map_err(error)?,
        })
    }
    #[napi]
    pub async fn discover(
        &self,
        voices: bool,
        language: Option<String>,
        timeout: Option<f64>,
        inherit: bool,
    ) -> Result<Vec<String>> {
        self.inner
            .discover(voices, language, timeout, inherit)
            .await
            .map_err(error)
    }
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await;
        Ok(())
    }
    #[napi]
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}
#[napi]
pub struct NativeStream {
    inner: Arc<AudioStream>,
}
#[napi(object)]
pub struct NativeRead {
    pub data: Option<Buffer>,
    pub ticket: u32,
}
#[napi]
impl NativeStream {
    #[napi]
    pub fn start(&self) -> Result<()> {
        let stream = self.inner.clone();
        napi::bindgen_prelude::within_runtime_if_available(move || stream.activate()).map_err(error)
    }
    #[napi(getter)]
    pub fn request_id(&self) -> Option<String> {
        self.inner.request_id()
    }
    #[napi]
    pub fn cancel(&self) {
        self.inner.cancel();
    }
    #[napi(getter)]
    pub fn source_chars(&self) -> u32 {
        self.inner.source_chars() as u32
    }
    #[napi(getter)]
    pub fn cleanup_timeout(&self) -> f64 {
        self.inner.cleanup_timeout()
    }
    #[napi]
    pub async fn wait_produced(&self) {
        self.inner.wait_produced().await;
    }
    #[napi]
    pub fn fail_source(&self) {
        self.inner.fail_source();
    }
    #[napi]
    pub async fn wait_stopped(&self) {
        self.inner.wait_stopped().await;
    }
    #[napi]
    pub fn accept_read(&self, ticket: u32) -> Result<()> {
        self.inner.accept_read(ticket).map_err(error)
    }
    #[napi]
    pub fn input_reply(&self, value: String) -> Result<()> {
        self.inner.input_reply(parse(&value)?).map_err(error)
    }
    #[napi]
    pub async fn input_request(&self) -> Option<u32> {
        self.inner.input_request().await
    }
    #[napi]
    pub async fn read(&self) -> Result<NativeRead> {
        let result = self.inner.next().await.map_err(error)?;
        Ok(NativeRead {
            data: result.data.map(|b| Buffer::from(b.to_vec())),
            ticket: result.ticket,
        })
    }
}
#[napi]
pub struct SentenceBuffer {
    inner: sdk_core::sentences::SentenceBuffer,
}
#[napi]
impl SentenceBuffer {
    #[napi(constructor)]
    pub fn new(limit: u32) -> Self {
        Self {
            inner: sdk_core::sentences::SentenceBuffer::new(limit as usize),
        }
    }
    #[napi]
    pub fn feed(&mut self, text: Utf16String, final_input: bool) -> Result<Vec<String>> {
        self.inner
            .feed_utf16(text.to_vec(), final_input)
            .map_err(error)
    }
    #[napi(getter)]
    pub fn retained_bytes(&self) -> u32 {
        self.inner.retained_bytes() as u32
    }
    #[napi(getter)]
    pub fn scans(&self) -> u32 {
        self.inner.scans() as u32
    }
}
#[napi]
pub struct Converter {
    inner: sdk_core::audio::Converter,
}
#[napi]
impl Converter {
    #[napi(constructor)]
    pub fn new(profile: String) -> Result<Self> {
        Ok(Self {
            inner: sdk_core::audio::Converter::new(
                sdk_core::audio::AudioProfile::parse(&profile).map_err(error)?,
            ),
        })
    }
    #[napi]
    pub fn process(&mut self, data: Buffer, final_input: bool) -> Result<Buffer> {
        Ok(self
            .inner
            .process(&data, final_input)
            .map_err(error)?
            .into())
    }
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        self.inner.cancel();
    }
}
impl Drop for NativeStream {
    fn drop(&mut self) {
        self.inner.cancel();
    }
}

#[cfg(feature = "test-support")]
#[napi]
impl NativeClient {
    #[napi(factory)]
    pub fn testing(config: String, target: String, policy: String) -> Result<Self> {
        Ok(Self {
            inner: Client::testing(parse(&config)?, target, parse(&policy)?).map_err(error)?,
        })
    }
}
