mod runtime;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use sdk_core::{client::Client, error::CoreError, stream::AudioStream};
use std::sync::Arc;
fn error(e: CoreError) -> PyErr {
    PyValueError::new_err(e.json())
}
fn parse<T: serde::de::DeserializeOwned>(value: &str) -> PyResult<T> {
    serde_json::from_str(value).map_err(|_| error(CoreError::input("Invalid native options")))
}
#[pyclass(frozen)]
struct NativeClient {
    inner: Arc<Client>,
    runtime: Arc<runtime::Slot>,
    pid: u32,
}
#[pymethods]
impl NativeClient {
    #[new]
    fn new(config: &str) -> PyResult<Self> {
        Ok(Self {
            inner: Client::new(parse(config)?).map_err(error)?,
            runtime: Arc::new(runtime::Slot::default()),
            pid: std::process::id(),
        })
    }
    #[cfg(feature = "test-support")]
    #[staticmethod]
    fn testing(config: &str, target: String, policy: &str) -> PyResult<Self> {
        Ok(Self {
            inner: Client::testing(parse(config)?, target, parse(policy)?).map_err(error)?,
            runtime: Arc::new(runtime::Slot::default()),
            pid: std::process::id(),
        })
    }
    fn stream(&self, options: &str) -> PyResult<NativeStream> {
        Ok(NativeStream {
            inner: self.inner.stream(parse(options)?).map_err(error)?,
            runtime: self.runtime.clone(),
            pid: self.pid,
        })
    }
    fn discover<'p>(
        &self,
        py: Python<'p>,
        voices: bool,
        language: Option<String>,
        timeout: Option<f64>,
        inherit: bool,
    ) -> PyResult<Bound<'p, PyAny>> {
        let client = self.inner.clone();
        self.runtime.future(py, async move {
            client
                .discover(voices, language, timeout, inherit)
                .await
                .map_err(error)
        })
    }
    fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let client = self.inner.clone();
        client.cancel();
        self.runtime.future(py, async move {
            client.close().await;
            Ok(())
        })
    }
    fn cancel(&self) {
        self.inner.cancel();
    }
}
#[pyclass(frozen)]
struct NativeStream {
    inner: Arc<AudioStream>,
    runtime: Arc<runtime::Slot>,
    pid: u32,
}
#[pymethods]
impl NativeStream {
    fn start(&self) -> PyResult<()> {
        self.runtime.enter(|| self.inner.activate().map_err(error))
    }
    #[getter]
    fn request_id(&self) -> Option<String> {
        self.inner.request_id()
    }
    #[getter]
    fn input_done(&self) -> bool {
        self.inner.input_done()
    }
    #[getter]
    fn source_chars(&self) -> usize {
        self.inner.source_chars()
    }
    #[getter]
    fn cleanup_timeout(&self) -> f64 {
        self.inner.cleanup_timeout()
    }
    fn wait_produced<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.inner.clone();
        self.runtime.future(py, async move {
            stream.wait_produced().await;
            Ok(())
        })
    }
    fn cancel(&self) {
        self.inner.cancel();
    }
    fn fail_source(&self) {
        self.inner.fail_source();
    }
    fn wait_stopped<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.inner.clone();
        self.runtime.future(py, async move {
            stream.wait_stopped().await;
            Ok(())
        })
    }
    fn accept_read(&self, ticket: u32) -> PyResult<()> {
        self.inner.accept_read(ticket).map_err(error)
    }
    fn input_reply(&self, value: &str) -> PyResult<()> {
        self.inner.input_reply(parse(value)?).map_err(error)
    }
    fn input_request<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.inner.clone();
        self.runtime
            .future(py, async move { Ok(stream.input_request().await) })
    }
    fn read<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.inner.clone();
        self.runtime.future(py, async move {
            let result = stream.next().await.map_err(error)?;
            Ok(Python::attach(|py| {
                (
                    result.data.map(|b| PyBytes::new(py, &b).unbind()),
                    result.ticket,
                )
            }))
        })
    }
}
#[pyclass]
struct SentenceBuffer {
    inner: sdk_core::sentences::SentenceBuffer,
}
#[pymethods]
impl SentenceBuffer {
    #[new]
    fn new(limit: usize) -> Self {
        Self {
            inner: sdk_core::sentences::SentenceBuffer::new(limit),
        }
    }
    #[getter]
    fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes()
    }
    #[getter]
    fn scans(&self) -> usize {
        self.inner.scans()
    }
    fn feed(&mut self, text: &str, final_input: bool) -> PyResult<Vec<String>> {
        self.inner.feed(text, final_input).map_err(error)
    }
}
#[pyclass]
struct Converter {
    inner: sdk_core::audio::Converter,
}
#[pymethods]
impl Converter {
    #[new]
    fn new(profile: &str) -> PyResult<Self> {
        Ok(Self {
            inner: sdk_core::audio::Converter::new(
                sdk_core::audio::AudioProfile::parse(profile).map_err(error)?,
            ),
        })
    }
    fn process<'p>(
        &mut self,
        py: Python<'p>,
        data: &[u8],
        final_input: bool,
    ) -> PyResult<Bound<'p, PyBytes>> {
        Ok(PyBytes::new(
            py,
            &self.inner.process(data, final_input).map_err(error)?,
        ))
    }
}
#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeClient>()?;
    module.add_class::<NativeStream>()?;
    module.add_class::<SentenceBuffer>()?;
    module.add_class::<Converter>()?;
    Ok(())
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            self.inner.cancel();
        }
    }
}
impl Drop for NativeStream {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            self.inner.cancel();
        }
    }
}
