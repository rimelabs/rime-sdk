use std::{borrow::Cow, error::Error as StdError, fmt, sync::Arc, time::Duration};

/// Stable categories for application error handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Missing or rejected credentials.
    Authentication,
    /// Credentials lack permission.
    Permission,
    /// Invalid configuration or source input.
    Input,
    /// A server or local resource limit was exceeded.
    ResourceLimit,
    /// The service could not be reached or is unavailable.
    Unavailable,
    /// An operation or progress deadline expired.
    Timeout,
    /// The client or stream was cancelled.
    Cancelled,
    /// Invalid or mismatched audio format.
    AudioFormat,
    /// A structured Prism service fault. Inspect Error::realtime_fault for details.
    Realtime,
    /// A protocol or stream failure.
    Stream,
}

/// An SDK error, with the service request ID when available.
#[derive(Clone, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: Cow<'static, str>,
    request_id: Option<String>,
    source: Option<Arc<dyn StdError + Send + Sync>>,
    status_code: Option<u16>,
    close_code: Option<u16>,
    retry_after: Option<Duration>,
}

#[derive(Debug)]
struct RealtimeClosed;
impl fmt::Display for RealtimeClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("realtime session closed")
    }
}
impl StdError for RealtimeClosed {}

impl Error {
    /// Report cancellation from an application-owned adapter after its cancellation token fires.
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "operation cancelled")
    }
    pub(crate) fn realtime_closed() -> Self {
        Self::new(ErrorKind::Cancelled, "realtime session closed").caused_by(RealtimeClosed)
    }
    pub(crate) fn is_realtime_closed(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|s| s.is::<RealtimeClosed>())
    }

    /// Report a failure from an application-provided text or audio source.
    pub fn input(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(ErrorKind::Input, "input source failed").caused_by(source)
    }

    pub(crate) fn new(kind: ErrorKind, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            message: message.into(),
            request_id: None,
            source: None,
            status_code: None,
            close_code: None,
            retry_after: None,
        }
    }

    pub(crate) fn caused_by(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// Refused realtime HTTP upgrade status, such as 503. None means unavailable.
    pub fn status_code(&self) -> Option<u16> {
        self.status_code
    }

    /// Received WebSocket close code, such as 1013. None means unavailable.
    pub fn close_code(&self) -> Option<u16> {
        self.close_code
    }

    /// Server-requested delay, including zero. None means absent or invalid.
    /// This does not initiate retries or make replay of accepted requests safe.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    pub(crate) fn with_connection_diagnostics(
        mut self,
        status: Option<u16>,
        close: Option<u16>,
        retry_after: Option<Duration>,
    ) -> Self {
        self.status_code = status;
        self.close_code = close;
        self.retry_after = retry_after;
        self
    }

    pub(crate) fn with_request_id(mut self, id: Option<&str>) -> Self {
        if self.request_id.is_none() {
            self.request_id = id.map(str::to_owned);
        }
        self
    }

    /// Structured Prism fault details, when supplied by the service.
    pub fn realtime_fault(&self) -> Option<&crate::realtime::RealtimeFault> {
        self.source.as_ref()?.downcast_ref()
    }

    /// Prism's idle limit when the session ended for inactivity.
    pub fn realtime_idle_timeout(&self) -> Option<&crate::realtime::RealtimeIdleTimeout> {
        self.source.as_ref()?.downcast_ref()
    }

    /// The category of this failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The failure explanation, preserving service-provided details when available.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The request ID supplied by the service, if present.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)?;
        if let Some(id) = &self.request_id {
            write!(formatter, " (request {id})")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|error| error as &(dyn StdError + 'static))
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        use tonic::Code;
        let kind = match status.code() {
            Code::Unauthenticated => ErrorKind::Authentication,
            Code::PermissionDenied => ErrorKind::Permission,
            Code::InvalidArgument => ErrorKind::Input,
            Code::ResourceExhausted => ErrorKind::ResourceLimit,
            Code::Unavailable => ErrorKind::Unavailable,
            Code::DeadlineExceeded => ErrorKind::Timeout,
            Code::Cancelled => ErrorKind::Cancelled,
            _ => ErrorKind::Stream,
        };
        let message = if status.message().trim().is_empty() {
            Cow::Borrowed("speech service request failed")
        } else {
            Cow::Owned(status.message().to_owned())
        };
        Self::new(kind, message)
            .with_request_id(request_id(status.metadata()))
            .caused_by(status)
    }
}

pub(crate) fn request_id(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    metadata
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
}
