use std::{error::Error as StdError, fmt, sync::Arc};

/// Stable categories for application error handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Missing or rejected credentials.
    Authentication,
    /// Credentials lack permission.
    Permission,
    /// Invalid configuration or text input.
    Input,
    /// A server or local resource limit was exceeded.
    ResourceLimit,
    /// The service could not be reached or is unavailable.
    Unavailable,
    /// An operation or progress deadline expired.
    Timeout,
    /// The client or audio stream was cancelled.
    Cancelled,
    /// Audio does not match the required PCM format.
    AudioFormat,
    /// A protocol or stream failure.
    Stream,
}

/// An SDK error, with the service request ID when available.
#[derive(Clone, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: &'static str,
    request_id: Option<String>,
    source: Option<Arc<dyn StdError + Send + Sync>>,
}

impl Error {
    /// Report a failure from an application-provided text source.
    pub fn input(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(ErrorKind::Input, "text source failed").caused_by(source)
    }

    pub(crate) fn new(kind: ErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            message,
            request_id: None,
            source: None,
        }
    }

    pub(crate) fn caused_by(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    pub(crate) fn with_request_id(mut self, id: Option<&str>) -> Self {
        if self.request_id.is_none() {
            self.request_id = id.map(str::to_owned);
        }
        self
    }

    /// The category of this failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
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
        Self::new(kind, "speech service request failed")
            .with_request_id(request_id(status.metadata()))
            .caused_by(status)
    }
}

pub(crate) fn request_id(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    metadata
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
}
