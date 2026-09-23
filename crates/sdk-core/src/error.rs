use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreError {
    pub kind: String,
    pub message: String,
    pub request_id: Option<String>,
}

impl CoreError {
    pub fn new(kind: &str, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            request_id: None,
        }
    }
    pub fn input(message: impl Into<String>) -> Self {
        Self::new("Input", message)
    }
    pub fn cancelled() -> Self {
        Self::new("Cancelled", "Synthesis cancelled")
    }
    pub fn json(&self) -> String {
        serde_json::to_string(self).expect("serializable error")
    }
    pub fn status(status: tonic::Status) -> Self {
        use tonic::Code;
        let kind = match status.code() {
            Code::Unauthenticated => "Authentication",
            Code::PermissionDenied => "Permission",
            Code::InvalidArgument => "Input",
            Code::ResourceExhausted => "ResourceLimit",
            Code::Unavailable => "Unavailable",
            Code::DeadlineExceeded => "Timeout",
            Code::Cancelled => "Cancelled",
            _ => "Stream",
        };
        let mut error = Self::new(kind, format!("Rime operation failed: {:?}", status.code()));
        error.request_id = request_id(status.metadata());
        error
    }
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for CoreError {}
pub type Result<T> = std::result::Result<T, CoreError>;
pub fn request_id(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    metadata
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_status_contract() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../../../conformance/contract.json")).unwrap();
        for code in 1..=16 {
            let code = tonic::Code::from_i32(code);
            let key = match code {
                tonic::Code::Cancelled => "CANCELLED",
                tonic::Code::Unknown => "UNKNOWN",
                tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
                tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
                tonic::Code::NotFound => "NOT_FOUND",
                tonic::Code::AlreadyExists => "ALREADY_EXISTS",
                tonic::Code::PermissionDenied => "PERMISSION_DENIED",
                tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
                tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
                tonic::Code::Aborted => "ABORTED",
                tonic::Code::OutOfRange => "OUT_OF_RANGE",
                tonic::Code::Unimplemented => "UNIMPLEMENTED",
                tonic::Code::Internal => "INTERNAL",
                tonic::Code::Unavailable => "UNAVAILABLE",
                tonic::Code::DataLoss => "DATA_LOSS",
                tonic::Code::Unauthenticated => "UNAUTHENTICATED",
                _ => unreachable!(),
            };
            if let Some(expected) = value["grpc_errors"][key].as_str() {
                assert_eq!(
                    format!(
                        "Rime{}Error",
                        CoreError::status(tonic::Status::new(code, "private")).kind
                    ),
                    expected
                );
            }
        }
    }
}
