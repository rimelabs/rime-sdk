use crate::{Error, ErrorKind};
use rimelabs_api::{SynthesisResponseTrailer, Timestamps};

/// A spoken, normalized word with offsets in seconds from synthesis start.
/// Offsets are independent of the output audio sample rate.
#[derive(Clone, Debug, PartialEq)]
pub struct WordTimestamp {
    /// The normalized word spoken by the service.
    pub text: String,
    /// Start offset in seconds.
    pub start: f64,
    /// End offset in seconds.
    pub end: f64,
}

/// Alignment status, independent of audio synthesis success.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimestampStatus {
    /// The service's google.rpc.Code: zero means OK.
    pub code: i32,
    /// The service's explanation, typically empty on success.
    pub message: String,
}

/// Final alignment status and word timings. Non-OK statuses carry no spans.
#[derive(Clone, Debug, PartialEq)]
pub struct TimestampResult {
    /// Whether alignment succeeded.
    pub status: TimestampStatus,
    /// Spoken words with offsets from the beginning of the entire synthesis.
    pub spans: Vec<WordTimestamp>,
}

#[derive(Default)]
pub(crate) struct TimestampTrailer {
    pub(crate) seen: bool,
    pub(crate) invalid: bool,
    value: Option<Timestamps>,
}

impl TimestampTrailer {
    pub(crate) fn accept(&mut self, trailer: SynthesisResponseTrailer) {
        if self.seen {
            self.invalid = true;
        } else {
            self.seen = true;
            self.value = trailer.timestamps;
        }
    }

    pub(crate) fn result(&self, request_id: Option<&str>) -> Result<TimestampResult, Error> {
        let malformed = || {
            Error::new(ErrorKind::Stream, "invalid timestamp trailer").with_request_id(request_id)
        };
        if self.invalid {
            return Err(malformed());
        }
        let value = self.value.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Stream,
                "service did not return requested timestamps",
            )
            .with_request_id(request_id)
        })?;
        let status = value.status.as_ref().ok_or_else(malformed)?;
        if status.code != 0 && !value.spans.is_empty() {
            return Err(malformed());
        }
        let seconds = |seconds: i64, nanos: i32| {
            if !(0..=315_576_000_000).contains(&seconds) || !(0..1_000_000_000).contains(&nanos) {
                return Err(malformed());
            }
            Ok(seconds as f64 + f64::from(nanos) / 1e9)
        };
        let mut spans = Vec::with_capacity(value.spans.len());
        for span in &value.spans {
            let start = span.start.as_ref().ok_or_else(malformed)?;
            let end = span.end.as_ref().ok_or_else(malformed)?;
            let start = seconds(start.seconds, start.nanos)?;
            let end = seconds(end.seconds, end.nanos)?;
            if span.text.trim().is_empty() || end < start {
                return Err(malformed());
            }
            spans.push(WordTimestamp {
                text: span.text.clone(),
                start,
                end,
            });
        }
        Ok(TimestampResult {
            status: TimestampStatus {
                code: status.code,
                message: status.message.clone(),
            },
            spans,
        })
    }
}
