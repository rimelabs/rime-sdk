use super::{session, types::*};
use crate::{Client, Error, ErrorKind};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::http::Uri;

/// Prism operations on a parent client.
pub struct Realtime<'a> {
    pub(crate) client: &'a Client,
}
pub(super) fn text(value: &str, limit: usize) -> Result<(), Error> {
    if value.trim().is_empty() || value.chars().count() > limit {
        Err(Error::new(
            ErrorKind::Input,
            "text must be nonblank and within the character limit",
        ))
    } else {
        Ok(())
    }
}
pub(super) fn request_metadata(labels: &Labels, reserved: &[(&str, &str)]) -> Result<Value, Error> {
    if labels.keys().any(|key| key.starts_with("prism_")) {
        return Err(Error::new(
            ErrorKind::Input,
            "request metadata keys cannot start with prism_",
        ));
    }
    let mut metadata = json!(labels);
    let object = metadata.as_object_mut().expect("labels form an object");
    for (key, value) in reserved {
        object.insert((*key).to_owned(), json!(value));
    }
    Ok(metadata)
}
fn tool_settings(definitions: &[ToolDefinition]) -> Result<Vec<Value>, Error> {
    let mut tools = vec![];
    let mut names = std::collections::HashSet::new();
    for t in definitions {
        text(&t.name, 4000)?;
        if !names.insert(&t.name) {
            return Err(Error::new(ErrorKind::Input, "tool names must be unique"));
        }
        let mut v = json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}});
        if let Some(f) = &t.prism_filler {
            v["prism_filler"] = match f {
                PrismFiller::None => json!({"type":"none"}),
                PrismFiller::Text(s) => {
                    text(s, 500)?;
                    json!({"type":"text","text":s})
                }
                PrismFiller::Auto(s) => {
                    let mut v = json!({"type":"auto"});
                    if let Some(s) = s {
                        text(s, 500)?;
                        v["instructions"] = json!(s)
                    }
                    v
                }
            }
        }
        tools.push(v)
    }
    Ok(tools)
}

pub(super) fn truncation(value: &Truncation) -> Result<Value, Error> {
    match value {
        Truncation::Auto => Ok(json!("auto")),
        Truncation::Disabled => Ok(json!("disabled")),
        Truncation::RetentionRatio(r) if *r > 0.0 && *r <= 1.0 => {
            Ok(json!({"type":"retention_ratio", "retention_ratio":r}))
        }
        _ => Err(Error::new(
            ErrorKind::Input,
            "retention ratio must be above zero and at most one",
        )),
    }
}
pub(super) fn patch(o: RealtimeUpdateOptions) -> Result<Value, Error> {
    let mut result = json!({});
    if let Some(value) = &o.truncation {
        result["truncation"] = truncation(value)?;
    }
    if let Some(value) = o.instructions {
        result["instructions"] = json!(value)
    }
    if let Some(value) = o.voice {
        result["voice"] = json!(value)
    }
    if let Some(value) = o.tools {
        result["tools"] = json!(tool_settings(&value)?)
    }
    if let Some(value) = o.interrupt {
        result["turn_detection"] = json!({"interrupt_response": value})
    }
    Ok(result)
}

pub(super) fn settings(o: &RealtimeConnectOptions) -> Result<Value, Error> {
    for d in [o.timeouts.connect, o.timeouts.ready, o.timeouts.request] {
        crate::client::validate_timeout(Some(d))?;
    }
    let u: Uri = o
        .endpoint
        .parse()
        .map_err(|_| Error::new(ErrorKind::Input, "invalid Prism endpoint"))?;
    let host = u.host().unwrap_or_default();
    if !matches!(u.scheme_str(), Some("ws" | "wss"))
        || host.is_empty()
        || u.authority().is_some_and(|a| a.as_str().contains('@'))
        || (u.port().is_some() && u.port_u16().is_none())
        || o.endpoint.contains('#')
        || u.path().trim_end_matches('/') != "/v1/realtime"
    {
        return Err(Error::new(
            ErrorKind::Input,
            "endpoint must be a ws or wss URL ending in /v1/realtime",
        ));
    }
    if u.scheme_str() == Some("ws") && !matches!(host, "localhost" | "127.0.0.1" | "[::1]") {
        return Err(Error::new(ErrorKind::Input, "use wss outside localhost"));
    }
    let mut d = json!({"interrupt_response":o.interrupt});
    if let Some(s) = &o.detection.eagerness {
        if !["low", "medium", "high", "auto"].contains(&s.as_str()) {
            return Err(Error::new(ErrorKind::Input, "invalid turn eagerness"));
        }
        d["eagerness"] = json!(s)
    }
    if let Some(n) = o.detection.end_of_turn_timeout_ms {
        if !(1000..=10000).contains(&n) {
            return Err(Error::new(
                ErrorKind::Input,
                "turn timeout must be 1000 to 10000 ms",
            ));
        }
        d["prism_end_of_turn_timeout_ms"] = json!(n)
    }
    let tools = tool_settings(&o.tools)?;
    if let Some(id) = &o.request_id {
        if id.trim().is_empty() || !id.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
            return Err(Error::new(
                ErrorKind::Input,
                "request ID must be nonblank printable ASCII",
            ));
        }
    }
    let mut result = json!({"modalities":["text","audio"],"input_audio_format":"pcm16","turn_detection":d,"tools":tools});
    if let Some(value) = &o.truncation {
        result["truncation"] = truncation(value)?;
    }
    if let Some(labels) = &o.response_metadata {
        result["prism_response_metadata"] = json!(labels)
    }
    for (key, value) in [("voice", &o.voice), ("instructions", &o.instructions)] {
        if let Some(s) = value {
            text(s, usize::MAX)?;
            result[key] = json!(s)
        }
    }
    Ok(result)
}
impl Realtime<'_> {
    /// Authenticate and initialize a Prism session. Dropping this future requests cleanup.
    pub async fn connect(
        &self,
        options: RealtimeConnectOptions,
    ) -> Result<super::RealtimeSession, Error> {
        crate::client::require_runtime()?;
        self.client.ensure_open()?;
        let settings = settings(&options)?;
        session::connect(self.client.inner.clone(), options, settings).await
    }
}
