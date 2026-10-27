use super::types::*;
use crate::{Error, ErrorKind, PcmFormat};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{Map, Value};

pub(super) struct Wire {
    pub kind: String,
    pub history: Vec<super::HistoryItem>,
    pub created_id: Option<String>,
    pub interrupt_known: bool,
    pub event_id: String,
    pub request_id: Option<String>,
    pub target: String,
    pub index: u64,
    pub token: Option<String>,
    pub info: Option<SessionInfo>,
    pub payload: Option<RealtimeEvent>,
}
fn invalid(field: &str) -> Error {
    Error::new(
        ErrorKind::Stream,
        format!("invalid realtime field: {field}"),
    )
}
fn object(v: &Value) -> Result<&Map<String, Value>, Error> {
    v.as_object().ok_or_else(|| invalid("object"))
}
fn string(v: &Value) -> Result<String, Error> {
    v.as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("string"))
}
fn id(v: &Value) -> Result<String, Error> {
    let s = string(v)?;
    if s.is_empty() {
        return Err(invalid("id"));
    }
    Ok(s)
}
fn optional(v: &Value) -> Result<Option<String>, Error> {
    if v.is_null() {
        Ok(None)
    } else {
        Ok(Some(string(v)?))
    }
}
fn integer(v: &Value) -> Result<u64, Error> {
    v.as_f64()
        .filter(|n| {
            n.is_finite() && (0.0..=9_007_199_254_740_991.0).contains(n) && n.fract() == 0.0
        })
        .map(|n| n as u64)
        .ok_or_else(|| invalid("integer"))
}
/// The client's own labels in `metadata`: string values under keys without
/// Prism's `prism_` prefix.
fn labels(metadata: &Value) -> Labels {
    metadata
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| !k.starts_with("prism_"))
        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
        .collect()
}
fn enumeration(s: String, known: &[&str]) -> String {
    if known.contains(&s.as_str()) {
        s
    } else {
        "unknown".into()
    }
}
fn revision(v: &Value) -> Result<Option<u64>, Error> {
    v.get("prism_settings_revision").map(integer).transpose()
}
fn response_truncation(response: &Value) -> Result<Option<ResponseTruncation>, Error> {
    let Some(value) = response.get("prism_truncation") else {
        return Ok(None);
    };
    object(value)?;
    let count = integer(&value["items_left_out"])?;
    if count == 0 {
        return Err(invalid("prism_truncation.items_left_out"));
    }
    Ok(Some(ResponseTruncation {
        items_left_out: count,
        first_kept_item_id: value.get("first_kept_item_id").map(string).transpose()?,
    }))
}
fn view(v: &Value) -> Result<SessionInfo, Error> {
    let o = object(v)?;
    let mut s = SessionInfo {
        truncation: o
            .get("truncation")
            .map(|value| {
                let t = match value.as_str() {
                    Some("auto") => Truncation::Auto,
                    Some("disabled") => Truncation::Disabled,
                    _ if value["type"] == "retention_ratio" => Truncation::RetentionRatio(
                        value["retention_ratio"]
                            .as_f64()
                            .ok_or_else(|| invalid("session.truncation"))?,
                    ),
                    _ => return Err(invalid("session.truncation")),
                };
                super::client::truncation(&t).map_err(|_| invalid("session.truncation"))?;
                Ok(t)
            })
            .transpose()?,
        settings_revision: revision(v)?,
        model: "prism",
        session_id: id(&v["id"])?,
        voice: String::new(),
        interrupt_on_speech: true,
        tool_result_timeout_s: None,
        tool_continuation_timeout_s: None,
        eagerness: None,
        end_of_turn_timeout_ms: None,
        request_id: optional(&v["request_id"])?,
        response_metadata: None,
    };
    if let Some(v) = o.get("voice") {
        s.voice = string(v)?
    }
    if let Some(v) = o.get("prism_response_metadata") {
        object(v)?;
        s.response_metadata = Some(labels(v))
    }
    if let Some(v) = o.get("prism_effective_interrupt_response") {
        s.interrupt_on_speech = v.as_bool().ok_or_else(|| invalid("interrupt"))?
    }
    if let Some(v) = o.get("turn_detection") {
        let d = object(v)?;
        if let Some(v) = d.get("eagerness") {
            s.eagerness = Some(enumeration(string(v)?, &["low", "medium", "high", "auto"]))
        }
        if let Some(v) = d.get("prism_end_of_turn_timeout_ms") {
            let n = integer(v)?;
            if !(1000..=10000).contains(&n) {
                return Err(invalid("turn timeout"));
            }
            s.end_of_turn_timeout_ms = Some(n)
        }
    }
    if let Some(v) = o.get("prism_tool_waits") {
        let d = object(v)?;
        let duration = |key| -> Result<Option<f64>, Error> {
            let v = d.get(key).ok_or_else(|| invalid(key))?;
            if v.is_null() {
                Ok(None)
            } else {
                v.as_f64()
                    .filter(|n| n.is_finite() && *n > 0.0)
                    .map(Some)
                    .ok_or_else(|| invalid(key))
            }
        };
        s.tool_result_timeout_s = duration("result_timeout_s")?;
        s.tool_continuation_timeout_s = duration("continuation_timeout_s")?
    }
    Ok(s)
}
fn fault(v: &Value) -> Result<RealtimeFault, Error> {
    let o = object(v)?;
    let owner = if v["owner"].is_null() {
        None
    } else {
        Some(object(&v["owner"])?)
    };
    let field = |k| -> Result<Option<String>, Error> {
        optional(owner.and_then(|o| o.get(k)).unwrap_or(&Value::Null))
    };
    let calls = match owner.and_then(|o| o.get("call_ids")) {
        None | Some(Value::Null) => vec![],
        Some(Value::Array(a)) => a.iter().map(id).collect::<Result<_, _>>()?,
        _ => return Err(invalid("call_ids")),
    };
    Ok(RealtimeFault {
        code: o
            .get("code")
            .map(string)
            .transpose()?
            .unwrap_or_else(|| "unknown".into()),
        message: o
            .get("message")
            .map(string)
            .transpose()?
            .unwrap_or_default(),
        scope: enumeration(
            o.get("scope").map(string).transpose()?.unwrap_or_default(),
            &[
                "event",
                "utterance",
                "response",
                "tool_roundtrip",
                "session",
            ],
        ),
        request_id: field("event_id")?,
        response_id: field("response_id")?,
        item_id: field("item_id")?,
        call_ids: calls,
        correlation_id: optional(&v["correlation_id"])?,
        parameter: optional(&v["param"])?,
    })
}
pub(super) fn fault_error(f: RealtimeFault) -> Error {
    Error::new(ErrorKind::Realtime, f.to_string())
        .with_request_id(f.request_id.as_deref())
        .caused_by(f)
}
pub(super) fn decode(raw: &str, session_id: &str) -> Result<Option<Wire>, Error> {
    let v: Value = serde_json::from_str(raw).map_err(|_| invalid("JSON"))?;
    let o = object(&v)?;
    let k = string(&v["type"])?;
    let mut w = Wire {
        interrupt_known: false,
        kind: String::new(),
        history: vec![],
        created_id: None,
        event_id: String::new(),
        request_id: None,
        target: String::new(),
        index: 0,
        token: None,
        info: None,
        payload: None,
    };
    let reference = |v: &Value| -> Result<ResponseRef, Error> {
        if session_id.is_empty() {
            return Err(invalid("event before session.created"));
        }
        Ok(ResponseRef {
            session_id: session_id.into(),
            response_id: id(v)?,
        })
    };
    use RealtimeEvent::*;
    match k.as_str() {
        "session.created" | "session.updated" => {
            w.kind = k;
            w.info = Some(view(&v["session"])?);
            w.interrupt_known = v["session"]
                .get("prism_effective_interrupt_response")
                .is_some()
        }
        "conversation.item.created" => {
            w.created_id = optional(&v["item"]["id"])?;
            let d = object(&v["item"])?;
            if v["item"]["type"] == "function_call_output" {
                w.kind = "tool.result.created".into();
                w.target = id(&v["item"]["call_id"])?
            } else {
                if d.get("type")
                    .is_some_and(|v| v != "message" && v != "function_call")
                {
                    return Err(invalid("item.type"));
                }
                w.kind = "item.created".into();
                w.target = id(&v["item"]["id"])?
            }
        }
        "prism.conversation" => {
            w.kind = "history.listed".into();
            w.history = v["items"]
                .as_array()
                .ok_or_else(|| invalid("items"))?
                .iter()
                .map(super::history::listed)
                .collect::<Result<_, _>>()?;
        }
        "conversation.item.deleted" => {
            w.kind = "item.deleted".into();
            w.target = id(&v["item_id"])?;
        }
        "input_audio_buffer.cleared" => w.kind = "audio.cleared".into(),
        "conversation.item.truncated" => {
            w.kind = "item.truncated".into();
            w.target = id(&v["item_id"])?;
            w.index = integer(&v["content_index"])?;
            w.payload = Some(ItemTruncated {
                item_id: w.target.clone(),
                content_index: w.index,
                audio_end_ms: integer(&v["audio_end_ms"])?,
                kept_text: v.get("prism_kept_text").map(string).transpose()?,
            });
        }
        "prism.typed_input.ready" => {
            w.kind = "input.ready".into();
            w.payload = Some(InputReady)
        }
        "prism.input_audio_buffer.speech_hint" => {
            w.kind = "speech.hint".into();
            w.payload = Some(SpeechHint {
                audio_start_ms: integer(&v["audio_start_ms"])?,
            })
        }
        "input_audio_buffer.speech_started" => {
            w.kind = "speech.started".into();
            w.payload = Some(SpeechStarted {
                item_id: id(&v["item_id"])?,
                audio_start_ms: integer(&v["audio_start_ms"])?,
            })
        }
        "input_audio_buffer.speech_stopped" => {
            w.kind = "speech.stopped".into();
            w.payload = Some(SpeechStopped {
                item_id: id(&v["item_id"])?,
                audio_end_ms: integer(&v["audio_end_ms"])?,
                end_reason: o
                    .get("prism_end_reason")
                    .map(|v| {
                        string(v).map(|s| {
                            enumeration(
                                s,
                                &[
                                    "semantic",
                                    "timeout",
                                    "silence",
                                    "max_duration",
                                    "detector_changed",
                                ],
                            )
                        })
                    })
                    .transpose()?,
            })
        }
        "input_audio_buffer.committed" => {
            w.kind = "input.committed".into();
            w.payload = Some(InputCommitted {
                item_id: id(&v["item_id"])?,
            })
        }
        "conversation.item.input_audio_transcription.delta" => {
            w.kind = "transcript.delta".into();
            w.payload = Some(TranscriptDelta {
                item_id: id(&v["item_id"])?,
                delta: string(&v["delta"])?,
            })
        }
        "conversation.item.input_audio_transcription.completed" => {
            w.kind = "transcript.final".into();
            w.payload = Some(TranscriptFinal {
                item_id: id(&v["item_id"])?,
                text: string(&v["transcript"])?,
            })
        }
        "conversation.item.input_audio_transcription.failed" => {
            w.kind = "transcript.failed".into();
            let item_id = id(&v["item_id"])?;
            let f = fault(
                &serde_json::json!({"code":string(&v["error"]["code"])?,"message":string(&v["error"]["message"])?,"scope":"utterance","owner":{"item_id":item_id}}),
            )?;
            w.payload = Some(TranscriptFailed { item_id, fault: f })
        }
        "response.created" | "response.done" => {
            let d = &v["response"];
            object(d)?;
            let response = reference(&d["id"])?;
            let m = &d["metadata"];
            if !m.is_null() {
                object(m)?;
            }
            if k == "response.created" {
                w.kind = "response.started".into();
                let cause = match m.get("prism_cause").map(string).transpose()?.as_deref() {
                    None => "speech",
                    Some("user_text") => "text",
                    Some("tool_continuation") => "tools",
                    Some("proactive") => "proactive",
                    _ => "unknown",
                }
                .into();
                let parent = if m["prism_parent_response_id"].is_null() {
                    None
                } else {
                    Some(reference(&m["prism_parent_response_id"])?)
                };
                let input_item_id = optional(&m["prism_input_item_id"])?;
                let input_item_ids = match &d["prism_input_item_ids"] {
                    Value::Null => input_item_id.iter().cloned().collect(),
                    Value::Array(a) => a.iter().map(id).collect::<Result<_, _>>()?,
                    _ => return Err(invalid("prism_input_item_ids")),
                };
                w.payload = Some(ResponseStarted {
                    settings_revision: revision(d)?,
                    response,
                    cause,
                    parent,
                    input_item_id,
                    metadata: labels(m),
                    input_item_ids,
                });
            } else {
                w.kind = "response.ended".into();
                let details = &d["status_details"];
                if !details.is_null() {
                    object(details)?;
                }
                w.token = optional(&m["prism_drain_token"])?;
                w.payload = Some(ResponseEnded {
                    settings_revision: revision(d)?,
                    truncation: response_truncation(d)?,
                    response,
                    status: enumeration(
                        string(&d["status"])?,
                        &["completed", "cancelled", "failed"],
                    ),
                    reason: optional(&details["reason"])?,
                })
            }
        }
        "response.function_call_arguments.done" => {
            w.kind = "tool.call".into();
            let args: Value = serde_json::from_str(&string(&v["arguments"])?)
                .map_err(|_| invalid("arguments"))?;
            w.payload = Some(ToolCall {
                call: ToolCallRef {
                    response: reference(&v["response_id"])?,
                    call_id: id(&v["call_id"])?,
                },
                item_id: id(&v["item_id"])?,
                name: string(&v["name"])?,
                arguments: object(&args)?.clone(),
            })
        }
        "response.output_item.added"
        | "response.text.delta"
        | "response.text.done"
        | "response.audio.delta"
        | "response.audio.done" => {
            let (item_id, index) = if k == "response.output_item.added" {
                let d = &v["item"];
                object(d)?;
                if string(&d["type"])? != "message" {
                    return Ok(None);
                }
                (
                    id(&d["id"])?,
                    o.get("content_index")
                        .map(integer)
                        .transpose()?
                        .unwrap_or(0),
                )
            } else {
                (id(&v["item_id"])?, integer(&v["content_index"])?)
            };
            if index != 0 {
                return Err(invalid("content_index must be zero"));
            };
            let output = OutputRef {
                response: reference(&v["response_id"])?,
                item_id,
                output_index: integer(&v["output_index"])?,
                content_index: index,
            };
            let (kind, payload) = match k.as_str() {
                "response.output_item.added" => ("message.started", MessageStarted { output }),
                "response.text.delta" => (
                    "text.delta",
                    TextDelta {
                        output,
                        delta: string(&v["delta"])?,
                    },
                ),
                "response.text.done" => (
                    "text.done",
                    TextDone {
                        output,
                        text: string(&v["text"])?,
                    },
                ),
                "response.audio.done" => ("audio.done", AudioDone { output }),
                _ => {
                    let data = STANDARD
                        .decode(string(&v["delta"])?)
                        .map_err(|_| invalid("audio.delta"))?;
                    if data.len() % 2 != 0 {
                        return Err(invalid("audio.delta"));
                    }
                    (
                        "audio.delta",
                        AudioDelta {
                            output,
                            audio: AudioChunk {
                                data: data.into(),
                                format: PcmFormat {
                                    sample_rate: 24000,
                                    channels: 1,
                                },
                            },
                        },
                    )
                }
            };
            w.kind = kind.into();
            w.payload = Some(payload);
        }
        "error" => {
            w.kind = "error".into();
            w.payload = Some(Fault(fault(&v["error"])?))
        }
        _ => return Ok(None),
    }
    w.event_id = o
        .get("event_id")
        .map(string)
        .transpose()?
        .unwrap_or_default();
    w.request_id = optional(&v["prism_request_event_id"])?;
    Ok(Some(w))
}
