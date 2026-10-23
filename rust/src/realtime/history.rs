use crate::{Error, ErrorKind};
use serde_json::{json, Value};

/// Who says a history message.
#[derive(Clone, Debug, PartialEq, Eq)]

pub enum HistoryRole {
    /// A user message.
    User,
    /// An assistant message.
    Assistant,
    /// A note for the model.
    System,
}
/// An item without audio. IDs are optional on input and present in history results.
#[derive(Clone, Debug, PartialEq)]

pub enum HistoryItem {
    /// A message that does not start a reply.
    Message {
        /// Optional client item ID, 1 to 32 characters; present when listed.
        id: Option<String>,
        /// Who says the message.
        role: HistoryRole,
        /// Message text, at most 4000 Unicode characters on creation.
        text: String,
    },
    /// A past call, shown after its result is added; never executes a tool.
    FunctionCall {
        /// Optional client item ID, 1 to 32 characters; present when listed.
        id: Option<String>,
        /// Identity of the tool call.
        call_id: String,
        /// Name of the application tool.
        name: String,
        /// Tool arguments as their original JSON string.
        arguments: String,
    },
    /// A result for a call already in the conversation.
    FunctionResult {
        /// Optional client item ID, 1 to 32 characters; present when listed.
        id: Option<String>,
        /// Identity of the tool call.
        call_id: String,
        /// The original tool output string.
        output: String,
    },
}
impl HistoryItem {
    /// Item identity, present in history returned by Prism.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::Message { id, .. }
            | Self::FunctionCall { id, .. }
            | Self::FunctionResult { id, .. } => id.as_deref(),
        }
    }
}
pub(super) fn settings(item: &HistoryItem) -> Result<Value, Error> {
    let bad = || Error::new(ErrorKind::Input, "invalid history item");
    if item
        .id()
        .is_some_and(|id| id.is_empty() || id.chars().count() > 32)
    {
        return Err(bad());
    }
    match item {
        HistoryItem::Message { text, .. } if text.chars().count() > 4000 => return Err(bad()),
        HistoryItem::FunctionCall { call_id, name, .. }
            if call_id.is_empty() || name.is_empty() =>
        {
            return Err(bad())
        }
        HistoryItem::FunctionResult { call_id, .. } if call_id.is_empty() => return Err(bad()),
        _ => {}
    }
    let mut value = match item {
        HistoryItem::Message { role, text, .. } => {
            json!({"type":"message","role":match role {HistoryRole::User=>"user",HistoryRole::Assistant=>"assistant",HistoryRole::System=>"system"},"text":text})
        }
        HistoryItem::FunctionCall {
            call_id,
            name,
            arguments,
            ..
        } => json!({"type":"function_call","call_id":call_id,"name":name,"arguments":arguments}),
        HistoryItem::FunctionResult {
            call_id, output, ..
        } => json!({"type":"function_call_output","call_id":call_id,"output":output}),
    };
    if let Some(id) = item.id() {
        value["id"] = json!(id)
    }
    Ok(value)
}
pub(super) fn listed(value: &Value) -> Result<HistoryItem, Error> {
    let bad = || Error::new(ErrorKind::Stream, "invalid history item");
    let field = |key: &str| value[key].as_str().map(str::to_owned).ok_or_else(bad);
    let id = Some(field("id")?);
    let item = match value["type"].as_str() {
        Some("message") => HistoryItem::Message {
            id,
            role: match value["role"].as_str() {
                Some("user") => HistoryRole::User,
                Some("assistant") => HistoryRole::Assistant,
                Some("system") => HistoryRole::System,
                _ => return Err(bad()),
            },
            text: field("text")?,
        },
        Some("function_call") => HistoryItem::FunctionCall {
            id,
            call_id: field("call_id")?,
            name: field("name")?,
            arguments: field("arguments")?,
        },
        Some("function_call_output") => HistoryItem::FunctionResult {
            id,
            call_id: field("call_id")?,
            output: field("output")?,
        },
        _ => return Err(bad()),
    };
    if item.id().is_none_or(str::is_empty) {
        return Err(bad());
    }
    match &item {
        HistoryItem::FunctionCall { call_id, name, .. }
            if call_id.is_empty() || name.is_empty() =>
        {
            return Err(bad())
        }
        HistoryItem::FunctionResult { call_id, .. } if call_id.is_empty() => return Err(bad()),
        _ => {}
    }
    Ok(item)
}
