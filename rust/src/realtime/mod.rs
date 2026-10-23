//! Prism realtime sessions and conversations with application-owned audio adapters.
//!
//! Consume the owned event stream concurrently with requests. Dropping an operation
//! future cancels its wait. A submitted request remains tracked, and a late accepted
//! reply is cancelled. Writes with unknown outcomes close the session without replay.
mod client;
mod conversation;
mod protocol;
mod session;
mod transport;
mod types;
pub use client::Realtime;
pub use conversation::{
    ConversationOutput, ConversationPlayback, ConversationRunner, ConversationSource,
};
pub use session::{RealtimeEvents, RealtimeSession};
pub use types::*;
#[cfg(test)]
mod tests;
