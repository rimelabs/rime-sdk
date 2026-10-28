//! Public Rust event pattern and root export compatibility.

use rimelabs_sdk::{RealtimeEvent, RealtimeIdleTimeout, ReplyRequestOptions, ResponseUsage};

// Compile this outside the SDK crate to protect existing exhaustive field patterns.
fn match_previous_fields(event: &RealtimeEvent) {
    match event {
        RealtimeEvent::ResponseStarted {
            settings_revision: _,
            response: _,
            cause: _,
            parent: _,
            input_item_id: _,
            metadata: _,
            input_item_ids: _,
        } => {}
        RealtimeEvent::ResponseEnded {
            settings_revision: _,
            truncation: _,
            response: _,
            status: _,
            reason: _,
        } => {}
        _ => {}
    }
}

#[test]
fn prior_patterns_and_new_root_exports_compile() {
    let _ = match_previous_fields as fn(&RealtimeEvent);
    let _ = ReplyRequestOptions::default();
    let _ = std::mem::size_of::<ResponseUsage>();
    let _ = std::mem::size_of::<RealtimeIdleTimeout>();
}
