# Response lifetime contract

This is a contract checklist, not a wire fixture. The linked tests execute the
scenarios through session methods and controlled protocol peers. The wire schema
and fixture copies do not change.

The session owns response metadata. It retains up to 128 ordinary responses and
128 protected responses. Generation, unresolved tool results, or an explicit hold
protect a response. Go and Rust also protect pending SDK operations. Multiple
holds share one protected slot. Eviction selects the oldest ordinary response and
skips protected responses. Holds retain no audio and send no protocol messages.

The session also remembers the last 128 evicted completed response IDs, in
eviction order. This history contains IDs only, not response metadata or audio.
A terminal event completes an active response once. Duplicate terminal events
are ignored for retained completed responses and IDs in this history. Duplicates
do not refresh the history. Any other terminal response ID fails the session
with a stream/protocol error, including a previously completed ID that has left
the history. A new response cannot reuse a retained or remembered ID.

| Scenario | Required behavior |
| --- | --- |
| Ordinary reference followed by 140 completed responses | The reference expires, even if an older response is protected. |
| Unresolved external tool followed by 140 completed responses | Its result can still be submitted. |
| Repeated tool result | An identical result sends no second write; a conflicting result fails. |
| Hold spans submission and another 140 responses | A later report request can use the tool reference. A superseded continuation still fails. |
| Playback delayed through 140 responses | The held reference can send its playback report. |
| Nested holds | Releasing one hold preserves the others. Repeated release is harmless where explicit release exists. |
| Last protection ends | Ordinary eviction resumes. Pending SDK operations keep their own protection. |
| 128 protected responses | A local request for another slot fails without closing the session. Existing owners remain protected. Incoming overflow fails explicitly. |
| Invalid, expired, or cross-session reference | Hold acquisition fails with an input error. |
| Cancellation after acquisition but before handle delivery | No hold leaks. Accepted wire operations are not replayed. |
| Full command queue or session closure | Release does not block. Rust holds do not extend the last session handle's lifetime. |
| Duplicate terminal event for retained state or one of the last 128 evicted completed IDs | No second completion, no protocol writes, and no session failure. |
| Completion for a never-started ID or an ID older than the 128-entry eviction history | Explicit stream/protocol failure. |
| Duplicate events at the history limit | The oldest remembered ID remains valid until another response is evicted; duplicates do not extend its lifetime. |
| New response reuses a retained or remembered ID | Explicit stream/protocol failure. |

Executable coverage:

- [Python reference](../../python/tests/test_realtime_lifetime.py)
- [TypeScript lifetime tests](../../typescript/tests/realtime-lifetime.test.mjs)
- [Go lifetime and race tests](../../go/realtime_lifetime_test.go)
- [Rust lifetime tests](../../rust/src/realtime/tests/lifetime.rs)
- [Rust controlled submission races](../../rust/src/realtime/tests/submission.rs)

Existing submission, cancellation, and conversation suites also check wire writes,
playback order, queue limits, and worker shutdown. Hold ownership stays inside the
session module. Playback adapters use holds and keep their existing queue limits.
