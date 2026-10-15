# Rust TTS implementation plan

Status: T0 contract checks implemented; runtime/transport features and plugin
adapters remain proposed. Rust must
implement every capability in the union of both plugins. The
[shared plan](livekit-tts-sdk-plan.md) owns behavioral decisions, source pins,
C01–C14 conformance families and Q1–Q8 release gates; there is no Rust LiveKit
adapter in this scope.

## Verified starting point

At SDK `b5c1fe4`, [Cargo.toml](../rust/Cargo.toml) declares `rimelabs-sdk`
0.1.0-alpha.6, edition 2021, Rust 1.88, Tokio and Tonic 0.14 with TLS ring/native
roots. `rimelabs-api` is 0.4.0; its downloaded package already supplies prost
messages and generated Serde implementations using protobuf JSON conventions.
CI tests Rust 1.88/stable on Linux, macOS and Windows. There is no HTTP/WebSocket
SDK dependency yet; `serde_json` is currently a dev dependency.

[client.rs](../rust/src/client.rs) exposes a reusable clonable `Client`, borrowed
`Tts` view and owned builder-style `SynthesisOptions`. `Model` is a public `Copy`
enum with Coda/Mist v3 variants. [stream.rs](../rust/src/stream.rs) implements
`Stream<Item = Result<Bytes, Error>>`, synchronous cancellation, async cleanup,
drop cancellation and final timestamps after successful consumption. Preserve
those guarantees and existing method signatures.

## Proposed public API and precedence

- Add `TtsConfig`, `SynthesisClient`, `TextPolicy`, `Timeouts`, `ResolvedAudio`,
  `ProviderCapabilities`, `SynthesisEvent` and `ProviderErrorDetails` as documented
  root exports. `client.tts().configure(config, defaults)` returns an owned
  configured handle, without network activity. Existing `ClientBuilder::model`
  and `endpoint` retain native gRPC behavior.
- `TtsConfig` uses a transport enum with gRPC, HTTP, WS3 and V1 variants; variant
  configuration owns an exact URL, model selection, V1 encoding and trust policy.
  Introduce a separate owned model identifier for Mist v2/custom service IDs;
  do not add `Custom(String)` to the existing `Copy` `Model` enum or remove its
  `Copy` trait. Known native models convert into the new identifier explicitly.
- Extend `SynthesisOptions` with provider/text/deadline builders. Private fields
  track inherit/value/clear rather than losing presence in `false`, default format
  or empty collections. Existing `.timestamps(false)` and `.custom_lexicon(vec![])`
  remain explicit assignments. `Default` continues resolving to current native
  behavior when no configured defaults exist.
- Add `.overlay(&overrides)` and typed clear/reset builders. Resolution is overlay
  values over supplied options over configured/SDK defaults. `.clone()` and
  builder replacement remain idiomatic copies; mutable maps/vectors are owned
  and copied at the operation boundary. `.timeout(None)` still disables an
  overall deadline; a separate reset builder restores inheritance.
- Configured handles retain `synthesize`, `stream` and `synthesize_to_buffer`
  signatures using `SynthesisOptions`, and add async `prewarm()`,
  `release_idle_connections()`, `retire()` and `close()`. Retirement stops new
  admission and closes idle resources while operations keep their generation.
  Clones share handle state; root client shutdown cancels all owned handles.
- `audio.events()` returns a borrowing event-stream view selected before the
  first read; it uses the same bounded reader as the audio stream. Reject a mode
  change after iteration has begun. Retain final `audio.timestamps()` after that
  view reaches successful EOF and is dropped. Add resolved audio metadata with
  optional actual rate/encoding, never fabricated codec values.

Illustrative future usage:

```rust
let options = SynthesisOptions::default().voice("lyra").sample_rate(24_000);
let tts = client.tts().configure(config, options.clone())?;
tts.prewarm().await?;
let mut audio = tts.stream(source, options.text_policy(TextPolicy::None))?;
while let Some(chunk) = audio.next().await {
    consume_audio(chunk?);
}
audio.close().await;
```

Use a non-exhaustive typed event enum and accessors for extensible metadata.
Keep transport-library internals behind SDK APIs except deliberately documented
injection types. Shared extension collision rules remain separate from options
overlay order. Do not add LiveKit crates or resurrect `complete_text`.

## Runtime and dependency work

Keep edition 2021, Rust 1.88, the caller's Tokio runtime and the existing default
gRPC functionality. Add async [`reqwest`](https://docs.rs/reqwest/latest/reqwest/)
with streaming responses, [`tokio-tungstenite`](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)
for WebSocket I/O, and direct `prost`/`serde`/`serde_json`/base64 dependencies as
needed. Match prost to the pinned API package's 0.14 generation. Reuse its
generated protobuf JSON implementations instead of deriving a second wire model.
Choose and lock released dependency versions only after proving their full
dependency graph supports Rust 1.88; these library selections are proposals,
not a claim that the newest versions share that MSRV.

Use explicit TLS features consistent with the current native-root policy and
avoid inadvertently requiring system OpenSSL on supported platforms. Add Tokio
production `net`/I/O features required by sockets; test single-thread and
multi-thread runtimes. Do not make the new transports optional-by-default and
then claim installed-package parity. If feature flags are useful later, qualify
the declared feature matrix and keep all required transports in the default
migration package.

`TtsConfig` accepts a caller-configured `reqwest::Client` clone for HTTP and an
`Arc<dyn WebSocketConnector + Send + Sync>` for custom socket establishment.
Ship a connector implementation that accepts the SDK's typed proxy/TLS options;
callers do not write Rime framing, authorization or state machines. The connector
returns a fresh socket/stream under an explicit ownership contract. HTTP and
WebSocket libraries do not automatically share proxy/TLS settings; qualify both
paths rather than claiming that supplying reqwest config also configures sockets.

Borrowing an HTTP client retains a handle to its shared pool; shutdown drops only
the SDK's handle and does not invalidate the caller's clone. SDK-created response
streams and WebSockets are owned and released on cancellation. Preserve borrowed
client request timeouts and enforce additional SDK phase limits without mutating
the client. A raw reqwest client can carry redirect policy the SDK cannot inspect;
R2 must establish an enforceable injection/redirect contract under Q6 before
credentials are sent. Use an SDK session wrapper with per-hop control if required,
and document any unsupported injection combination rather than silently changing
caller settings.

## Implementation structure and lifecycle

Add `tts/` modules for options/configuration, provider operations, HTTP, WS3, V1,
pooling and events; keep public root re-exports and existing `Tts`/`AudioStream`
paths intact. Move current TTS code only where needed for this separation;
`client.rs` remains owner of shared credentials/root lifetime and STT stays
independent. Reuse `TaskTracker`, cancellation tokens, bounded queues, buffers,
lexicons and final-timestamp validation.

One task owns each socket reader and routes its ordered events. A pool lease
allows one active context per socket; concurrent streams obtain different
sockets. A generation owns its setup tasks and leases. `prewarm` shares setup
without letting a dropped waiter cancel another waiter's preparation. Capture
generation/options when creating an operation, not when first polled. Recheck
admission after awaits so late connections are closed after root shutdown.

V1 validates ready/version/subprotocol, started/request ID, frame type, oneof,
context and terminal sequence using prost or generated protobuf JSON. WS3 keeps
its separate query/messages and sends final flush at EOF. `Drop` requests
cancellation; it cannot await a wire cancel. Track bounded cancel/drain cleanup
in the existing task owner so explicit `close().await`/root shutdown can join it.
If drain fails or the runtime is gone, discard the socket rather than pooling it.

`Timeouts` uses typed optional `Duration` settings with inherit/disable/value
states. Preserve open input pauses and existing gRPC backpressure semantics;
apply route-specific response-idle and terminal budgets as shared fixtures
require. Cancellation must win over buffered output when the existing contract
says it does. Add provider detail accessors to the existing opaque `Error`,
preserving `kind`, `message`, request ID, source and clone behavior. Sanitize
`Debug` for endpoints, credentials and nested causes as well as `Display`.

## Complete capability mapping

| Shared matrix row | Rust implementation and acceptance |
| --- | --- |
| Options | Extend private option state, typed builders/clear/overlay, owned snapshots and old signatures. C01. |
| Runtime compatibility | Rust 1.88/stable, existing OS matrix and Tokio modes; dependency/TLS feature qualification. C02. |
| Transport | HTTP/WS3/V1 provider modules alongside native gRPC; streamed HTTP response bodies and no text-source buffering. C03. |
| Endpoints and sessions | Typed URLs/config, borrowed reqwest handle/custom connector, SDK trust/redirect controls, binary/JSON. C04. |
| Models | Separate custom/Mist v2 identifier while preserving old `Model: Copy` and native defaults. C05; Q2. |
| Mist controls | Optional bracket builders map to native `MistParameters`; legacy reduce-latency follows route rules. C06. |
| Legacy Coda controls | Typed legacy generation builders, HTTP/WS3 mapping and native rejection; reserved proto fields stay reserved. C07. |
| Provider extensions | Owned map of typed JSON values and native inline/OOV controls; legacy collision/filter rules; retain Coda lexicons. C08. |
| Speed | Separate alpha/time-scale builders, route-aware precedence/bounds/defaults, explicit zero and Mist v2 restrictions. C09. |
| Output | Add non-exhaustive `AudioFormat` variants `OggOpus`, `WebMOpus`, `Pcmu`; qualified rates, resolved metadata, fixed presets/WAV/buffer preserved. C10. |
| Text policy | Typed native split enum and independent WS3 segment; preserve text and stable unit boundaries without a tokenizer. C11. |
| Timings | Borrowing ordered event stream, bytes-only discard, bounded backpressure, final result after successful consumption. C12; Q4. |
| Preparation and timeouts | Public asynchronous setup, waiter/drop isolation, typed phase budgets and late-connection cleanup. C13. |
| Errors and ownership | Additive opaque error metadata, root/handle clone lifetimes, tracked Drop cleanup and cancel/drain/reuse. C14. |

## Conformance and Rust lifecycle tests

Extend `src/tests/tts_features.rs` and add `src/tests/tts_http.rs`, `tts_ws3.rs`,
`tts_websocket_v1.rs`, `tts_events.rs`, `tts_options.rs` and `tts_lifecycle.rs`.
Use local HTTP/WebSocket peers alongside existing Tonic servers. Copy all shared
C01–C14 scripts into `rust/testdata/tts-compat/` and update CI equality checks;
the crate's `testdata/**` include must retain them in packaged tests.

Test Drop before connection, while a send/read is pending, during full-queue
backpressure, and after audio but before final status. Test cancellation safety
of `select!` around prewarm/reads, dropped prewarm waiters, explicit close joining
workers, last-client/handle clone drop, retained stream generations, and sibling
isolation. Ensure EOF is withheld until provider success and final timestamps
cannot succeed after cancellation. Test single-thread Tokio without blocking
its executor, wrong-context frames, malformed canonical JSON and late sockets.
Assert Send requirements with compile tests and retain old builder/Model examples.

From `rust/` run `cargo fmt --check`,
`cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked`,
`cargo doc --locked --no-deps`, and `cargo package --locked`. Run on Rust 1.88
and stable using registry dependencies. Test the packaged crate in a clean
consumer with HTTP/WS3/V1/gRPC public smoke cases; compile/doc-test all new APIs.
Also resolve a fresh downstream lockfile at the MSRV: this repository's locked
build alone cannot prove that a semver-compatible transitive update works for
new consumers. No local path overrides belong in release evidence.

## Work items and coordinated releases

The items below join the shared cross-language PR stages defined in the
[shared plan](livekit-tts-sdk-plan.md#pr-structure-and-completion-gate).
They are not separate language PRs. T0 includes the shared corpus and
validation/loading checks in all four SDK suites; it adds no SDK features.

| Work item | Scope | Dependencies and exit criterion |
| --- | --- | --- |
| R0 | Shared contract fixtures and inventories | Same shared P0; every C family assigned to Rust. |
| R1 | Add reusable TTS configuration | R0; owned handles/options, model-ID design, phase limits/errors and native preparation; old API compile checks. |
| R2 | Add TTS transport resources | R1; MSRV-compatible dependencies, TLS/injection/redirect contract and pool generations; C02/C04/C13/C14. |
| R3 | Expose TTS provider and text controls | R1; native bracket/split and legacy request mapping/validation; C05–C09/C11. |
| R4 | Expand TTS audio formats | R1; formats, metadata, qualified rate behavior, preserved presets/buffer; C10. |
| R5 | Add streaming HTTP synthesis | R2–R4; early body bytes, exact payloads and cancellation-safe response ownership. |
| R6 | Add WS3 synthesis events | R2–R4; event view, live timings and bounded pooled lifecycle. |
| R7 | Add native WebSocket synthesis | R6 infrastructure; codecs, terminal validation, tracked cancel/drain and reuse. |
| R8 | Qualification and package documentation | R5–R7, all four runners and Q gates; clean crate/MSRV installs, live and performance evidence. |

Follow the Rust section of [RELEASING.md](../RELEASING.md): Conventional Commit
PR titles, Release Please's independent crate version and `rust-v` tags, reviewed
release PR, and verified crates.io publication/clean exact-version installation.
The release guide still mentions API 0.3.x while the manifest/lock use 0.4.0;
correct that documentation in R8, using the then-current verified dependency.
Do not downgrade the dependency to match stale prose. Preserve upstream license
notices for adapted code and retain all fixture files in the crate package.

Use a fixed external Rust consumer to test oldest/newest versions in the declared
SDK range. Default Cargo prerelease matching is not a compatibility promise;
record actual tested versions and lockfiles. This complements the later fixed
Agents/plugin upgrade tests in Python/Node and cannot certify those adapters.
No plugin implementation or hosted Inference work belongs in R0–R8.

## Remaining decisions

R2 must lock dependencies and enforce the borrowed-client/proxy/TLS/redirect
contract at Rust 1.88. Q2–Q6 settle endpoint availability, codec rates, timestamp
units and provider fields. Q1 is resolved by the shared current-release baseline
decision; Q7/Q8 remain shared qualification and compatibility
gates. Keep unresolved service behavior visible even though Rust has no direct
LiveKit plugin consumer.
