# Rime SDK TTS compatibility plan

The SDK will support every provider capability needed to replace the client
implementations in both official LiveKit Rime plugins, for Python and TypeScript,
without removing existing plugin behavior. Every compatibility addition must
ship with equivalent behavior in all four SDK languages: Python, TypeScript,
Go, and Rust.

This is the shared SDK requirements and sequencing plan. Language-specific API
designs, runtime changes, dependencies, and implementation tasks belong in later
plans for each SDK. Those plans must implement the same capability contract using
idiomatic APIs; they must not reduce scope based on which SDK a plugin consumes.
The LiveKit adapters, their public arguments, audio emission, metrics, and error
translation remain subsequent work.

The scope is TTS; this plan does not establish feature parity for unrelated STT
or Prism functionality. T0 contract fixtures and validation checks are implemented;
SDK runtime/transport features and plugin adapters have not started. The four
linked implementation plans describe proposed APIs, not available SDK features.

## Baseline and compatibility requirement

Source snapshots for the compatibility inventory, recorded on October 14, 2026:

- SDK: `b5c1fe4a02e15df6cbd66c26ee0eb0626e20465d`.
  The changes since `9e79cef` concern Prism and releases; TTS is unchanged.
- `livekit/agents`: `43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0`.
  Sources: [public interface][plugin-tts], [README][plugin-readme],
  [WS3 adapter][plugin-ws3], [V1 adapter][plugin-v1], and
  [regression tests][plugin-tests].
- `livekit/agents-js`: `86b27fd5a586fcfe15b431e79a59d2322b63dd13`.
  Sources: [plugin directory][plugin-js], [options and request mapping][plugin-js-options].
- SDK behavioral baseline: [shared conformance contract](../conformance/README.md).

### Source recheck for the language plans

Rechecked October 14–15, 2026, America/Los_Angeles, with a final check at
2026-10-15 07:05 UTC. SDK local HEAD and GitHub `main` both remain
`b5c1fe4a02e15df6cbd66c26ee0eb0626e20465d`. No SDK implementation changes were
present; this shared plan was untracked. Preserve it when staging future work.

- Python upstream `main` is now
  [`31c6026df14eebb71975c16ca6a422ce68644b71`](https://github.com/livekit/agents/compare/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0...31c6026df14eebb71975c16ca6a422ce68644b71).
  The five newer commits change Rime's version from 1.8.7 to 1.8.8 and its Agents
  dependency floor accordingly; the Rime implementation and its regression test
  file are unchanged. The later T0 artifact check below establishes the release
  baseline; these older pins remain source history only.
- Node upstream `main` is now
  [`1871ba2c4eb8fa69e8d04ee60365eab468d051e1`](https://github.com/livekit/agents-js/compare/86b27fd5a586fcfe15b431e79a59d2322b63dd13...1871ba2c4eb8fa69e8d04ee60365eab468d051e1).
  The five newer commits change Rime's package version/changelog from 1.9.2 to
  1.9.3, with no Rime implementation changes. Agents also changes package exports;
  include package-loading checks when qualifying the newer framework pair.
- Local reference checkouts are Python `3f479e242428d5fa9a7208810f036bbb8ccf9a48`
  and Node `558991c6617d6edffa79e82403e3217cc6ebe3b6`. Both were clean. They are
  not the baseline pins. Python's baseline files were read from GitHub at the
  recorded revision; Node's Rime directory matches its recorded revision.
- Engine/API local HEAD is `4645997bca5da24f2710b5dfcf4bf90010b7767b`, clean.
  Checked `interfaces/rime/text_to_speech.proto`, `engine/src/http.rs`,
  `engine/src/grpc.rs`, and Mist synthesis/timing stages. They still define native
  bracket controls, reserved legacy Coda fields, final timestamps, and no V1 flush.
  This records local source evidence, not a claim about a deployed engine version.
  The earlier shared plan did not pin the engine, so no engine delta is asserted.

### T0 release baseline decision

On October 15, the user selected the current released Rime plugins and their
framework/runtime floors. Earlier plugin releases are outside this migration's
qualification scope. Freeze these artifacts rather than following a moving
`latest` version:

| Plugin | Framework baseline | Runtime contract |
| --- | --- | --- |
| [`livekit-plugins-rime` 1.8.8](https://pypi.org/pypi/livekit-plugins-rime/1.8.8/json) | Agents 1.8.8; declared dependency `livekit-agents[codecs]>=1.8.8` | Plugin Python >=3.10; Agents 1.8.8 declares >=3.10,<3.15 |
| [`@livekit/agents-plugin-rime` 1.9.3](https://registry.npmjs.org/@livekit%2fagents-plugin-rime/1.9.3) | Agents 1.9.3; exact peer dependency `1.9.3`, RTC peer `^1.1.0` | Neither package declares `engines`; official documentation requires Node 20+; both module loaders are exported |

[LiveKit's runtime requirements](https://docs.livekit.io/agents/start/voice-ai/)
establish Python 3.10+ and Node 20+. Exact patch levels and transitive dependency
resolution remain Q6 qualification work. The SDK currently requires Python 3.11
and Node 22; supporting the plugin floors is a prerequisite, not a T0 runtime
change. Go 1.24 and Rust 1.88 remain the existing SDK floors.

The downloaded Python wheel's SHA-256 and Node tarball's SHA-512 integrity were
verified against their registry records. Five implementation files from each
artifact match the release source pins `31c6026` and `1871ba2` byte-for-byte.
Artifact metadata, full revisions and individual source hashes are recorded in
[`contract.json`](../conformance/tts-compat/contract.json). A subsequent upstream
check found Python `533c210c1073e5ef8756a143d133592714b3d55a` and Node
`b14617071c691a15cd77421efd1f263c287503a2`, with no further Rime implementation
or Rime regression-test changes. SDK HEAD remains `b5c1fe4`.

These are qualification baselines, not tested migrated adapters. Python's open
dependency range does not certify every future Agents release, and Node's exact
peer is not an unrestricted lower bound. Future SDK-only upgrades hold the
selected migrated plugin and Agents artifacts fixed within a tested SDK range.

### Contract differences to freeze

These are source observations at the original pins, not replacement SDK defaults.
The future adapters retain their own option resolution and LiveKit tokenizers;
the SDK owns serialization and Rime protocol state.

| Case | Python plugin | Node plugin |
| --- | --- | --- |
| No model/voice supplied, HTTP | Coda / `lyra`, language `eng`, 24,000 Hz | Coda / `luna`, language omitted, 24,000 Hz |
| Explicit Coda, omitted voice, legacy route | `lyra` | `lyra`; differs from omitted `modelId` |
| Mist, omitted voice | `cove` | Legacy `luna`; V1 `cove` |
| Omitted sample rate | Coda/Mist v3 24,000; other IDs 22,050 | Mist v2 22,050; other IDs 24,000 |
| WS3 segment | `bySentence` | `bySentence` |
| V1 defaults | Binary, `audio/pcm`, language `eng` | Binary, `audio/pcm`, language `eng` |
| Coda `speedAlpha` on HTTP/WS3 | Forwarded | Not forwarded by `modelParams` |
| Mist v2 `reduceLatency` | HTTP only | HTTP only |
| Additional provider fields | No arbitrary constructor keyword bag | HTTP forwards additional fields; WS3 builds a fixed query field set |
| HTTP timeout | Model total: Coda 240 s, others 30 s; separate socket-connect limit | Connection option bounds fetch and each body read; no equivalent fixed model total |
| Sessions / updates | Borrowed `aiohttp` session; explicit update signature | No session argument today; broad partial option updates; public idle-connection release |

Also freeze custom IDs (`startswith("mist")` in Python versus `includes("mist")`
in Node), explicit versus inherited sample rates after updates, rejected transport
switches, URL-derived versus explicit models, and snapshots taken before updates.
Do not normalize these differences away. All four SDKs must express their union.

The existing findings are inputs to the inventory, not a completed audit of both
plugins. Build the shared SDK requirements from the union of their supported
capabilities. Record differences in defaults, validation, and request behavior;
do not assume the two plugins have identical contracts.

Compatibility includes valid constructor and update options, defaults, supported
runtime environments, explicit endpoints and sessions, text boundaries, audio
formats and rates, timestamp delivery, cancellation, and resource ownership. Customers must
not need a new backend selector or a model change to preserve existing behavior.
Existing SDK defaults and public methods must also continue working.

The migration should move necessary HTTP and WebSocket implementations into the
SDK. A gRPC-only replacement cannot preserve arbitrary HTTP/WebSocket endpoints,
caller-supplied HTTP sessions, or the existing WS3 timestamp stream. Transport
selection must be deterministic from configuration; do not introduce automatic
fallback or replay after a request fails.

## Required capability matrix

Every capability below is required in all four SDK languages. API shapes,
client/session types, and runtime requirements are defined in the individual
SDK plans while preserving the shared behavior.

| Area | Current SDK gap | Required outcome |
| --- | --- | --- |
| Options | The public options contract is not uniform across SDKs | Provide forwardable synthesis options with consistent omission, override, validation, and snapshot semantics |
| Runtime compatibility | SDK and consuming plugin runtime requirements may differ | Preserve supported SDK environments and support the environments required by each consuming plugin |
| Transport | TTS uses native gRPC only | SDK-owned HTTP, WS3, and native V1 WebSocket support alongside gRPC |
| Endpoints and sessions | Native hostname/port configuration cannot represent current plugin settings | Preserve HTTP URLs, WebSocket URLs, binary/JSON selection, custom endpoint authorization, and caller-owned sessions |
| Models | Native SDK selects Coda or Mist v3 | Support Mist v2 through its compatible transport; preserve accepted custom model IDs where the configured service supports them |
| Mist controls | Bracket flags and legacy controls are absent | Add bracket pause/phonemization controls and route-appropriate reduce-latency support |
| Legacy Coda controls | No SDK representation | Preserve HTTP/WS3 forwarding of temperature, top-p, repetition penalty, and maximum tokens; preserve native V1 rejection |
| Provider extensions | Existing plugins can forward additional provider parameters | Preserve route-appropriate inline speed, text normalization, OOV controls, and additional parameter forwarding, with explicit collision rules |
| Speed | Strict SDK duration bounds differ from legacy handling | Preserve route/model-specific speed-alpha and time-scale controls, precedence, defaults, and bounds |
| Output | PCM/WAV/MP3 at 8/16/24 kHz; fixed mu-law preset | Add Ogg Opus, WebM Opus, and general PCMU; support all qualified plugin rate/format combinations, including 22,050 Hz |
| Text policy | All native requests force sentence accumulation | Expose SDK-owned text/split policy and WS3 segmentation settings without conflating their different meanings |
| Timings | Mist v3 final-only timestamps | Retain final results and add provider timing events needed to preserve WS3 aligned transcripts |
| Preparation and timeouts | Preparation is private; connection/progress limits are private | Public prewarming plus distinct setup, progress, completion, and overall deadline controls where needed for compatibility |
| Errors and ownership | Typed errors exist but lack some protocol status information | Preserve enough status/request metadata for plugin error mapping; make owned/borrowed resource lifetime explicit |

This is parity between SDK languages, not a promise that every model and transport
supports every feature. Provider restrictions must behave consistently across
SDKs. Record each plugin's accepted/rejected combinations separately so its
adapter can preserve its existing public contract.

## Implementation sequence

**1. Freeze the compatibility cases before changing SDK behavior.**

Create a checked-in matrix and shared fixtures under `conformance/` covering
every public constructor/update argument and valid combination from both plugins.
Pin source revisions and package versions for each plugin independently using
the current-release baseline decision above. Include provider extension parameters and record
which routes forward, reject, or ignore them.

Record omitted versus explicit values, generated HTTP bodies/WS messages, and
observable lifecycle behavior. Preserve supported behavior; internal private
implementation details are not compatibility requirements.

Keep each plugin's defaults explicit in these fixtures: model, voice, language,
sample rate, transport, and segmentation. Preserve differences between plugins.
The future adapters can pass these to the SDK without changing SDK defaults.
Include model/endpoint changes, retained explicit sample rates, and streams
created before an update.

**Acceptance:** every compatibility row has a case, an owning SDK capability,
and an identified transport. Unknown service behavior remains a release blocker,
not an implicitly dropped feature.

**2. Define the shared public configuration and runtime contract.**

Separate client transport/connection configuration from per-synthesis options.
Provide immutable or defensively snapshotted synthesis options accepted by all
synthesis methods; preserve existing public call forms. Define consistent
precedence: explicit per-call overrides, then supplied options, then defaults.
Distinguish omission, explicit clearing, false, zero where valid, and empty
collections. Provide a public way to copy/replace options without rebuilding
every field in an adapter. Define collision rules for additional provider
parameters so their interaction with typed options is explicit and testable.

Define public resolved audio metadata and provider capabilities where the adapter
needs them. Public APIs must not require importing private SDK modules or LiveKit.
Snapshot options at operation creation so later configuration changes cannot
change a running stream.

Audit each SDK's supported runtime environments and dependency graph against
its consumers. Preserve existing SDK runtime support and avoid requiring plugin
users to raise their runtime minimum solely to adopt the SDK-backed adapter.
Cover package loading, transitive dependencies, cancellation, and clean installs
in CI. Specific runtime versions and backport techniques belong in each SDK plan.

**Acceptance:** existing SDK examples still run, new options are validated
consistently, and clean environments across the required runtime matrix can
install and use each SDK.

**3. Establish transport ownership, preparation, and error contracts.**

Add explicit TTS transport configuration for native gRPC, HTTP, WS3, and V1
WebSocket; retain gRPC as the default for existing SDK callers. Preserve exact
custom routes and model selection rather than rewriting URLs to another service.
Keep endpoint trust checks and credential handling inside the SDK.

Support externally owned clients/sessions, preserving their connection settings,
proxy/TLS behavior, and lifetime. Closing the SDK must not close borrowed
resources. Define equivalent HTTP/WebSocket client injection capabilities in
every SDK, without requiring consumers to implement Rime's protocol themselves.
Concrete client types and integration mechanisms belong in the individual plans.

Add public asynchronous prewarming that prepares reusable transport resources
without synthesizing dummy speech. Define behavior for repeated/concurrent calls,
shutdown races, and per-operation cancellation. Prewarming must not claim that
the remote synthesis model has been warmed.

Expose the timeout controls required by each plugin's compatibility cases.
Distinguish connection setup, response progress, protocol start/end waits, and
total deadlines. Preserve permitted streaming input pauses and route-specific
timeout defaults. Do not map every setting to the SDK's overall timeout.

Extend typed errors with optional provider status and request metadata sufficient
for later LiveKit mapping. Distinguish local validation/resource limits from
provider failures where retry classification differs. Preserve sanitization,
task cancellation, and successful completion only after final provider status.
The SDK must not automatically retry synthesis; LiveKit's existing retry policy
remains the plugin/framework's responsibility.

**Acceptance:** public APIs cover configuration and lifecycle without private
access, and shared tests prove borrowed-resource ownership and timeout behavior.

**4. Implement HTTP compatibility and provider options.**

Implement complete-text HTTP synthesis with streamed audio responses in all
languages. Match accepted request fields, model IDs, explicit endpoints, and
omitted-field behavior. Add the two Mist bracket controls to native requests too;
the protocol already defines them. Add Mist v2 and reduce-latency controls on the
transports that currently accept them.

Support additional provider parameters wherever an existing plugin forwards
them, including inline speed, text normalization, and OOV controls. Preserve
route-specific forwarding and rejection; accepting a field does not establish
that every engine uses it. Test its interaction with typed synthesis options.

Preserve legacy Coda generation controls as transport-specific request fields.
The current native protocol reserves those former fields; do not recreate them
as native controls or claim they affect the current engine. Preserve existing
native V1 rejection instead of silently ignoring them.

For speed controls, use route-specific compatibility cases. Current engine HTTP
translation prefers `timeScaleFactor`, otherwise reciprocates nonzero
`speedAlpha`, defaults to 1, and clamps the resulting factor to `[0.4, 2.5]`.
Mist v2 differs. Keep legacy forwarding where that best preserves behavior;
do not apply one conversion globally or turn previously accepted inputs into
new SDK validation failures on compatibility transports.

**Acceptance:** fake HTTP services observe equivalent requests, streamed output,
errors, and deadlines in all languages; qualified live endpoints accept the cases.

**5. Expand audio formats and rate support.**

Add named Ogg Opus, WebM Opus, and variable-rate PCMU formats. Preserve the
existing fixed 24 kHz PCM and 8 kHz mu-law presets and their conversion behavior;
general PCMU must not silently force 8 kHz. Remove the universal 8/16/24 kHz
restriction where the selected service supports other plugin rates. Qualify
rate/encoding pairs with actual responses; include 22,050 Hz regression cases.

Keep raw PCM as signed 16-bit little-endian mono. Document encoded-format
metadata accurately, including any difference between a requested rate and a
codec's actual rate. Test sample-frame boundaries, complete encoded output,
WAV finalization, existing presets, and the existing buffered-output size limit.
LiveKit-specific decoding and audio emission remain outside the SDK.

**Acceptance:** all qualified plugin format/rate combinations produce correctly
described output in all four languages without breaking existing SDK presets.

**6. Implement WS3 and V1 WebSocket compatibility.**

Implement the transports as separate SDK modules, sharing client/error/ownership
helpers where their behavior agrees. WS3 needs its existing URL query options,
`contextId`, text messages, final wire flush, audio/timestamp/done events, and
connection reuse. V1 needs binary protobuf and canonical JSON subprotocols,
start/text/end/cancel lifecycle, request IDs, final-status validation, and
cancel/drain before a connection is reused. Preserve the one-active-context-per-
socket behavior; concurrent operations use separate pooled WebSockets.

Expose text split policy through the SDK. Keep sentence accumulation as the
existing native SDK default while allowing already prepared sentence units and
service-default behavior for compatibility. Preserve text and whitespace. When
accumulation is disabled, automatic wire chunking must not turn one stable text
unit into unrelated synthesis units.

WS3 `segment` modes are server settings and are not interchangeable with native
split strategy. V1 has no wire flush operation: draining a LiveKit tokenizer
must leave the same SDK operation/context open; only input completion sends end.
Custom LiveKit tokenizers remain in the plugin and feed the SDK. Do not add a
second mandatory tokenizer in the SDK.

Support safe connection reuse across requests and retiring changed client
configurations after in-flight work completes. A model/endpoint change must not
cancel a sibling stream that still uses the old configuration.

**Acceptance:** protocol peers verify exact lifecycle sequences, early audio,
partial sentences, input pauses, empty input behavior, concurrency, cancellation,
and reuse across all four SDKs.

**7. Add streaming timing metadata and settle timestamp semantics.**

Keep the existing final timestamp result after successful audio consumption.
Add a public metadata delivery mechanism for WS3 word-timing events, including
context/request identity and timestamp origin. Specify ordering, bounded memory,
and behavior when a caller does not consume metadata. Audio and metadata must
not compete for the same underlying stream reader or deadlock each other.

Do not present final native Mist timestamps as incremental alignment. Preserve
WS3's timing delivery and native V1's existing capabilities separately. Expose
alignment failure independently of successful audio; final synthesis failure or
cancellation must not be reported as a successful timestamp result.

Resolve timing units against actual output at different rates and speed factors.
The protocol comment describes offsets before time scaling, while Mist derives
timings from synthesis frames after applying its length scale. Establish the
authoritative contract and correct documentation or behavior consistently in
every affected SDK. Do not add an unverified multiplication in the plugin.

**Acceptance:** all languages pass the same normalized-text, timestamp-origin,
speed/rate, missing/malformed metadata, and partial-failure cases.

**8. Qualify parity and publish SDK prerequisites.**

Extend the existing shared conformance corpus with HTTP, WS3, and V1 peers and
expected outcomes. Keep any packaged fixture copies synchronized. For every
capability, run equivalent semantic cases in all languages and exercise each
language's native cancellation/close conventions. Run the package's documented
lint, type, test, build, and packaging checks.

Perform live qualification separately from fake-server tests: Coda lexicons,
Mist bracket controls and final timestamps, supported rates/formats, required
legacy routes, and concurrency/cancellation. Confirm actual direct-endpoint
Mist v2 availability; a hosted Inference retirement notice does not settle
plugin endpoint support. An unavailable required service needs an explicit
resolution before declaring the SDK migration-ready.

Benchmark cold and prewarmed first audio, end-of-input drain, long utterances,
and concurrent streams against each existing plugin's request behavior. Establish
regression thresholds before implementation results are accepted. Compare
protocol/content behavior rather than requiring nondeterministic speech bytes
to match exactly.

Release through [RELEASING.md](../RELEASING.md), using additive Conventional
Commit PRs and the existing independent package versions. A capability is ready
only when its four language implementations, docs, and clean package installs
pass; coordinate publication before announcing parity. Test the oldest and newest
SDK versions in the declared compatible range. Alpha version numbers alone are
not a compatibility guarantee.

## PR structure and completion gate

Use one coordinated PR covering all four SDKs for each capability stage, rather
than separate Python, TypeScript, Go and Rust PRs. Keep stages focused: contract
fixtures (T0); runtime compatibility; public configuration/resources; provider
and text controls; audio; HTTP; WS3 and incremental events; V1; qualification and
documentation. Each PR includes the applicable language changes and shared
fixtures together. A language with no code change for a stage still supplies
the required verification. All four implementations must converge on the same
fixtures before a capability is declared complete or released.

The P/T/G/R numbered rows in the language plans are implementation work items,
not independent PRs. Group them by capability and respect their dependencies;
the matching numbers do not imply identical scope across languages. P0, T0, G0
and R0 all refer to the same shared T0 PR. Use an unscoped Conventional Commit
title for a cross-language feature PR. Preserve upstream attribution/license
notices for adapted code.

### T0 scope and prerequisites

T0 establishes the compatibility contract and its validation infrastructure for
all four languages. It does not add SDK transports or change either plugin.
Its deliverables are:

1. Independently pinned Python and Node plugin inventories: public constructor
   and update arguments, defaults, validation, request forwarding, stream and
   resource lifecycle, and observed differences between the plugins.
2. A schema, manifest and fixtures under `conformance/tts-compat/` covering
   C01–C14, with source evidence and an identified capability/transport for each
   case. Separate source-observed behavior from proposed SDK behavior and live
   service qualification. Record unresolved expectations explicitly.
3. Corpus validation/loading checks in all four SDK test suites, synchronized
   Go/Rust packaged copies, and CI checks that detect missing families, dangling
   case references, invalid presence states and copy drift. Passing these checks
   validates the contract corpus; it must not report unimplemented SDK behavior
   as passing conformance.
4. A coverage report showing known baselines, outstanding evidence and Q gates.
   Runtime changes, transport peers that execute the new SDK APIs, and service
   qualification remain in their owning subsequent stages.

Q1 is resolved by the current-release baseline decision above. No user-input
question blocks T0. The [T0 corpus and coverage report](../conformance/tts-compat/README.md)
record 103 public argument/method entries and 126 cases across C01–C14. Corpus
validation is separate from SDK behavior verification; every case starts with
`not_run` for every SDK language.

Q2–Q8 do not block starting T0. Source-resolvable forwarding, collision and
lifecycle questions are investigation tasks in T0. Service availability, actual
codec rates, timestamp scaling, runtime qualification, performance budgets and
future SDK version ranges are recorded with their later owning stage. Unknown
wire behavior must not become an invented expected value or a passing test.

The SDK phase is complete when every capability matrix row passes in all four
SDKs, required runtime environments are supported, all transport/resource
contracts are public, and no compatibility case requires a Rime client
implementation to remain in either plugin. No production plugin changes are
part of this phase.

The later plugin phase must prove unchanged public arguments, defaults, option
updates, stream behavior, errors, and metadata for both plugins using their
respective SDKs. It must also test upgrading only the SDK while keeping the
corresponding Agents framework and migrated plugin fixed. Existing published
plugins do not gain an SDK dependency retroactively; initial adoption follows
each plugin's package release/dependency policy. Full end-to-end backward
compatibility cannot be certified until both adapters pass their integration suites.

## Shared design decisions for implementation

The following refine the existing requirements. Freeze them in the first contract
PR before implementing transports; any adjustment updates this document and all
four fixture runners together.

- Keep the existing gRPC entry points and defaults. Add explicit transport
  configuration without overloading a native hostname with an HTTP URL. HTTP
  complete-text requests return streaming audio. Incremental text over HTTP is
  unsupported; never collect an open text source to emulate streaming.
- Support three option states: inherit, explicit value, and clear/reset. Reset
  removes an inherited provider field; an SDK-required default may then apply.
  Disabling an optional deadline is distinct from inheriting it. Preserve each
  old API's existing `None`/null/zero behavior at its compatibility boundary.
  Snapshot nested lexicons, extensions, and transport identity at operation
  creation, including before the first audio read. New option-copy operations
  replace collections; empty collections explicitly clear them.
- Resolve typed overrides over an options object over defaults. Keep provider
  extension collision policy explicit and separate from that precedence. The
  proposed legacy HTTP mode merges extensions last over provider body fields,
  matching Node's `fetchPayload` (even its extra `text` key); connection,
  credential, and SDK control fields are never serialized as extensions. Test
  collisions with speaker, model, rate, text, and speed, including what the
  existing plugin filters before calling the SDK. WS3 adapters pass only the
  query fields their original plugin forwarded. Native transports reject fields
  they cannot represent. Do not promise legacy forwarding affects the engine.
- A public immutable configured TTS handle owns a pool generation. Replacing
  configuration creates a new handle; retiring the old one closes idle sockets
  and waits to release resources until its existing operations finish, including
  operations created but not yet consumed. Client shutdown cancels everything it
  owns. Public idle-connection release retires sockets without invalidating a
  handle or canceling active operations. Borrowed session objects remain open;
  request bodies and sockets created by the SDK through them remain SDK-owned.
- Prewarm prepares local HTTP resources (without a dummy POST), a ready gRPC
  channel, or a reusable WebSocket (including V1 `ready`). It is asynchronous,
  idempotent for the same configuration, and does not promise model warmup.
  A canceled waiter must not cancel shared preparation needed by another waiter.
- Expose separate setup, write, first-response/start, response-idle, completion,
  cleanup, and overall budgets. Record which clocks run during input/output
  pauses per route. Preserve the Python WS3 receive timeout and the native V1
  allowance for open input pauses; no single generic stall timer can express both.
  Existing native SDK setup/first-audio/progress defaults remain 10/30/60 seconds,
  with no overall timeout by default. The adapters explicitly supply their
  original route limits; V1 cancellation/drain uses its bounded cleanup budget
  (currently at most one second in both plugins).
- Native split choices are service default, sentence splitting, no splitting,
  and sentence accumulation. Preserve current SDK accumulation by default. A
  caller-supplied stable unit must not be split into different synthesis units to
  satisfy wire size limits; reject an oversized unit explicitly if the transport
  cannot preserve its meaning. WS3 `segment` is a separate provider setting.
  Empty input is a recorded route case; an adapter can finish an empty LiveKit
  turn locally rather than fabricating an SDK synthesis request.
- Keep general PCMU bytes separate from the existing 24-to-8 kHz PCM conversion
  preset; encoded Opus/MP3/WAV data must never enter that converter. Replace the
  conformance corpus's universal invalid-rate list with route/format-qualified
  cases as support expands, retaining genuine invalid values and preset conflicts.
- Add an optional ordered event view containing audio and provider timing events
  from one bounded reader/queue. Choose bytes-only or event consumption before
  reading; reject competing readers. Bytes-only mode discards incremental timing
  events without blocking audio. Event mode supplies lossless timing/audio order
  under bounded backpressure and makes metadata available before EOF. Retain the
  existing final timestamp accessor and its success-after-consumption rule.
  Alignment failure is metadata; synthesis failure/cancellation is terminal.
- Preserve wire status (HTTP status, gRPC code, WebSocket kind/close code), error
  origin, request/context identity, and whether audio was delivered. This enables
  plugins to retain distinct LiveKit error and retry mapping. SDK synthesis never
  retries or falls back. Sanitize provider messages, bodies, URLs, and causes.
- Endpoint configuration owns trust checks, direct authorization, and redirect
  policy. Preserve custom path/query and borrowed proxy/TLS behavior. Never send
  credentials to a redirect destination implicitly. Python HTTP currently uses
  session redirect defaults while Node requests `redirect: 'error'`; freeze
  supported same-origin redirect behavior and resolve cross-origin cases before
  claiming parity (Q6). Do not enable the dormant SDK token exchange as part of
  this migration.

## Shared conformance work and open gates

Create `conformance/tts-compat/` in the first implementation PR, not during this
planning task. Its manifest records case ID, plugin revision and package pair,
constructor/update input with presence tags, transport/model, expected request,
events/errors, service qualification, and Python/TypeScript/Go/Rust result.
Use a language-neutral JSON event script and local protocol peers in each test
runner. Use generated protobuf codecs for V1 rather than hand-written JSON field
renaming. Keep packaged Go/Rust copies and CI path filters synchronized.

The IDs below are proposed test families used by every language plan. Every row
of the required capability matrix maps to at least one family.

| ID | Required semantic cases |
| --- | --- |
| C01 Options | Omission/value/clear, false/zero/empty, precedence, copy/replace, nested mutation, delayed start, old call forms, constructor/update snapshots |
| C02 Runtime | Clean package install and public imports at minimum/current supported runtimes; consumer dependency resolution and module loading |
| C03 Transport | All four transports; HTTP early chunks; bidirectional early audio; deterministic selection; no retries after failure before/after audio |
| C04 Endpoints | Exact paths/queries, trust/authorization, redirect policy, binary/JSON, borrowed session settings/lifetime, changed endpoint generations |
| C05 Models | Coda/Mist v3, direct Mist v2, custom IDs, omitted/explicit model differences, endpoint/model update rules |
| C06 Mist | Both bracket flags including explicit false, reduce-latency forwarding/rejection, model restrictions; retain lexicon coverage alongside provider tests |
| C07 Legacy Coda | Four generation controls, omitted/zero values, HTTP/WS3 forwarding and V1 rejection; no claim of engine effect |
| C08 Extensions | Inline speed, normalization, OOV and unknown fields; HTTP collisions; WS3 omission; native typed mappings/rejection |
| C09 Speed | Both controls alone/together/zero/outside native bounds, model-specific defaults, HTTP precedence/clamping, Mist v2 restrictions |
| C10 Audio | Qualified format/rate pairs including 22,050; requested/actual codec metadata; PCM fragmentation/truncation; encoded finalization; both fixed presets; 64 MiB buffer cap |
| C11 Text | All native split choices, WS3 segment, unchanged whitespace/Unicode, stable units/oversize, partial sentences, local flush then more text, empty input and input pauses |
| C12 Timings | WS3 incremental order/context/origin, no metadata consumer, slow event consumer, malformed/missing timings, normalized words, native final-only results, failure after audio, speed/rate units |
| C13 Preparation | Concurrent/canceled prewarm, no speech on prewarm, start/idle/end/overall timeouts, pause semantics, late connection after shutdown |
| C14 Lifetime/errors | Cancel before connect/during send/read/drain; sibling isolation; stale/wrong-context frames; terminal validation; cancel/drain/reuse; retire vs close; status/error-origin and request ID; bounded queues |

Open questions are explicit release gates. Planning can finish with these open;
SDK migration readiness cannot. The owning PR must attach evidence or obtain an
explicit scope resolution, not silently skip a case.

| Gate | Required resolution | Dependency |
| --- | --- | --- |
| Q1 Supported deployment range — resolved | Current released plugins: Python 1.8.8/Agents 1.8.8 and Node 1.9.3/Agents 1.9.3, with declared/documented runtime floors. Earlier plugin releases excluded by user decision. Artifact identity verified; execution qualification remains later work. | T0 scope resolved; Q6 handles exact dependency/runtime qualification |
| Q2 Direct legacy services | Qualify authenticated direct Mist v2 HTTP/WS3 endpoints and required custom IDs/routes. Record URL class, service version/date and accepted options without secrets. Hosted Inference retirement is not evidence. | Model/transport release gate |
| Q3 Audio qualification | Enumerate actual accepted rate/format pairs, including 22,050 and Opus container rates; identify requested versus actual output metadata. | Audio/transport PRs |
| Q4 Timestamp authority | Resolve native proto pre-scaling wording versus Mist length-scaled frames, verify WS3 origin/units, and define malformed timing handling without treating final native results as incremental. | Timing PR and engine/API owner |
| Q5 Extension contract | Freeze legacy collision/filter behavior, including Node HTTP extra `text`, and distinguish forwarded/ignored/rejected controls against each deployed route. | Contract/provider PRs |
| Q6 Endpoint and runtime edges | Set supported Python HTTP redirect behavior and custom proxy/TLS/session combinations; qualify dependency versions at Python 3.10 and Node 20 floors, including exact patch levels and ESM/CommonJS loading. | Runtime/lifecycle/HTTP PRs |
| Q7 Regression budgets | Record baseline first-audio/drain latency, memory, concurrent-stream and cancellation results for both plugins; choose numerical regression thresholds before evaluating implementations. | Qualification PR |
| Q8 Independent SDK upgrades | Choose finite tested SDK dependency ranges and test their oldest/newest releases with fixed Agents and migrated plugin artifacts. Existing alpha versions imply no range guarantee. | SDK publication and later adapter release |

## Language implementation plans

- [Python](livekit-tts-python-plan.md)
- [TypeScript / Node.js](livekit-tts-typescript-plan.md)
- [Go](livekit-tts-go-plan.md)
- [Rust](livekit-tts-rust-plan.md)

These plans map every matrix row to language-specific APIs, files, dependencies,
tests, PR dependencies, and release checks. Shared behavioral decisions and gate
IDs remain here; implementation fixtures become the executable form of them.

[plugin-tts]: https://github.com/livekit/agents/blob/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0/livekit-plugins/livekit-plugins-rime/livekit/plugins/rime/tts.py
[plugin-readme]: https://github.com/livekit/agents/blob/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0/livekit-plugins/livekit-plugins-rime/README.md
[plugin-tests]: https://github.com/livekit/agents/blob/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0/tests/test_plugin_rime_tts.py
[plugin-ws3]: https://github.com/livekit/agents/blob/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0/livekit-plugins/livekit-plugins-rime/livekit/plugins/rime/_legacy_websocket_adapter.py
[plugin-v1]: https://github.com/livekit/agents/blob/43ca0f8ddfc4c54ec9491b2492c20f98974b6cb0/livekit-plugins/livekit-plugins-rime/livekit/plugins/rime/_websocket_v1_adapter.py
[plugin-js]: https://github.com/livekit/agents-js/tree/86b27fd5a586fcfe15b431e79a59d2322b63dd13/plugins/rime
[plugin-js-options]: https://github.com/livekit/agents-js/blob/86b27fd5a586fcfe15b431e79a59d2322b63dd13/plugins/rime/src/options.ts
