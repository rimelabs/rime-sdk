# Go TTS implementation plan

Status: T0 contract checks implemented; runtime/transport features and plugin
adapters remain proposed. Go must
implement the full union of both plugins' capabilities even though neither
official plugin consumes Go. The [shared plan](livekit-tts-sdk-plan.md) owns
behavior, source pins, C01–C14 conformance families and Q1–Q8 release gates.

## Verified starting point

At SDK `b5c1fe4`, [go.mod](../go/go.mod) requires Go 1.24.0 and
`github.com/rimelabs/rime-api/go` v0.4.0. [version.txt](../go/version.txt) is
0.1.0-alpha.8. Dependencies already supply gRPC and protobuf; no HTTP/WebSocket
TTS implementation exists. CI tests Go 1.24 and stable on Linux/macOS/Windows,
including `-race`, and builds with CGO disabled.

[client.go](../go/client.go) exports `Config` and `SynthesisOptions` with public
fields; [stream.go](../go/stream.go) exposes `TextSource`, `Synthesize`, `Stream`,
`AudioStream.Recv` and `Close`. [buffer.go](../go/buffer.go) and
[timestamps.go](../go/timestamps.go) already provide bounded buffered output and
post-EOF timestamps. Preserve zero-value conventions and context cancellation.

## Proposed public API and precedence

Do not change existing exported struct layouts just to add fields: unkeyed
`Config`, `SynthesisOptions`, or `Error` literals may exist outside this module.
Preserve their constructors and method signatures. Add a richer public surface
on a configured TTS handle, backed by the same implementation:

- `client.TTS.Configure(config TTSConfig, defaults RequestOptions)
  (*SynthesisClient, error)`. `TTSConfig` is a new type with constructors/options
  for gRPC, HTTP, WS3 and V1, model, exact URL, V1 encoding and endpoint trust.
  Configuration performs validation, not network I/O. Existing `Config.Endpoint`
  remains a native hostname/port and the root service remains gRPC by default.
- `RequestOptions` has private presence-aware fields, a zero value that inherits
  defaults, `NewRequestOptions(...SynthesisOption)`, and
  `With(...SynthesisOption) RequestOptions` for independent copies. Provide typed
  setters and explicit clear/reset setters for optional controls. A public
  `SynthesisOptions.ToRequestOptions()` preserves the old struct's semantics.
- Configured methods are `Synthesize(ctx, text, options, overrides...)`,
  `Stream(ctx, source, options, overrides...)`, and
  `SynthesizeToBuffer(ctx, text, options, overrides...)`. The options argument is
  `RequestOptions`; overrides are typed `SynthesisOption` functions applied last.
  The old root methods still take the existing `SynthesisOptions` unchanged.
  This avoids weakening existing signatures to `any` or duplicating serializers.
- Functional setters preserve omission versus explicit zero/false. Empty
  collections clear, and `With` snapshots nested slices/maps. Clearing an optional
  provider field removes it; disabling an overall deadline is explicit. Existing
  `Timeout == nil` inherits and pointer-to-zero disables; old zero sample rate
  still selects its default. Do not impose these old zero conventions on new
  presence-aware provider controls.
- `Prewarm(ctx) error`, `ReleaseIdleConnections(ctx) error`, `Retire(ctx) error`
  and `Close() error` on the handle. Retirement stops admission and closes idle
  resources; streams created before retirement keep their generation. It does
  not wait for arbitrarily paused input. `Close` cancels owned operations.
- `AudioStream.Events() (*EventStream, error)` selects ordered events;
  `EventStream.Recv()` returns an owned event or `io.EOF`, while ordinary `Recv()`
  remains bytes-only. Forbid competing modes/readers. Expose `ResolvedAudio()`
  and provider capabilities; preserve `Timestamps()` after successful EOF.

Illustrative future usage:

```go
options := rime.NewRequestOptions(rime.WithVoice("lyra"), rime.WithSampleRate(24000))
tts, err := client.TTS.Configure(config, options)
if err != nil { return err }
defer tts.Close()
if err := tts.Prewarm(ctx); err != nil { return err }
audio, err := tts.Stream(ctx, source, options.With(rime.WithTextPolicy(rime.TextNone)))
if err != nil { return err }
defer audio.Close()
```

Final exported names are frozen in G1. They must retain strong typing and an
unchanged old call path. Maps of additional provider values use a documented
JSON-value type and defensive snapshots, not caller-mutated `map[string]any`
passed straight to the wire. Extension collisions use the shared contract.

## Runtime, dependencies and borrowed resources

Keep Go 1.24.0, all current OS targets and CGO-free builds. Use `net/http` for
streamed HTTP and propose [`github.com/coder/websocket`](https://pkg.go.dev/github.com/coder/websocket)
for context-aware WebSockets and its HTTP-client dial option. Pin a released
version compatible with the existing Go floor in G2; the plan does not assume
that an unqualified latest version is compatible. Use existing `proto` and
`protojson` with the pinned Rime API for binary/canonical JSON V1 messages.

`TTSConfig` accepts a borrowed `*http.Client` and an optional `WebSocketDialer`
interface for callers with custom networking. Ship the standard dialer adapter
in the SDK: injected code establishes a socket, never encodes Rime messages.
Allow proxy/TLS configuration through the HTTP client/transport and preserve it
for upgrades. Return structured handshake failures without credential-bearing
headers or URLs in error messages.

If no client is supplied, create an SDK-owned `http.Transport`/`http.Client`;
never close or modify `http.DefaultTransport`. A borrowed client's transport,
cookie jar and caller timeout remain caller-owned. Set per-request contexts and
document that an externally imposed shorter timeout still applies. If a safe
redirect policy requires a client wrapper/copy, preserve its transport and jar
without mutating the original; Q6 settles supported redirect behavior. Close all
SDK-created response bodies and sockets, but call `CloseIdleConnections` only
on SDK-owned transports. A caller may explicitly request ownership transfer in
a new typed resource option; borrowing is the injection default.

Use `time.Duration` phase budgets and parent contexts. Keep setup and completion
separate from idle progress and overall deadline. Cancellation during WebSocket
I/O may render that connection unusable; only attempt cancel/drain if the library
still permits it. Cleanup uses a fresh bounded context, never the already-canceled
operation context. If reuse cannot be proved, close the socket. No retries.

## Files and implementation structure

Keep the existing root package. Add `tts_options.go`, `tts_config.go`,
`tts_http.go`, `tts_ws3.go`, `tts_websocket_v1.go`, `tts_pool.go`, and
`tts_events.go`; avoid a broad move into subpackages. Extract a private provider
operation interface from `stream.go` while reusing its operation registry,
queues, buffer handling and cancellation conventions. Add per-generation leases
so endpoint/model changes cannot cancel a sibling using old configuration.

Keep existing `*Error`, `errors.Is` categories and `errors.As` behavior.
Add `ProviderDetails()` accessors backed by a typed detail-bearing cause for new
protocol failures, preserving `Error`'s public fields. Do not discard an existing
cause when adding metadata; wrap it through `Unwrap`. Details include origin,
transport status, request/context identity and audio-delivery state. Preserve
legacy gRPC behavior and sanitize messages/causes. LiveKit mapping stays external.

Append audio enum constants without renumbering the existing `iota` values.
Replace range-based format checks in `audio.go` with explicit supported-format
logic; extending the enum must not accidentally feed encoded audio through the
PCM-to-mu-law converter. General PCMU is distinct from `MULAW8000` conversion.

## Complete capability mapping

| Shared matrix row | Go implementation and acceptance |
| --- | --- |
| Options | Presence-aware `RequestOptions`, typed setters/clear/copy, old struct conversion and explicit overrides. C01; external compile fixtures for old APIs. |
| Runtime compatibility | Go 1.24/stable, three OS targets, CGO-free build and pinned WebSocket dependency. C02. |
| Transport | HTTP/WS3/V1 providers alongside existing gRPC; HTTP emits response chunks immediately. C03. |
| Endpoints and sessions | `TTSConfig`, borrowed HTTP client/dialer, exact URL, trust, binary/JSON, preserved proxy/TLS. C04. |
| Models | Typed known IDs plus explicit custom-ID constructor; Mist v2 through compatible legacy routes without changing old native defaults. C05; Q2. |
| Mist controls | Presence-aware bracket setters, protobuf `MistParameters`, legacy `reduceLatency` route rules. C06. |
| Legacy Coda controls | Four typed legacy setters serialize to HTTP/WS3 and reject native V1; no resurrected proto fields. C07. |
| Provider extensions | Typed inline speed/normalization/OOV controls, owned extension values, shared HTTP collisions and WS3 filtering. C08; retain lexicon snapshots. |
| Speed | Distinct speed-alpha/time-scale setters, route-specific bounds and precedence, explicit zero and Mist v2 cases. C09. |
| Output | `OGGOpus`, `WebMOpus`, `PCMU`, qualified rates and `ResolvedAudio`; preserve enum values, presets, WAV and 64 MiB cap. C10. |
| Text policy | Typed native split policy; separate WS3 segment option, unchanged whitespace and stable units. C11. |
| Timings | `EventStream` shares the single bounded reader; bytes-only mode discards live timing events; final `Timestamps` retained. C12; Q4. |
| Preparation and timeouts | Public context-aware prewarm, phase `time.Duration` controls, canceled waiter isolation and close races. C13. |
| Errors and ownership | Provider details without changing old struct layouts, body/socket cleanup, pool leases and sibling isolation. C14. |

## Conformance and Go lifecycle tests

Use `httptest` HTTP/WebSocket peers and existing gRPC peers; consume shared
C01–C14 scripts copied into `go/testdata/tts-compat/`. Add focused `tts_*_test.go`
files and extend existing cancellation, stream-state, timestamp, text and audio
tests. Update CI fixture-copy checks and path filters for the new subdirectory.

Run `go test -race` with concurrent prewarm/synthesis/retirement/close. Test
context cancellation during dial, blocked `Recv`, write, response read and
cancel/drain; operation sources must honor their supplied context as documented.
Assert goroutines exit, response bodies close, late sockets close and borrowed
clients remain usable. Test full audio/event queues, empty sources, two readers,
one canceled sibling, and reused sockets rejecting stale context frames. A
source that ignores cancellation cannot be forcibly stopped; preserve that
documented limitation while ensuring it does not retain SDK network resources.

From `go/` run `go vet ./...`, `go test -race ./...`, and
`CGO_ENABLED=0 go build ./...`; use `GOWORK=off` when an enclosing workspace
would change resolution. Format implementation with `gofmt`. Check a clean
module download/build/test using only the registry API dependency and packaged
fixtures. Audit `tools/package.sh` against actual source layout: it currently
lists an `internal` directory that is not tracked; fix packaging deliberately
in the packaging PR rather than creating an unused directory. Live qualification
and latency/memory thresholds remain shared requirements, not substitutes for CI.

## Work items and coordinated releases

The items below join the shared cross-language PR stages defined in the
[shared plan](livekit-tts-sdk-plan.md#pr-structure-and-completion-gate).
They are not separate language PRs. T0 includes the shared corpus and
validation/loading checks in all four SDK suites; it adds no SDK features.

| Work item | Scope | Dependencies and exit criterion |
| --- | --- | --- |
| G0 | Shared contract fixtures and inventories | Same shared P0; all C families assigned to Go despite no Go LiveKit adapter. |
| G1 | Add reusable TTS configuration | G0; typed option/handle APIs, old external call forms compile, phase limits/error metadata and native prewarm. |
| G2 | Add TTS transport resources | G1; dependency pin, HTTP/dialer injection, pool leases and retirement; race and ownership tests. |
| G3 | Expose TTS provider and text controls | G1; native bracket/split fields and legacy request builders, C05–C09/C11. |
| G4 | Expand TTS audio formats | G1; append-only constants, metadata, qualified rate handling, C10. |
| G5 | Add streaming HTTP synthesis | G2–G4; request equivalence, early bytes, deadlines and body cleanup. |
| G6 | Add WS3 synthesis events | G2–G4; ordered event API, timing delivery, connection reuse/cancellation. |
| G7 | Add native WebSocket synthesis | G6 infrastructure; binary/JSON, ready/started/final state, cancel/drain. |
| G8 | Qualification, packaging and documentation | G5–G7 and all four runners; exported fixtures, source archive, live Q gates and clean module test. |

Follow [RELEASING.md](../RELEASING.md) with Conventional Commit PR titles and
preserved squash titles. Release Please owns versions and `go/v` tags. Verify
the exact public module through the existing release check, without a local
replace directive or private Git credentials; do not move a published tag.
The post-tag check cannot prevent publication, so package and conformance gates
must pass before the release PR is merged. Shared-only commits do not select Go.

Qualify SDK-only upgrades using a fixed external Go consumer and oldest/newest
versions in the declared supported range; Go consumers choose module versions
in `go.mod`. This supports the common compatibility claim but does not replace
the later Python/Node fixed-Agents/plugin tests. No Go LiveKit plugin or hosted
Inference integration is proposed.

## Remaining decisions

G2 must select and qualify the WebSocket dependency and custom dialer contract on
all three OS targets at Go 1.24. Q2–Q6 settle direct service, codec, timing and
network behavior. Q1 is resolved by the shared current-release baseline decision;
Q7/Q8 remain shared qualification/release gates. G1's public
API review must preserve old struct literals and error handling; an alpha version
does not waive those requirements.
