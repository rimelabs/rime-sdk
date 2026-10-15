# TypeScript / Node.js TTS implementation plan

Status: T0 contract checks implemented; runtime/transport features and plugin
adapters remain proposed. Implements
every row of the [shared plan](livekit-tts-sdk-plan.md), including Python-only
consumer needs such as caller-owned sessions. Source pins, shared behavior,
C01–C14 test families and Q1–Q8 gates are defined there.

## Verified starting point

At SDK `b5c1fe4`, [package.json](../typescript/package.json) declares
`@rimelabs/sdk` 0.1.0-alpha.15, Node >=22, ESM-only exports and TypeScript 5.9.
Dependencies already include `ws`, `@bufbuild/protobuf`, `@grpc/grpc-js` and
`@rimelabs/api` 0.4.0. CI covers Node 22 and 24. The installed API package has
both import and require exports; do not assume the SDK does too.

The [SDK TTS client](../typescript/src/tts/client.ts) already accepts
`SynthesisOptions`, but has no public copy/reset contract, transport selection,
session injection, or preparation API. `AudioStream` is an async byte iterator;
buffered synthesis is a separate Promise. Preserve this distinction.

The pinned [Node plugin manifest](https://github.com/livekit/agents-js/blob/1871ba2c4eb8fa69e8d04ee60365eab468d051e1/plugins/rime/package.json)
exports ESM and CommonJS, pins `@rimelabs/api` 0.0.1 and uses Agents/RTC peer
dependencies. Neither that manifest nor the inspected Agents manifest declares
a Node engine floor. Upstream CI uses Node 24; that alone does not establish the
supported minimum. The published 1.9.3 plugin pins its Agents peer to exactly
1.9.3 and RTC to `^1.1.0`; this is the Q1 baseline. Official LiveKit documentation
requires Node 20+, while exact patch/dependency resolution remains Q6. The release
changes metadata and Agents exports, not the previously reviewed Rime implementation.

## Proposed public API and precedence

Keep `Rime`, `AudioStream`, `SynthesisOptions`, `AudioFormat` and every existing
method signature valid. Add public root exports for `TTSConfig`,
`SynthesisClient`, `SynthesisOverrides`, `TtsTimeouts`, `TextPolicy`,
`ResolvedAudio`, `ProviderCapabilities`, `SynthesisEvent` and `CLEAR`.

- `client.tts.configure(config, defaults?)` returns a configured immutable handle.
  A discriminated config union selects `grpc`, `http`, `ws3` or `v1`, with exact
  endpoint, model, binary/JSON encoding, trust setting and resource bindings.
  Existing `new Rime({model, endpoint})` continues using native gRPC.
- Both existing and configured methods accept
  `synthesize(text, options?, overrides?)`, `stream(source, options?, overrides?)`
  and `synthesizeToBuffer(text, options?, overrides?)`. Existing two-argument
  calls remain identical. Overrides win over supplied options over handle/SDK
  defaults. Add `copySynthesisOptions(options, changes)`; it retains omission
  and supports `CLEAR` without requiring plugin code to reconstruct every field.
- Treat `undefined` as omitted. Preserve existing nullable field behavior
  (`voice`, format, rate, time scale and timeout); do not reinterpret every null
  as a provider clear. Use explicit `CLEAR` in the new override type for reset.
  False, zero where valid, and empty arrays/maps are explicit values. Defensively
  copy nested data synchronously when the operation is created, before lazy I/O.
- Add `prewarm({signal}?)`, `releaseIdleConnections()`, `retire()` and `close()`
  on handles. Retirement closes idle connections and defers remaining cleanup
  until existing streams finish; it must retain streams not yet iterated.
  Root `close()` still waits for all owned work. Preserve async disposal.
- `audio.events()` exposes one ordered async event iterator; bytes-only iteration
  remains the default. Reject switching modes after a read or simultaneous
  readers. Keep `audio.timestamps()` final-only and add `audio.resolvedAudio`.
  Do not expose LiveKit frames, tokenizer types or error classes.

Illustrative future usage:

```typescript
const tts = client.tts.configure(
  { transport: "ws3", endpoint, model: "mistv2" },
  { voice: "luna", sampleRate: 22050, segment: "bySentence" },
);
await tts.prewarm({ signal });
const audio = tts.stream(textUnits, options, { speedAlpha: 1.1 });
try {
  for await (const event of audio.events()) consumeProviderEvent(event);
} finally {
  await audio.cancel();
}
```

Keep configuration/default resolution outside protocol serializers. Legacy HTTP
extension merging follows the shared contract, including Node's extra `text`
case; typed option precedence is a separate operation. Never automatically copy
the entire plugin options object onto the wire. Its SDK-control fields and WS3
filtering differ from HTTP forwarding.

## Runtime and packaging work

Add Node 20 support while retaining Node 22/24. Q1 selects the current plugin's
documented runtime floor; Q6 must qualify its exact patch level and dependency
graph before adapter adoption. Audit `AbortSignal.any`, web stream
APIs, disposal symbols and transitive dependencies at the actual minimum. Reuse
`cancellation.ts` for abort composition and listener cleanup. Node TTS remains a
server SDK; browser/edge TTS support is not implied by browser examples.

Add CommonJS loading with type declarations while preserving the current ESM
entry point. Prefer a single CommonJS runtime implementation with an ESM facade
and matching declaration entry points to preserve `AudioFormat` and error-class
identity when both loaders coexist. Confirm the build can preserve all current
exports; if separate builds are necessary, test cross-entry option/error values
and remove identity-dependent validation. Do not rely on modern Node's ability
to require some ESM modules as the compatibility strategy. Compile ESM `.mts` and
CommonJS `.cts` consumers and run `require()`/`import()` against the packed tarball.

Use existing `ws` for WebSocket connections and native fetch for default HTTP.
Public `HttpSession` can bind a `fetch` implementation that owns its dispatcher
and proxy/TLS settings; public `WebSocketConnector` creates a `ws` connection
using caller settings. Export SDK-owned session helpers for common agent/TLS
configuration so callers need not implement a protocol. No new HTTP runtime
library is required unless Q6 proves native fetch cannot preserve a supported
configuration. Keep npm lockfile, exports, declarations and package files aligned.

## Transport and lifetime implementation

Add `tts/options.ts`, `configuration.ts`, `events.ts`, `http.ts`, `ws3.ts`,
`websocket-v1.ts`, and `pool.ts`. Keep `transport.ts` for gRPC and reuse shared
`grpc.ts`, `auth.ts`, `cancellation.ts`, queues, buffer finalization and lexicons.
Refactor `stream.ts` to consume a private provider operation without exposing
private methods in the eventual adapter.

An injected session/connector is borrowed; do not close its dispatcher/agent or
mutate its configuration. The SDK owns request abort controllers, response-body
readers and sockets returned specifically for its lease. Cancel the body and
release its reader in all exit paths. A connector resolving after cancellation
must have its newly opened socket closed. Never close Node's global fetch pool.
Credential generation, trusted endpoints and redirect policy remain SDK-owned.

WebSocket peers must validate the selected subprotocol and ready/version event,
then enforce one active context per socket. Use `create`/`toBinary`/`fromBinary`
and canonical protobuf JSON functions from the existing API/Buf dependencies.
Validate frame types, mutually exclusive payloads, IDs, terminal ordering and
base64. WS3 has its own JSON parser and final flush. Cancel/drain V1 before reuse;
discard a poisoned socket. Keep old pool generations alive across option updates.

Existing SDK `timeout` is in seconds. Keep that unit; introduce explicit `*Ms`
fields in `TtsTimeouts` for setup/write/start/idle/completion/cleanup/overall
budgets and reject conflicting old/new overall settings unless precedence resolves
them explicitly. The adapter can pass LiveKit `timeoutMs` without confusing it
with seconds. Preserve per-read HTTP bounds and V1 open-input pauses.
Extend `RimeError` options additively with sanitized provider status/origin,
request/context ID and delivered-audio state; leave LiveKit retry decisions out.

## Complete capability mapping

| Shared matrix row | TypeScript implementation and acceptance |
| --- | --- |
| Options | `SynthesisOptions`, override/copy helpers, explicit reset and deep snapshots; old method overloads preserved. C01. |
| Runtime compatibility | Node floor qualification, ESM/CJS artifacts and type declarations, clean peer resolution. C02. |
| Transport | Explicit four-way config; HTTP streams chunks and rejects incremental input; WS3/V1 coexist with default gRPC. C03. |
| Endpoints and sessions | Exact URLs, `HttpSession`/`WebSocketConnector`, borrowed agent ownership, trust and subprotocol checks. C04. |
| Models | Configured model strings, Mist v2 legacy routes and custom IDs, retaining gRPC defaults/validation. C05; Q2. |
| Mist controls | `pauseBetweenBrackets`, `phonemizeBetweenBrackets`, route-appropriate `reduceLatency`; preserve explicit false. C06. |
| Legacy Coda controls | Typed `temperature`, `topP`, `repetitionPenalty`, `maxTokens` mapped to exact legacy names; native rejection. C07. |
| Provider extensions | `providerParameters` plus inline speed/normalization/OOV types; shared collisions and route filtering, including Python's Coda speed capability. C08. |
| Speed | Separate `speedAlpha`/`timeScaleFactor`, route-specific forwarding/bounds/defaults and Mist v2 restriction. C09. |
| Output | Named `OGG_OPUS`, `WEBM_OPUS`, `PCMU`; resolved audio metadata, qualified rates, unchanged presets/WAV/buffer limit. C10. |
| Text policy | Native split enum and independent WS3 `segment`; Unicode-safe accumulation chunks, unsplit stable units, no SDK tokenizer. C11. |
| Timings | Ordered `SynthesisEvent` iterator, bytes-only discard, bounded memory; preserve final accessor and alignment status. C12; Q4. |
| Preparation and timeouts | Coalesced prewarm with independent AbortSignals and phase budgets; idle release, late acquisition and close races. C13. |
| Errors and ownership | Additive error metadata, request IDs, old pool retention, reader/socket cleanup, no synthesis retry. C14. |

## Tests and validation

Add options, HTTP, WS3, V1, event and packaging tests under `typescript/tests/`;
extend `fixtures/tts.ts` and type tests. Read the same shared C01–C14 event scripts
as the other SDKs, using local HTTP/`ws` servers and existing gRPC peers.

Node-specific tests include `AbortSignal` before construction and during each
I/O phase, async-iterator `return()`/early break, failed or blocked source `next()`,
stream/client async disposal, socket send callback failures, and delayed connector
resolution after close. Assert no unhandled Promise rejections, leaked listeners,
timers or open handles. Fragment UTF-16 surrogate pairs in accumulator mode;
stable-unit mode rejects malformed/oversized units without changing boundaries.
Test slow event consumers, paused input, full queues and sibling cancellation.

From the repository root run `npm ci`; from `typescript/` run `npm run check`,
`npm run lint`, `npm test`, and `npm pack --workspace @rimelabs/sdk`. Run example
type/tests from their workspace as documented in [CONTRIBUTING.md](../CONTRIBUTING.md).
Install the tarball outside the workspace under every qualified Node version,
test both loaders and declarations and rerun public protocol smoke cases.
Packaging changes must also smoke-test STT/Prism imports and existing exports.
Live qualification and measured regression budgets remain separate gates.

## Work items and coordinated releases

The items below join the shared cross-language PR stages defined in the
[shared plan](livekit-tts-sdk-plan.md#pr-structure-and-completion-gate).
They are not separate language PRs. T0 includes the shared corpus and
validation/loading checks in all four SDK suites; it adds no SDK features.

| Work item | Scope | Dependencies and exit criterion |
| --- | --- | --- |
| T0 | Shared contract and plugin inventories | Same shared P0 as the other plans; Node omission/default/update and HTTP extension cases recorded. |
| T1 | Support compatible package loaders | T0/Q1/Q6; Node matrix and ESM/CJS tarball/type checks pass, current imports unchanged. |
| T2 | Add reusable TTS configuration | T1; options, public handles, phase limits, resource bindings, errors and native preparation; C01/C04/C13/C14. |
| T3 | Expose TTS provider and text controls | T2; typed native fields plus legacy mapping/validation and split policies; C05–C09/C11. |
| T4 | Expand TTS audio formats | T2; metadata, formats/rates, existing converter and buffer cases; C10. |
| T5 | Add streaming HTTP synthesis | T3/T4; exact payloads, early bytes, read deadlines and injected-session cleanup. |
| T6 | Add WS3 synthesis events | T3/T4; bounded ordered events, incremental timings, pool generations and lifecycle. |
| T7 | Add native WebSocket synthesis | T6 infrastructure; both codecs, terminal validation and cancellation/reuse. |
| T8 | Qualification and package documentation | T5–T7, all four SDK runners, Q gates and clean published artifact checks. |

Follow [RELEASING.md](../RELEASING.md): Conventional Commit titles, squash title
preservation, Package checks and a reviewed Release Please release PR. Keep npm
alpha/version policy; no manual version/tag changes. Verify both packed and
published exports, exact-version clean installs and published example checks.
Shared changes alone do not select an npm release. Retain upstream attribution
when adapting protocol code.

The later adapter must remain in `livekit/agents-js`. After initial adoption via
its actual package policy, test upgrading only `@rimelabs/sdk` with Agents, the
migrated Rime plugin and RTC dependencies held fixed. Declare only the finite
SDK range proven by oldest/newest artifact tests; alpha semver and an open peer
range are not evidence of unrestricted compatibility. No adapter implementation
or official plugin release is part of T0–T8.

## Remaining decisions

Q1 is resolved: plugin/Agents 1.9.3 and the documented Node 20 major floor;
earlier plugin releases are excluded. Q6 must qualify the exact patch floor and
dependency graph. T1 must validate the proposed single-runtime dual-loader
build against all root exports. Q2–Q5/Q7/Q8 block service qualification, timing,
extension claims and release compatibility just as in the other SDKs. No
dependency minimum is invented here where source evidence is absent.
