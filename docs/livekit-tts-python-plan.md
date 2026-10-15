# Python TTS implementation plan

Status: T0 contract checks implemented; runtime/transport features and plugin
adapters remain proposed. Implements
every row of the [shared plan](livekit-tts-sdk-plan.md), including capabilities
needed only by the Node plugin. Shared decisions, C01–C14 conformance families,
source pins, and Q1–Q8 release gates are authoritative there.

## Verified starting point

At SDK `b5c1fe4`, [pyproject.toml](../python/pyproject.toml) declares
`rimelabs-sdk` 0.1.0a14, Python >=3.11, `rime-api==0.4.0`, `grpcio>=1.74,<2`,
`httpx>=0.28,<1`, and `websockets>=15,<17`. CI currently covers 3.11 and 3.13.
The pinned Python plugin requires Python >=3.10 and borrows an
`aiohttp.ClientSession`. Its 1.8.8 source release raises the Agents dependency
floor but leaves the TTS implementation unchanged; see the shared source recheck.

[The public client](../python/src/rimelabs_sdk/_client.py) exposes synchronous
stream creation, async context management and `close()`. The private
[TTS client](../python/src/rimelabs_sdk/tts/_client.py) accepts repeated keyword
arguments, with no public options object or preparation method. Existing
[`AudioStream`](../python/src/rimelabs_sdk/tts/_stream.py) supplies bytes, request
ID, cancellation and final timestamps; preserve these call forms and semantics.

## Proposed public API and option resolution

Export the following from `rimelabs_sdk` and its public `tts` package:

- Frozen `SynthesisOptions`, `TTSConfig`, `Timeouts`, `TextPolicy`,
  `ResolvedAudio`, `ProviderCapabilities`, `SynthesisEvent`, and explicit
  `UNSET`/`CLEAR` values. Options include existing voice/language/audio/rate,
  time scale, timestamps and lexicon fields plus the shared provider controls.
  `SynthesisOptions.replace(**changes)` returns an independent snapshot.
- `client.tts.configure(config, *, defaults=...) -> SynthesisClient` constructs
  an immutable configured handle with no network activity. `TTSConfig` selects
  gRPC, HTTP, WS3 or V1, model, exact endpoint, V1 encoding, endpoint trust and
  borrowed HTTP session. Existing `Rime(model=..., endpoint=...)` remains native
  gRPC; its hostname validation and defaults stay intact.
- All existing `synthesize`, `stream`, and `synthesize_to_buffer` methods gain
  `options=...` while retaining named keyword overrides. Explicit keywords win
  over options, then handle defaults, then the existing SDK defaults. Change
  internal keyword defaults to an omission sentinel so `False`, empty lexicons,
  and explicitly supplied defaults can override an options object. Preserve
  existing `voice=None`, `audio_format=None`, and `timeout=None` behavior.
  `CLEAR` removes an inherited optional provider value; it does not mean false.
- The configured handle has the same synthesis methods plus async `prewarm()`,
  `release_idle_connections()`, `retire()`, and `close()`. Retirement refuses
  new operations and releases idle resources; existing operations retain the
  generation until their terminal cleanup. Closing cancels its operations.
- `audio.events()` selects an async iterator of audio/timing/metadata events;
  ordinary `async for chunk in audio` remains bytes-only. Selection is exclusive
  and made before consumption. `audio.resolved_audio` distinguishes requested
  and known actual encoding/rate; unavailable actual metadata remains unknown.
  `audio.timestamps()` keeps its current final-result contract.

Illustrative future usage (not runnable today):

```python
options = SynthesisOptions(voice="lyra", language="eng", sample_rate=24000)
tts = client.tts.configure(
    TTSConfig(transport="v1", endpoint=endpoint, model="coda", http_session=session),
    defaults=options,
)
await tts.prewarm()
audio = tts.stream(text_units, options=options.replace(text_policy=TextPolicy.NONE))
async with audio:
    async for event in audio.events():
        consume_provider_event(event)
```

Use enums for transport/encoding/policy internally and accept documented string
values where appropriate. Snapshot lexicons and extension mappings recursively;
`frozen=True` alone does not protect nested caller-owned data. New configuration
does not mutate a live handle. Provider-extension precedence follows the shared
contract, independently of Python keyword precedence. No LiveKit type is imported.

## Runtime and dependencies

1. Restore Python 3.10 support before adding the plugin dependency. The published
   [`rime-api` 0.4.0 metadata](https://pypi.org/pypi/rime-api/0.4.0/json) already
   permits >=3.10; qualify the full resolved graph rather than changing that pin
   speculatively. Add conditional `typing_extensions` for `Self`/`NotRequired`
   and `async-timeout` for the 3.11 timeout APIs, or a single equivalent tested
   compatibility module. Use one timeout abstraction and its matching exception
   type across `_auth.py`, TTS, STT and realtime; preserve task cancellation.
2. Audit package-wide imports: `_client.py`, TTS/STT streams, realtime protocol
   typing, and realtime session deadlines all use newer APIs. A TTS-only patch
   that leaves `import rimelabs_sdk` broken on 3.10 is insufficient. These are
   mechanical runtime prerequisites, not new STT/Prism features.
3. Add `aiohttp` as a direct SDK dependency for TTS HTTP/WS3/V1. Accept the actual
   caller's `ClientSession`; wrapping it in an unrelated `httpx` session would
   lose connector/proxy/TLS behavior. Keep existing `httpx` and `websockets`
   dependencies for their current responsibilities. Select an `aiohttp` range
   that resolves with pinned LiveKit environments and Python 3.10; record it in
   `pyproject.toml` and regenerate `uv.lock` in the runtime PR.
4. Add Python 3.10 to CI, retain 3.11/3.13 and the existing 3.12 development setup,
   and add current supported versions after checking dependency wheels. Check
   fresh wheel/sdist installs, not only the uv workspace. Do not raise existing
   users' runtime minimum. Set Ruff/mypy targets and examples consistently.

## Transport and resource implementation

Keep `_client.py`, `_auth.py` and `_errors.py` as shared infrastructure. Add
`tts/_options.py`, `_configuration.py`, `_events.py`, `_http.py`, `_ws3.py`,
`_websocket_v1.py`, and `_pool.py`; retain `_transport.py` as the native gRPC
implementation. Refactor `_stream.py` around a private provider-operation
interface while retaining its public `AudioStream` type and bounded byte queue.
Reuse existing validation, buffers, timestamp parsing and lexicon snapshots.

The SDK creates an `aiohttp` session lazily on its event loop if none is supplied.
It owns that session and every response/socket it opens. An injected session is
borrowed: never close it, replace its connector, or mutate its defaults. Reject
closed or incompatible-loop sessions clearly. Keep existing process/event-loop
affinity checks. Borrowed sessions may serve both HTTP and WebSocket requests.
Validate endpoint trust before adding authorization; prevent credential-bearing
redirects outside the explicitly permitted policy (Q6).

Use one receiver per WebSocket and one active synthesis per leased socket.
WS3 parses chunk/timestamps/done and sends final wire flush only at input EOF.
V1 uses the pinned `rime-api` protobuf classes and `google.protobuf.json_format`
for canonical JSON, validates oneof/frame/subprotocol/ready/started/terminal
events, and cancels/drains before reuse. Close a socket whose clean state cannot
be established. Canceling a lease must not cancel another operation or prewarm.

`Timeouts` uses seconds and separate optional budgets from the shared contract.
Map the Python plugin's HTTP model total and socket-connect limit separately;
WS3 receive timeouts and V1 start/end watchdogs need distinct settings. Do not
silently substitute a session's total timeout for these phases. Extend
`RimeError` with optional sanitized provider metadata while preserving existing
subclasses and `request_id`; propagate task cancellation rather than wrapping it
as an ordinary connection failure.

## Complete capability mapping

All C families run in this SDK, irrespective of which plugin needs them.

| Shared matrix row | Python implementation and acceptance |
| --- | --- |
| Options | `_options.py`; public frozen options and replace, named overrides, presence tracking and deep snapshots. C01, including old keyword calls. |
| Runtime compatibility | Python 3.10 backports, dependency lock and clean installs above. C02. |
| Transport | `_http.py`, `_ws3.py`, `_websocket_v1.py` alongside gRPC; HTTP yields before body EOF and never buffers an open text source. C03. |
| Endpoints and sessions | `TTSConfig`, borrowed `aiohttp` session, exact URL/query, V1 binary/JSON and SDK trust checks. C04. |
| Models | Handle-level model strings; Mist v2 legacy routes and custom IDs, without broadening old gRPC model validation. C05; Q2 blocks qualification. |
| Mist controls | Optional bracket booleans in native `MistParameters` and legacy requests; `reduce_latency` only where supported. C06 includes false/omitted. |
| Legacy Coda controls | `temperature`, `top_p`, `repetition_penalty`, `max_tokens` forwarded over compatible HTTP/WS3; explicit native rejection. C07. |
| Provider extensions | Typed inline speed/normalization/OOV controls plus snapshotted `provider_parameters`; shared collision rules and route filters. C08; preserve Coda lexicons. |
| Speed | Separate `speed_alpha` and `time_scale_factor`; route-aware resolution, no global reciprocal conversion or new legacy native bounds. C09. |
| Output | Extend `_audio.py` with Ogg Opus, WebM Opus and general PCMU; qualify rates, preserve `PCM_24000`, `MULAW_8000`, WAV/buffer behavior. C10. |
| Text policy | `TextPolicy` maps native enums; separate `segment` string for WS3. Preserve source whitespace and stable units; no SDK tokenizer. C11. |
| Timings | `_events.py` ordered event view plus existing final accessor; no second reader or unconsumed metadata queue. C12; Q4. |
| Preparation and timeouts | Public handle preparation and phase budgets, coalesced prewarm, cancellation and shutdown races. C13. |
| Errors and ownership | Optional provider metadata on existing errors; handle/session ownership, pool retirement, bounded shutdown, no synthesis replay. C14. |

## Tests and checks

Add `python/tests/test_tts_options.py`, `test_tts_http.py`, `test_tts_ws3.py`,
`test_tts_websocket_v1.py`, `test_tts_events.py`, and a controlled protocol peer.
Read `conformance/tts-compat/` directly. Extend existing audio, text, timestamp,
transport, endpoint and limit tests rather than copying their gRPC coverage.

Python-specific lifecycle tests must exercise `asyncio.Task.cancel()` during
acquisition/send/receive, early async-iterator exit and explicit `cancel()`,
async-generator finalizers, cancellation while awaiting `close()`, borrowed
session reuse after SDK shutdown, wrong-loop/process use, and simultaneous
prewarm/retire/close. Assert no pending SDK tasks or unclosed session warnings.
Test client shutdown with unconsumed streams, blocked source generators and
full event queues. Keep external task cancellation distinguishable on 3.10.

Run the documented checks from `python/`: `uv sync --locked --dev`, Ruff lint
and format checks on `src tests examples`, `uv run mypy src`, `uv run pytest`,
and `uv build`. Run all suites after package-wide runtime/cleanup changes.
Install built distributions outside the workspace, exercise public exports and
the local protocol peers, and check examples against that exact package.
Live service and performance checks follow shared Q2–Q4/Q7; fake peers alone
cannot certify those results.

## Work items and coordinated releases

The items below join the shared cross-language PR stages defined in the
[shared plan](livekit-tts-sdk-plan.md#pr-structure-and-completion-gate).
They are not separate language PRs. T0 includes the shared corpus and
validation/loading checks in all four SDK suites; it adds no SDK features.

Each coordinated PR includes its tests and documentation. Split by capability
if necessary; keep the four language implementations of that capability together.

| Work item | Scope | Dependencies and exit criterion |
| --- | --- | --- |
| P0 | Shared contract fixtures and both plugin inventories | Shared first PR; C01–C14 cases and Q gates recorded for all four languages. |
| P1 | Support Python 3.10 runtimes | P0; dependency resolution, all package imports and existing suites pass at supported floors. |
| P2 | Add reusable TTS configuration | P1; options, public configured handles, timeouts, error metadata, retirement/prewarm contract; C01/C04/C13/C14 native cases. |
| P3 | Expose TTS provider and text controls | P2; native bracket/split support, legacy request mapping and validation fixtures; C05–C09/C11. |
| P4 | Expand TTS audio formats | P2; formats, resolved metadata, existing presets and buffer regressions; C10. |
| P5 | Add streaming HTTP synthesis | P3/P4; exact legacy bodies, borrowed sessions and early audio; C03/C04 plus HTTP cases. |
| P6 | Add WS3 synthesis events | P3/P4; ordered event API, WS3 timings and pool lifecycle together; C11–C14. |
| P7 | Add native WebSocket synthesis | P6 pool/event infrastructure; binary/JSON, terminal validation and cancel/drain; all V1 cases. |
| P8 | Qualification and package documentation | P5–P7 and all four SDK runners green; resolve Q gates, live evidence and clean installs. |

Release via [RELEASING.md](../RELEASING.md), preserving Conventional Commit titles
on squash merge. Use Release Please's actual Python alpha version and tag;
validate the wheel/sdist, publishing job and exact-version published example
checks. Shared-file-only commits do not select a Python release. Do not manually
edit versions or claim Python readiness before four-language capability parity.

For the later adapter phase, pin an Agents/plugin artifact pair and change only
`rimelabs-sdk` within an explicitly tested finite range. Test oldest/newest
versions, clean resolution and lockfile replacement while retaining the same
Agents and plugin artifacts. Initial migration must follow LiveKit's release
policy; an already published plugin cannot gain this dependency retroactively.
No plugin code belongs in P0–P8.

## Remaining decisions

Q1 is resolved: plugin/Agents 1.8.8 is the baseline, with Python 3.10 support
required; earlier plugin releases are excluded. Q6 determines `aiohttp` bounds,
redirect behavior and exact dependency/runtime qualification. P1 must prove its backports
against resolved dependencies; metadata alone is insufficient. Q2–Q5 and Q7–Q8
remain shared release gates. API names above are the proposed design to freeze
in P2; they are not permission to omit any capability if a spelling changes.
