# Development

Clone this repository to develop and test the SDKs.
Start each command block from the repository root.

## Source layout

Python and TypeScript keep the shared client, credentials, and errors at the package
root. Feature directories own their configuration, transport, and operation
state.

| Responsibility | Python | TypeScript |
| --- | --- | --- |
| Shared client and cleanup | `python/src/rimelabs_sdk/_client.py` | `typescript/src/client.ts` |
| Shared credentials and errors | `_auth.py`, `_errors.py` | `auth.ts`, `errors.ts` |
| Shared input PCM values and conversion | `_pcm.py` | `pcm.ts` |
| TTS, voice and language discovery | `python/src/rimelabs_sdk/tts/` | `typescript/src/tts/` |
| Realtime / Prism | `python/src/rimelabs_sdk/realtime/` | `typescript/src/realtime/` |
| Streaming speech recognition | `python/src/rimelabs_sdk/stt/` | `typescript/src/stt/` |

The shared client delegates feature cleanup and then releases credentials.
TTS owns synthesis streams, discovery requests, and the gRPC connection.
Realtime owns WebSocket connections and conversation state. Shared credentials
retain the TTS token-exchange implementation as well as direct API-key handling.

Authentication in TypeScript accepts an explicit configuration with
`exchangeUrl`, `audience`, and `authTimeout`. TTS supplies these values from its
deployment configuration. Shared authentication does not import TTS modules.
The shared `cancellation.ts` module owns the `abortable` helper.

Prism and STT input preparation share PCM16 conversion and the reference samples
in `conformance/pcm-input.json`. The input format is 8/16/24/48 kHz, mono or
interleaved stereo; conversion produces signed little-endian PCM16 mono 16 kHz.
Each operation owns its converter state. Python exposes `PCMFormat` from the
package root and retains the same value at `rimelabs_sdk.realtime.PCMFormat`.
TypeScript retains its root `PCMFormat` export.

STT input preparation accepts arbitrary byte boundaries, keeps incomplete frames
between chunks, and refuses a truncated frame at end of input. Each output
payload contains complete frames and is at most 64 KiB. Conversion is lazy and
uses bounded portions of the source chunk so a transport can apply backpressure.
Prism retains its complete-frame input requirement, 192,000-byte send limit,
40 ms wire appends, and cancellation rollback behavior.

STT owns its independent gRPC connection, recognition operations, and typed
transcript queue. The queue preserves atomic snapshots; it cannot use the audio
byte queue, which splits data. TTS and STT share gRPC status mapping and endpoint
validation; Node also shares bidirectional call completion handling.

Shared STT protocol cases live in `conformance/stt/transcripts.json`. Local gRPC
peers exercise the public API, cancellation and terminal status handling without
credentials. [Live qualification](conformance/stt/README.md) uses the configured
API key and production TLS endpoints; fake-server tests alone do not demonstrate
deployed compatibility.

TTS uses `client.tts.synthesize(text)` for complete strings and
`client.tts.stream(chunks)` for async text sources. Both return streaming audio.
Both packages expose `client.realtime.connect(...)`. Voice and language discovery
remain on the top-level client. Tests that use private dependency seams import
them from the feature directories.

## Python

All three SDKs consume the published, versioned API packages pinned in their
manifests and lockfiles. Those packages contain the STT definitions, so normal
clean installs can run the full suites without a local schema build.

Every SDK has an `examples/` directory. Examples use the local SDK by default.
Python uses a uv workspace under `python/`; TypeScript and its examples are npm
workspace members declared at the repository root. Go and Rust examples belong
to their SDK module or crate. See [the example index](docs/examples.md) for run
commands and instructions for using published packages outside this repository.

Use Python 3.11 or later and `uv`:

```sh
cd python
uv sync --locked --dev
uv run ruff check src tests examples
uv run ruff format --check src tests examples
uv run mypy src
uv run pytest
uv build
```

## Node.js

Install Node.js 22 or later, then run:

```sh
npm ci
cd typescript
npm run check
npm run lint
npm test
npm pack --workspace @rimelabs/sdk
```

Check examples from the repository root:

```sh
uv sync --project python/examples --locked
npm ci
npm --prefix typescript run build
npm run check --workspace rime-sdk-examples
npm test --workspace rime-sdk-examples
```

Python example tests run with the SDK's pytest suite. TypeScript example tests
use the shared controlled Prism peer. Neither suite opens audio devices. Device
release checks are listed in [the example guide](docs/examples.md).

For changes to release automation, run
`uv run --project python pytest .github/scripts/tests` from the repository root.

## Go

From the repository root:

```sh
cd go
go vet ./...
go test -race ./...
CGO_ENABLED=0 go build ./...
```

The Go module supports TTS and STT. Its root package owns the shared client,
operation registry, endpoint validation and error mapping. `transcription*.go`
owns recognition; `transcript*.go` owns protocol validation and the atomic update
queue; `pcm.go` owns input conversion. The input converter is separate from
`audio.go`, which converts 24 kHz TTS output to mu-law using a different filter.

Keep `go/testdata/` copies of the four shared TTS fixtures, `pcm-input.json` and
`stt/transcripts.json` identical to `conformance/`; CI checks them. Each module export includes
those fixtures so tests work outside the monorepo. The versioned
`github.com/rimelabs/rime-api/go` dependency supplies both TTS and STT protocol
code. BlingFire build instructions are in `go/internal/`. Run `gofmt` on Go changes.

When this checkout is nested inside another Go workspace, run module checks with
`GOWORK=off` to avoid inheriting its `go.work`. The Go release archive includes
the README with TTS and STT documentation and the `examples/transcribe` command.

## Documentation

Keep the package READMEs focused on users of the installed SDKs. Show package
imports, working examples, defaults, and behavior that affects callers.
Place repository setup and release commands in maintainer documentation.

Public documentation ships with each package release. Check README examples
against the corresponding SDK before publishing. Keep all languages' examples
and option tables consistent while preserving their different API conventions.

Each package README contains installation and quick starts. Detailed API guides
live in `python/docs/` and `typescript/docs/`; Go documents both APIs in
`go/README.md`. Examples live under each SDK directory. The shared index is `docs/examples.md`.
Use absolute repository URLs in package READMEs so guide links work on PyPI and
npm. The Python source distribution and npm package include their guides.

Prism example tests use a controlled WebSocket peer and require no credentials
or audio hardware. The example scripts themselves connect to the endpoint the
reader configures. A microphone example still needs validation of capture,
playback, interruption, and cleanup on real devices before it is added.

See [RELEASING.md](RELEASING.md) for release PRs, registry setup, and recovery.

## Prism and the local LiveKit plugin

The Python realtime module owns the Prism protocol. The LiveKit Rime plugin owns
LiveKit history, generation streams, and playback reports. Keep raw wire events
out of the plugin. Test the public SDK with a controlled peer; the pinned schema
is in `conformance/prism/`.

Run these commands from the `agents` checkout to test both checkouts before release:

```sh
uv pip install --python .venv/bin/python --editable ../rime-sdk/python
uv run --no-sync pytest tests/test_plugin_rime_realtime.py -q --unit
```

`--no-sync` keeps the local SDK installation for this run. No local filesystem
path belongs in a published dependency. Publish the Prism-enabled SDK and set the
plugin's minimum SDK version to that release before publishing the plugin.
## Rust

Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`,
`cargo test --locked`, and `cargo package --locked` from `rust/`.
Tests use local gRPC services and the shared
conformance cases. Keep `rust/testdata` identical to the corresponding files in
`conformance/`. Keep the BlingFire binary identical to the Go copy.

Local checks, CI, and releases use the published `rimelabs-api` dependency
from crates.io. No sibling API checkout or Cargo override is required.

