# Development

These instructions require access to the private source repository.
Start each command block from the repository root.

## Source layout

Both languages keep the shared client, credentials, and errors at the package
root. Feature directories own their configuration, transport, and operation
state.

| Responsibility | Python | TypeScript |
| --- | --- | --- |
| Shared client and cleanup | `python/src/rimelabs_sdk/_client.py` | `typescript/src/client.ts` |
| Shared credentials and errors | `_auth.py`, `_errors.py` | `auth.ts`, `errors.ts` |
| TTS, voice and language discovery | `python/src/rimelabs_sdk/tts/` | `typescript/src/tts/` |
| Realtime / Prism | `python/src/rimelabs_sdk/realtime/` | `typescript/src/realtime/` |

The shared client delegates feature cleanup and then releases credentials.
TTS owns synthesis streams, discovery requests, and the gRPC connection.
Realtime owns WebSocket connections and conversation state. Shared credentials
retain the TTS token-exchange implementation as well as direct API-key handling.

Authentication in TypeScript accepts an explicit configuration with
`exchangeUrl`, `audience`, and `authTimeout`. TTS supplies these values from its
deployment configuration. Shared authentication does not import TTS modules.
The shared `cancellation.ts` module owns the `abortable` helper.

Public imports and `client.tts.stream(...)` remain unchanged.
Both packages expose `client.realtime.connect(...)`. Moving implementation files
does not change these interfaces or the top-level voice and language discovery
methods. Tests that use private dependency seams import them from the feature
directories.

## Python

Use Python 3.11 or later and `uv`:

```sh
cd python
uv sync --locked --dev
uv run ruff check src tests ../examples/python
uv run ruff format --check src tests ../examples/python
uv run mypy src
uv run pytest
uv build
```

## Node.js

Install Node.js 22 or later, then run:

```sh
cd typescript
npm ci
npm run check
npm run lint
npm test
npm pack
```

## Documentation

Keep the package READMEs focused on users of the installed SDKs. Show package
imports, working examples, defaults, and behavior that affects callers.
Place repository setup and release commands in maintainer documentation.

Public documentation ships with each package release. Check README examples
against the corresponding SDK before publishing. Keep both languages' examples
and option tables consistent while preserving their different API conventions.

Each package README contains installation and quick starts. Detailed API guides
live in `python/docs/` and `typescript/docs/`; examples are grouped by language
and API under `examples/`. Use absolute repository URLs in package READMEs so
guide links work on PyPI and npm. The Python source distribution and npm package
include their guides.

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
