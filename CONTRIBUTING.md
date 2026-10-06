# Development

These instructions require access to the private source repository.
Start each command block from the repository root.

## Python

Use Python 3.11 or later and `uv`:

```sh
cd python
uv sync --locked --dev
uv run ruff check src tests
uv run ruff format --check src tests
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

See [RELEASING.md](RELEASING.md) for release PRs, registry setup, and recovery.

## Prism and the local LiveKit plugin

The Python realtime module owns the Prism protocol. The LiveKit Rime plugin owns
LiveKit history, generation streams, and playback reports. Keep raw wire events
out of the plugin. Test the public SDK with a controlled peer; the pinned schema
is in `conformance/prism/`.

To test both checkouts before the SDK release, run from the `agents` checkout:

```sh
uv pip install --python .venv/bin/python --editable ../rime-sdk/python
uv run --no-sync pytest tests/test_plugin_rime_realtime.py -q --unit
```

`--no-sync` keeps the local SDK installation for this run. No local filesystem
path belongs in a published dependency. Publish the Prism-enabled SDK and set the
plugin's minimum SDK version to that release before publishing the plugin.
