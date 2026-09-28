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
