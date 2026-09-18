# Rime SDK

First-party Python and Node.js SDKs for Coda streaming speech.
Both packages accept complete text or an async text source and return audio chunks.
The SDK handles sentence detection, API-key exchange, gRPC, conversion, and cancellation.

| Package | Location | Runtime | Local version |
| --- | --- | --- | --- |
| `rime-sdk` | [python](python/) | Python 3.11+ | 0.1.0a1 |
| `@rimelabs/sdk` | [typescript](typescript/) | Node.js 22+; ESM | 0.1.0-alpha.1 |

These are local alpha implementations. See [validation](docs/validation.md) for
completed tests and checks that require the deployed services.
The private [Themis contract](docs/authentication.md) uses the requested assumption
that the service is complete. Its wire format still needs deployment validation.

## Local setup

```sh
cd python
uv sync --locked --dev
uv run pytest
cd ../typescript
npm ci
npm test
```

Set `RIME_API_KEY` in your application environment. The SDK does not load `.env` files.
See the package READMEs for use and the [examples](examples/) for runnable scripts.

## Repository boundaries

- Each language owns its dependencies, tests, version, changelog, and release workflow.
- [Shared cases](conformance/) keep sentence, audio, and error behavior consistent.
- Canonical schemas remain in their existing repository. These packages consume
  published `rime-api` and `@rimelabs/api` version 0.0.1.
- LiveKit plugin code stays outside this repository. The local
  `../rime-sdk-livekit-poc` uses `python/` as an editable dependency.

Read the [specification and plan](docs/rime-sdk-api-spec.md),
[internal design](docs/internal-design.md), and [release process](docs/releases.md).
