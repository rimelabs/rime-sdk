# Rime SDK

First-party Python and Node.js SDKs for Coda streaming speech.
Both packages accept complete text or an async text source and return audio chunks.
The SDK handles sentence detection, authentication, gRPC, conversion, and cancellation.

| Package | Location | Runtime | Local version |
| --- | --- | --- | --- |
| `rimelabs-sdk` | [python](python/) | Python 3.11+ | 0.1.0a1 |
| `@rimelabs/sdk` | [typescript](typescript/) | Node.js 22+; ESM | 0.1.0-alpha.1 |

## Local setup

```sh
uv sync --project python --locked --dev
npm ci --prefix typescript
uv run --no-project tools/build-native.py --test-support
uv run --project python --no-sync pytest python/tests
npm test --prefix typescript
cargo test --locked -p sdk-core --features test-support
```

Set `RIME_API_KEY` in your application environment. The SDK does not load `.env` files.
See the package READMEs for use and the [examples](examples/) for runnable scripts.

## Repository boundaries

- The Rust core owns SDK behavior; Python and Node own their public adapters.
- Each package keeps its version, changelog, types, and release workflow.
- [Shared cases](conformance/) keep sentence, audio, and error behavior consistent.
- Canonical schemas remain in `rime`. Native builds use the pinned protocol descriptor.
