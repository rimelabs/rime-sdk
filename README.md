# Rime SDK

First-party Python and Node.js SDKs for Coda streaming speech.
Both packages accept complete text or an async text source and return audio chunks.
The SDK handles sentence detection, API-key exchange, gRPC, conversion, and cancellation.

| Package | Location | Runtime | Local version |
| --- | --- | --- | --- |
| `rimelabs-sdk` | [python](python/) | Python 3.11+ | 0.1.0a1 |
| `@rimelabs/sdk` | [typescript](typescript/) | Node.js 22+; ESM | 0.1.0-alpha.1 |

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

## License

The SDK is licensed under the [MIT License](LICENSE).
Third-party components retain their own licenses.
