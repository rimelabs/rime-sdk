# Rime SDK

First-party Python, Node.js, and Go SDKs for Coda and Mist v3 streaming speech.
All three accept complete text or an incremental text source and return audio chunks.
The SDK handles sentence detection, API-key authentication, gRPC, conversion, and cancellation.

Python and Node.js support Prism speech-to-speech sessions through
`client.realtime.connect`. Prism requires an explicit realtime endpoint.

Coda is the default TTS model. Select `mistv3` to use Mist v3:
`Rime(model="mistv3")` in Python or `new Rime({ model: "mistv3" })` in Node.js.
The SDK selects the model's endpoint and default voice. Both models use the same
streaming and discovery methods. Mist v1 and v2 are not supported.

| Package | Location | Runtime |
| --- | --- | --- |
| `rimelabs-sdk` | [python](python/) | Python 3.11+ |
| `@rimelabs/sdk` | [typescript](typescript/) | Node.js 22+; ESM |
| `github.com/rimelabs/rime-sdk/go` | [go](go/) | Go 1.24+; TTS only |

Use the package READMEs for installation and quick starts. Detailed guides cover
[Python TTS](python/docs/tts.md), [Python Realtime](python/docs/realtime.md),
[Node.js TTS](typescript/docs/tts.md), and [Node.js Realtime](typescript/docs/realtime.md).
Runnable scripts are in the
[example index](examples/README.md).

## Local setup

```sh
cd python
uv sync --locked --dev
uv run pytest
cd ../typescript
npm ci
npm test
cd ../go
go test -race ./...
```

Set `RIME_API_KEY` in your application environment. The SDK does not load `.env` files.
See the package READMEs for use and the [examples](examples/) for runnable scripts.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the full package checks and build commands.

## Repository boundaries

- Each language owns its dependencies, tests, version, changelog, and release job.
- [Shared cases](conformance/) keep sentence, audio, and error behavior consistent.
- Canonical schemas remain in `rimelabs/rime`. The public `rime-api` repository
  generates and releases the Python, TypeScript, and Go protocol packages.
- Each SDK consumes a versioned API package. Dependabot checks for API releases
  daily and opens dependency PRs. CI and review precede each SDK release.
- Go includes BlingFire WASM and requires no C toolchain. See [Go setup](go/README.md).

## Releases

Release Please prepares version updates and changelogs in a release PR.
Merge that PR to publish the affected packages through GitHub Actions.
See [RELEASING.md](RELEASING.md) for setup, release steps, and recovery.

## License

The SDK is licensed under the [MIT License](LICENSE).
The Python and TypeScript Prism protocol modules use the [Apache 2.0 license](python/LICENSE-PRISM).
Third-party components retain their own licenses.
