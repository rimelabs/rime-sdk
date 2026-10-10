# Rime SDK

First-party Python, Node.js, Go, and Rust SDKs for Coda and Mist v3 streaming speech.
All four use `synthesize` for complete text and `stream` for incremental text input.
Both methods return streaming audio. Go uses `Synthesize` and `Stream`.
The SDK handles sentence detection, API-key authentication, gRPC, conversion, and cancellation.

Python, Node.js, and Go support streaming speech recognition: `client.stt.stream` in
Python/Node.js and `client.STT.Stream` in Go. Supply a spoken language and a
streaming audio source; updates replace the current
transcript and source exhaustion ends the utterance.

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
| `github.com/rimelabs/rime-sdk/go` | [go](go/) | Go 1.24+; TTS and STT |
| `rimelabs-sdk` on crates.io | [rust](rust/) | Rust 1.88+ and Tokio; TTS only |

Use the package READMEs for installation and quick starts. Detailed guides cover
[Python TTS](python/docs/tts.md), [Python STT](python/docs/stt.md), [Python Realtime](python/docs/realtime.md),
[Node.js TTS](typescript/docs/tts.md), [Node.js STT](typescript/docs/stt.md), [Node.js Realtime](typescript/docs/realtime.md), and [Go STT](go/README.md#transcribe-speech).
Runnable scripts are in the
[example index](docs/examples.md).

## TTS methods

| Language | Complete text | Incremental text |
| --- | --- | --- |
| Python | `client.tts.synthesize(text)` | `client.tts.stream(chunks)` |
| TypeScript | `client.tts.synthesize(text)` | `client.tts.stream(chunks)` |
| Go | `client.TTS.Synthesize(ctx, text, opts)` | `client.TTS.Stream(ctx, source, opts)` |
| Rust | `client.tts().synthesize(text, opts)` | `client.tts().stream(chunks, opts)` |

This changes the earlier alpha API. Move complete-string calls from `stream` to
`synthesize`. In Go, rename `StreamSource` to `Stream`. In Rust, move synthesis
under `client.tts()` and rename `synthesize_stream` to `stream`. The old entry
points have no compatibility aliases. STT and realtime methods retain their names.

## Local setup

All three SDKs use versioned API packages with STT definitions. The commands
below install those dependencies; no local schema build is required.

```sh
cd python
uv sync --locked --dev
uv run pytest
cd ..
npm ci
cd typescript
npm test
cd ../go
go test -race ./...
cd ../rust
cargo test --locked
```

Set `RIME_API_KEY` in your application environment. The SDK does not load `.env` files.
See the package READMEs for use and the [examples](docs/examples.md) for runnable scripts.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the full package checks and build commands.

## Repository boundaries

- Each language owns its dependencies, tests, version, changelog, and release job.
- [Shared cases](conformance/) keep sentence, audio, and error behavior consistent.
- Canonical schemas remain in `rimelabs/rime`. The public `rime-api` repository
  generates and releases the Python, TypeScript, Go, and Rust protocol packages.
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
