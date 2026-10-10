# Rime SDK

First-party Python, Node.js, and Go SDKs for Coda and Mist v3 streaming speech.
All three accept complete text or an incremental text source and return audio chunks.
The SDK handles sentence detection, API-key authentication, gRPC, conversion, and cancellation.

All three SDKs support streaming speech recognition: `client.stt.stream` in
Python/Node.js and `client.STT.Stream` in Go. Supply a spoken language and a
streaming audio source; updates replace the current
transcript and source exhaustion ends the utterance.

Python and Node.js support Prism speech-to-speech sessions through
`client.realtime.connect`. Prism requires an explicit realtime endpoint.

Coda is the default TTS model. Select `mistv3` to use Mist v3:
`Rime(model="mistv3")` in Python or `new Rime({ model: "mistv3" })` in Node.js.
The SDK selects the model's endpoint and default voice. Both models use the same
streaming and discovery methods. Mist v1 and v2 are not supported.

Mist v3 also supports opt-in word timestamps: pass `timestamps=True` in Python,
`{ timestamps: true }` in Node.js, or `SynthesisOptions{Timestamps: true}` in Go.
After fully consuming audio, use `await stream.timestamps()` (Python/Node.js) or
`stream.Timestamps()` (Go). The final result includes status and word start/end
times in seconds. Timestamps are delivered after generation, not incrementally.

Coda supports request-wide pronunciation overrides through `custom_lexicon`
(Python), `customLexicon` (Node.js), or `CustomLexicon` (Go). Each entry contains
`spelling` and a space-separated X-SAMPA `pronunciation`. Service-rejected entries
raise an input error with the service's explanation and request ID. Malformed
entry types and invalid Unicode fail locally, without a service response or
request ID. See the TTS guides below for examples and supported languages.

| Package | Location | Runtime |
| --- | --- | --- |
| `rimelabs-sdk` | [python](python/) | Python 3.11+ |
| `@rimelabs/sdk` | [typescript](typescript/) | Node.js 22+; ESM |
| `github.com/rimelabs/rime-sdk/go` | [go](go/) | Go 1.24+; TTS and STT |

Use the package READMEs for installation and quick starts. Detailed guides cover
[Python TTS](python/docs/tts.md), [Python STT](python/docs/stt.md), [Python Realtime](python/docs/realtime.md),
[Node.js TTS](typescript/docs/tts.md), [Node.js STT](typescript/docs/stt.md), [Node.js Realtime](typescript/docs/realtime.md), and [Go STT](go/README.md#transcribe-speech).
Runnable scripts are in the
[example index](examples/README.md).

## Local setup

All three SDKs use versioned API packages with STT definitions. The commands
below install those dependencies; no local schema build is required.

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
