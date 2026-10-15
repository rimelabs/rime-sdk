# Shared conformance cases

Python and TypeScript test runners read these files directly. Go and Rust use
copies in `go/testdata/` and `rust/testdata/` so exported packages can run their
tests. CI checks that the copies match. Changes here run all four language jobs.

- `text-input.json` gives source text that the SDK must send unchanged,
  including original whitespace. Rime handles sentence accumulation automatically.
- `contract.json` gives chunk sizes, invalid timeouts, audio profiles, gRPC error
  mappings, and required lifecycle scenarios.
- `audio.json` gives exact PCM and mu-law bytes for a deterministic sample signal.
- [`tts-compat/`](tts-compat/README.md) inventories both LiveKit Rime plugins and
  specifies the proposed cross-language TTS contract. Its T0 checks validate the
  corpus only; they do not claim the new transports are implemented.

Text transport tests vary source chunk boundaries. Node.js also tests splits
inside UTF-16 surrogate pairs. The corpus covers abbreviations, numbers, URLs, mixed
scripts, whitespace, emoji, zero-width spaces, and BOM characters. It is an
initial corpus, not full coverage of every supported language.

Lifecycle tests use each language's real gRPC library with a local controllable
service. They test the public API. Private transport and authentication seams point
to the local services; those seams are not public package exports.
