# Shared conformance cases

Both package test runners read these files directly. Changes to this directory
run both language jobs in CI.

- `sentences.json` gives the exact expected sequence, including original whitespace.
- `contract.json` gives chunk sizes, invalid timeouts, audio profiles, gRPC error
  mappings, and required lifecycle scenarios.
- `audio.json` gives exact PCM and mu-law bytes for a deterministic sample signal.

Sentence chunk sizes count Unicode code points. Node.js also tests splits inside
UTF-16 surrogate pairs. The corpus covers abbreviations, numbers, URLs, mixed
scripts, whitespace, emoji, zero-width spaces, and BOM characters. It is an
initial corpus, not full coverage of every supported language.

Lifecycle tests use each language's real gRPC library with a local controllable
service. They test the public API. Private transport and authentication seams point
to the local services; those seams are not public package exports.
