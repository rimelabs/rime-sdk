# STT conformance and qualification

`transcripts.json` contains protobuf-JSON response sequences read directly by the
Python and Node.js test suites. The Go and Rust suites read synchronized copies at
`go/testdata/stt/transcripts.json` and `rust/testdata/stt/transcripts.json`;
update all three files when changing these cases.
CI checks that the copies are identical.

Each partial is a replacement snapshot. A final requires source exhaustion,
matching text/revision/language, and successful gRPC completion.
Unknown future payloads are ignored; known messages in an invalid order fail.

Shared PCM vectors are in `../pcm-input.json`. Language-specific audio tests also
cover arbitrary byte splits, truncated frames, independent resampler state, and
bounded conversion/wire payloads. Native local gRPC peers cover acceptance,
concurrent upload/download, status and request-ID propagation, bounded queues,
deadlines, cancellation, and isolation from TTS and Prism. These tests run in the
repository's normal Python, Node, Go, and Rust suites without credentials or audio hardware.

## Live qualification: 2026-10-07 (America/Los_Angeles)

Both public SDK interfaces were exercised against `stt.api.rime.ai:443` using
TLS and the configured API key. Input was synthetic English and Spanish speech
generated through Coda: mono PCM16 at 24 kHz, converted by the SDK to 16 kHz.
Four utterances ran concurrently in each SDK, covering both languages and both
written/verbatim modes with the recognition hint `Rime`. A subsequent English
utterance reused the same client. Separate silence and early-cancellation checks
also passed. Every successful operation produced a final with the expected
language and a request ID; cancellation produced `RimeCancelledError`.

| Measurement | Python | Node.js |
| --- | --- | --- |
| First partial, four concurrent requests | 559–603 ms | 595–650 ms |
| Input EOF to final, four concurrent requests | 599–637 ms | 680–764 ms |
| First partial, repeated English request | 211 ms | 210 ms |
| Input EOF to final, repeated English request | 433 ms | 542 ms |

These are local smoke-test observations, not latency guarantees or a recognition
accuracy benchmark. Inputs were uploaded as fast as backpressure allowed, not
paced as microphone audio. Written/verbatim formatting is model intent; tests
do not require a particular representation of numbers or punctuation.

The schema source used for qualification was canonical `rime-api` commit
`ac3dea5644d5b14c6ceefc9bb2d4b6fa2fd26c5f`. The 0.2.0 release candidate
`fcc1856e300d4c644eb7b7d72db99a4fd6aa7141` has the same definitions; its package
build and all ten upstream package tests passed. Published 0.1.0 artifacts do
not contain STT. These qualification runs used local schema builds; the SDKs
now consume published API packages through their normal dependencies.

## Go live qualification: 2026-10-08 (America/Los_Angeles)

The public Go API passed the same English/Spanish, written/verbatim, recognition
hint, concurrent-operation, client-reuse, silence and cancellation checks over
production TLS. Input used the same synthetic 24 kHz recordings, supplied with
odd 3,201-byte chunk boundaries and converted by the SDK. The run enabled Go's
race detector. All six completed utterances returned a final with the expected
language and a request ID; the cancelled operation returned `ErrCancelled`.
The standalone `examples/transcribe` command also passed against production.

The four concurrent calls delivered their first partial in 441–482 ms and their
final 585–629 ms after source EOF. Reusing the client produced a first partial
in 210 ms and a final 498 ms after EOF. Silence returned an empty final in 86 ms.
These are individual smoke-test observations under the same qualifications above.
That qualification used bundled protocol code generated from canonical schema
commit `ac3dea5644d5b14c6ceefc9bb2d4b6fa2fd26c5f`. The Go SDK now consumes the
versioned `github.com/rimelabs/rime-api/go` dependency for both services.
