# Local validation

Checked on 2026-09-17 on macOS arm64. Python was 3.12.13; Node.js was 24.18.0.

| Check | Result |
| --- | --- |
| Python source tests | 167 passed |
| Python wheel tests in a clean environment, before review fixes | 110 passed |
| Node.js source tests | 133 passed |
| Installed npm archive tests in a clean directory, before review fixes | 86 passed |
| LiveKit adapter tests | 13 passed |
| Python Ruff lint, formatting, and mypy | Passed |
| Node.js TypeScript build and Prettier | Passed |
| Python wheel and source archive | Built |
| npm package archive and public import | Passed |
| Installed LiveKit patch checksum | Passed |
| Agent CLI loading | Passed |

Both language suites read the same sentence, audio, profile, and error fixtures.
They use local gRPC services with canonical protocol messages. Tests cover early
audio, concurrent input and output, final-status errors, no replay, cancellation,
shared token refresh, deadline inheritance, caller pauses, queue bounds, cleanup,
and incomplete final samples. API-key exchange tests use local HTTP responses.
Review regression tests cover server rejection during Python writes, HTTP 429
and 5xx error types, and cancellation of unfinished Node.js HTTP error bodies.
A separate 100-request rejection check produced no text-source errors or replay.
Further regression tests cover cancellation after Node.js queue reads, failures
without audio metadata, empty responses with only trailers, request IDs in either
metadata location, and release of Python tokens after client shutdown.

The sentence corpus includes abbreviations, URLs, decimals, mixed scripts,
whitespace, emoji, zero-width spaces, BOM characters, and multiple chunk sizes.
The Node.js tests also split UTF-16 surrogate pairs. Shared audio tests compare
exact mu-law bytes across both
implementations and several input chunk sizes. Separate Python checks measure
passband output and high-frequency attenuation.

Sentence-buffer regression tests cover both reported `Dr.` examples, every
two-part split in the shared corpus, empty chunks, final calls with text,
retained-context rotation, bounded storage, and delivery at the lookahead
threshold. Local RPC tests compare the actual sentence messages for complete
strings and incremental sources. A scan-count test checks that a 65,536-byte
sentence arriving one character at a time does not scan once per input chunk.
A separate seeded check generated 200 mixed-language inputs. Python output was
unchanged by irregular chunks, and Node.js matched it with three chunk patterns.
The fixtures retain the pinned detector's known split after `Dr.` in these
examples; consistency does not establish linguistic accuracy.

Both package archives were rebuilt after these changes. Tests against installed
archives and the adapter checks listed above were not repeated in this pass.

The Python test decoder uses standard `audioop` on Python 3.12, which produces
one deprecation warning. Python 3.13 uses the test-only `audioop-lts` dependency.
The SDK runtime does not use either decoder.

## Checks not completed locally

- Real API-key exchange and authenticated Coda synthesis. The implementation uses
  the requested [Themis assumption](authentication.md); it sends no real credentials
  during these local tests.
- Interactive microphone playback and interruption with the new SDK.
- Every service language, native/WASM platform combination, and CI runtime.
- Server capacity release after cancellation, active-RPC token expiry, long-input
  rollover, production load, and listening tests of converted real service audio.
- GitHub workflow execution, registry publication, and upstream LiveKit plugin delivery.

The existing POC audio artifacts came from its old unauthenticated implementation.
They do not establish results for the new SDK.
