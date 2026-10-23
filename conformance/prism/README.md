# Prism protocol tests

This directory contains shared fixtures for the Python, TypeScript, Rust, and Go realtime
tests. The schema uses AsyncAPI 3.0.0 and describes protocol version 1.0.0.
Prism events and fields use `prism.` and `prism_`. The SDK does not send or
recognize the former `prsm` names.

- `speech_to_speech.asyncapi.yaml` contains message shapes and validation rules.
  It is a test fixture. Descriptive annotations and server connection settings
  are omitted.
  The tool filler rules follow `interfaces/speech_to_speech.asyncapi.yaml` in
  `rimelabs/rime` at commit `e0ce7ac08`, which added per-tool filler controls.
  The filler whitespace pattern spells out Unicode `White_Space` to match
  the engine's Rust validation in both Python and JavaScript. Their `\S`
  patterns use different whitespace sets. SDK tests also reject invalid UTF-8
  before connection setup, as required by the engine's JSON parser.
- `schema.json` checks whether values match the current protocol schema,
  including rejection of requests with the former prefix.
- `protocol.json` checks all four SDK decoders, including malformed messages,
  unknown events, extra fields, and unknown enum values.
- `error-retry.json` checks retry classification for every code in the schema
  fixture and unknown codes. The classification and `context_length_exceeded`
  follow `rimelabs/rime#3356` at commit `61ddc5429`. Only `capacity_exceeded`,
  `unavailable`, and `response_timeout` are retryable. This is SDK metadata,
  derived from the code, not a field sent by Prism. Existing error codes in this
  fixture remain supported even if newer contracts replace them.

The schema describes known protocol values. The decoders tolerate unknown
server events, fields, and enum values for forward compatibility. Separate
decoder tests check that behavior without relaxing client request validation.
A future server value can fail the current schema check and still be accepted
by the SDK decoder. The SDK does not use this fixture to reject incoming events.

Each SDK runs the cases in `schema.json` against the schema fixture. Local test
peers also validate outgoing SDK requests against the message schemas, including
initialization, tools, audio, and playback reports. Rust and Go use package-local
fixture copies; CI checks that these copies match this directory.

All four SDKs report terminal session failures before queued events. Normal
closure ends event consumption without a terminal error. Canceling audio before
socket submission
sends nothing. Canceling an active audio submission closes the session because
the submitted audio can have an unknown outcome; it is never replayed.

Session tests use local WebSocket peers to check request acknowledgments,
readiness, typed input, tool calls, cancellation, and playback reports. Audio
tests use the shared conversion samples in `../pcm-input.json`.

Rust and Go use these request states:

| State | Tracking and cancellation |
| --- | --- |
| Queued | The operation waits for admission. Cancellation sends nothing. |
| Writer handoff | The worker retains request matching before it can process incoming acknowledgments or cancellation. Its internal `submitted` flag is conservative: it also covers this handoff. Only a writer-confirmed skip releases the reservation without a write. |
| Submitted | The writer commits to the write when its final cancellation check succeeds, immediately before transport entry. Reply, control, and tool-result matching remains active after caller cancellation. |
| Settled | The write outcome and required acknowledgment are known. Accepted replies remain tracked until caller delivery or cancellation is observed. |

The handoff is one synchronous worker transition. A writer can start on another
task or goroutine, but the worker cannot process its results halfway through
that transition. An acknowledgment can arrive before write completion; the
worker retains it and cancels an abandoned accepted reply exactly once.
Controlled private writers test cancellation on both sides of submission.

Cancellation flags, tokens, and wake signals do not use the bounded work queues.
Queue saturation tests cover future drop, last-handle drop, and socket shutdown.
Runner tests cover receipt waits concurrent with playback operations, dropped
Rust run futures during playback and stop confirmation, and the cooperative
cleanup contract for application adapters.

For SDK usage and connection settings, see the
[Python realtime guide](../../python/docs/realtime.md) or the
[Node.js realtime guide](../../typescript/docs/realtime.md).

## Deployed request IDs

Local peers verify that the SDK sends `x-request-id` and reports the ID from
Prism's session info. A gateway can replace the header before Prism sees it.
Use the returned ID when you search service logs.

With `RIME_API_KEY` set, run this check from the repository root:

```sh
RIME_PRISM_LIVE=1 uv run --project python pytest -q -s python/tests/test_prism_gateway.py
```

Each of four connections prints the supplied and returned IDs, then closes
without requesting a reply. Set `PRISM_URL` to check another endpoint.
To require preservation, also set `RIME_PRISM_REQUIRE_REQUEST_ID=1`. A failure
then identifies a server or gateway behavior that a local peer cannot test.
