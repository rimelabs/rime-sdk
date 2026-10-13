# Prism protocol tests

This directory contains shared fixtures for the Python and TypeScript realtime
tests. The schema uses AsyncAPI 3.0.0 and describes protocol version 1.0.0.
Prism events and fields use `prism.` and `prism_`. The SDK does not send or
recognize the former `prsm` names.

- `speech_to_speech.asyncapi.yaml` contains message shapes and validation rules.
  It is a test fixture. Descriptive annotations and server connection settings
  are omitted.
- `schema.json` checks whether values match the current protocol schema,
  including rejection of requests with the former prefix.
- `protocol.json` checks both SDK decoders, including malformed messages,
  unknown events, extra fields, and unknown enum values.

The schema describes known protocol values. The decoders tolerate unknown
server events, fields, and enum values for forward compatibility. Separate
decoder tests check that behavior without relaxing client request validation.
A future server value can fail the current schema check and still be accepted
by the SDK decoder. The SDK does not use this fixture to reject incoming events.

Session tests use local WebSocket peers to check request acknowledgments,
readiness, typed input, tool calls, cancellation, and playback reports. Audio
tests use the shared conversion samples in `../pcm-input.json`.

For SDK usage and connection settings, see the
[Python realtime guide](../../python/docs/realtime.md) or the
[Node.js realtime guide](../../typescript/docs/realtime.md).
