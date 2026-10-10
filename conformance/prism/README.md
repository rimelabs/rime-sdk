# Prism contract

The schema is based on Rime PR #1894, commit
`32d6bd1021f55dc43ed133180faa64ae86bad6a7`, API version 0.4.0 using AsyncAPI 3.0.0.

Source: https://github.com/rimelabs/rime/blob/32d6bd1021f55dc43ed133180faa64ae86bad6a7/interfaces/speech_to_speech.asyncapi.yaml

The SDK snapshot includes one local compatibility adjustment: server response
causes, statuses, error scopes, and owner kinds accept future string values.
This follows the contract's requirement that clients tolerate unknown server
values. The source schema remains unchanged and rejects these unknown values.
Known error owners still require their identity fields, and client request
causes remain strict. The SDK continues to target API version 0.4.0.

Python and TypeScript protocol tests validate outgoing messages against this snapshot. The
SDK uses the readiness, typed-input, tool and playback contracts from this
version. It also sends both `response_id` and `drain_token` in playback receipts,
which the later Rime #2499 implementation accepts.

This file records the test target. It does not prove that a deployed endpoint
implements that target. Run the reference endpoint conformance suite before a
production release. Update the snapshot and tests together if the target changes.

## Implementation audit

Design context: [system overview](https://app.excalidraw.com/s/9Ls6baHJjhi/8zQOvEzhAdG)
and [technical interfaces](https://app.excalidraw.com/s/9Ls6baHJjhi/5zJXYhqtDLN).
The implementations cover Python 3.11 and later and Node.js 22 and later.
TypeScript uses camelCase fields, plain data objects, and `AbortSignal` for
cancellation. Its tests use a local WebSocket server and the same pinned schema.

Audio conversion vectors in `../pcm-input.json` were generated with Python's
`audioop.tomono` and `audioop.ratecv`. Python and Node tests compare Prism and STT
input preparation against these samples, in one chunk and across chunk boundaries.

Shared incoming-message cases in `protocol.json` exercise both decoders. They
check the fields the SDK uses, while preserving unknown events, extra fields,
and unknown enum values. Session tests also verify request-specific acknowledgment
types and tool call identities. Tool-result acknowledgments use `call_id` and
do not require an item ID.

Shared schema cases in `schema.json` check server extensibility and client
validation against the SDK snapshot in both languages.

| Design requirement | Implementation and verification |
| --- | --- |
| One public SDK interface for direct apps and LiveKit | `Rime.realtime.connect`, frozen typed values and events; no LiveKit dependency in the SDK and no wire dictionaries in the plugin. |
| SDK owns protocol state | Readiness, request correlation, tool rounds, PCM conversion, cancellation, timeouts and bounded queues are covered by public SDK tests. |
| Explicit LiveKit intent | `initialize`, `set_input_text` and `playback_finished` distinguish startup, history, typed turns and playback. The fallback adapter forwards these hooks. |
| Correct playback ownership | AgentSession tests cover caller interruption, application stop, skipped output, disabled audio, fatal shutdown and fallback. Generation completion remains separate from playback completion. |
| Recoverable refusals stay local | Tests cover playback/cancel refusals and continuation retry after a confirmed zero-effect refusal. Unknown outcomes do not permit replay. |
| Shared credentials, separate TTS policy | Realtime uses a Bearer API key and a full WebSocket endpoint. Existing TTS behavior remains separate. |

The implementation corrects two wire examples from the original proposal:
tools use the nested `function` object, and session setup omits `tool_choice`,
as required by the pinned schema. `send_audio` waits for transport capacity;
it does not add an unbounded local send queue.

Before release, verify the deployed gateway handshake and endpoint conformance,
run real-room tests, publish the Prism SDK, and set the plugin's minimum SDK
version to that release. Local protocol tests do not prove those release gates.
