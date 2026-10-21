# Speech recognition

## Complete recordings

For a complete recording, use the native unary `Transcribe` RPC through the SDK.
Pass **complete WAV bytes, including the header**, up to **16 MiB (16,777,216
bytes)**. PCM16 mono 16 kHz WAV is the portable format. The SDK sends the file
unchanged; it does not decode, resample, or add a header. The service validates
its encoding and the supplied language and recognition hints.

The result is a `Transcription` containing final text, resolved language, and a
request ID when available. Silence may return empty text. There are no partial
updates or automatic retries. TTS client settings do not apply. An optional
overall timeout includes connection and upload; the SDK also bounds connection
time and the unary request (120 seconds). Timeout, cancellation, and service
errors use the existing SDK error categories. A request ID is included when the
transport has made it available.

```typescript
import { readFile } from "node:fs/promises";
import { Rime } from "@rimelabs/sdk";

const client = new Rime();
try {
  const result = await client.stt.transcribe(await readFile("utterance.wav"), {
    language: "en",
    mode: "written",
    contextTerms: ["Super-G", "USOC"],
    timeout: 120,
  });
  console.log(result.text, result.language, result.requestId);
} finally {
  await client.close();
}
```

`TranscribeOptions` also accepts an `AbortSignal` as `signal`. Aborting it or
closing the client cancels pending requests with `RimeCancelledError`.

## Streaming recognition

`client.stt.stream` transcribes one utterance from an async source of `Uint8Array` chunks (including Node buffers).
The SDK handles the gRPC protocol, authentication, audio conversion, transcript
revisions, backpressure, and cancellation. Your application decides when speech
ends by exhausting the source.

```typescript
import { createReadStream } from "node:fs";
import { Rime } from "@rimelabs/sdk";

async function* audioChunks() {
  yield* createReadStream("utterance.pcm");
}

const client = new Rime();
try {
  const transcript = client.stt.stream(audioChunks(), {
    language: "en",
    inputFormat: { sampleRate: 16000, channels: 1 },
    mode: "written",
    contextTerms: ["Super-G", "USOC"],
    timeout: 120,
  });
  for await (const update of transcript) {
    if (update.kind === "partial") console.log("Current transcript:", update.text);
    else if (update.kind === "final")
      console.log("Final:", update.text, "Language:", update.language);
  }
  console.log("Request:", transcript.requestId);
} finally {
  await client.close();
}
```

The runnable [example](../examples/stt/stream.mjs) reads a PCM file
without loading it all into memory. Use Node.js 22 or later; browsers are not supported.
Open files inside the async generator so file errors are reported through the
SDK and resources are opened only after the service accepts the utterance.

## Append-fragment streaming

Choose `outputContract: "append_fragments"` on
`client.stt.stream`. The default is `"revised_hypotheses"`; `transcribe()` does not
accept an output contract. Both streaming modes use gRPC only.

```javascript
let text = "";
const transcript = client.stt.stream(audioChunks(), {
  language: "en", outputContract: "append_fragments",
});
for await (const update of transcript) {
  if (update.kind === "delta") text += update.text;
  else if (update.kind === "final") console.log(update.text);
}
```

`TranscriptionDelta` is a permanent fragment, not a replacement partial. The
server recognizes independent five-second audio windows; fragments after the
first include newline separators. Do not strip, add separators, or drop deltas.
The final text must match their concatenation. Empty speech can produce no
updates before an empty final. A final is delivered only after `done` and an OK
gRPC status, so a connection failure never promotes accumulated fragments to a
successful result.

Append mode suits longer recordings and consumers that cannot revise text. Send
recordings as fast as backpressure permits. It does not enable unlimited-length
streams: the service's 16 MiB audio and 120-second total-call limits still apply.
Split larger recordings into separate streams. Live voice agents usually prefer
revised hypotheses for earlier provisional text and corrections across windows.

## Options

| Option | Meaning |
| --- | --- |
| `language` | Required spoken BCP-47 tag. Passed unchanged to the service; no detection or SDK language allowlist. The deployed catalog currently supports English and Spanish. |
| `mode` | `"written"` (default) or `"verbatim"`. Formatting intent, not a guarantee of exact tokens. |
| `contextTerms` | Sequence of recognition hints, copied unchanged and in order. The service validates terms and prompt budgets. |
| `outputContract` | `"revised_hypotheses"` (default) or `"append_fragments"`. Streaming only. |
| `inputFormat` | `PCMFormat`: signed little-endian PCM16 at 8, 16, 24 or 48 kHz, mono or interleaved stereo. Defaults to 16 kHz mono. |
| `timeout` | Optional positive overall timeout in seconds. Includes connection time, source pauses and consumer pauses. `null` disables this SDK deadline, but not internal or server deadlines. |

Input contains raw samples, with no WAV header or compressed codec. Chunks may
split sample frames; the SDK carries incomplete bytes into the next chunk. A
truncated final frame raises `RimeAudioFormatError`, without committing the
utterance. Each gRPC audio payload is at most 64 KiB of 16 kHz mono PCM16.

Recognition connects to `stt.api.rime.ai:443` using TLS and the same API key as
the other features. No endpoint configuration is needed.
The existing `model`, `endpoint` and client `timeout` settings remain TTS settings.
There is no STT voice/model selector; the service selects its recognizer.

## Results and completion

`TranscriptionPartial`, `TranscriptionDelta`, and `TranscriptionFinal` are discriminated types exported from `@rimelabs/sdk`. In the default revised-hypotheses mode, every update contains the complete current `text`.
**Replace the previous transcript. Do not concatenate partials.** Later partials
can change earlier words. `TranscriptionFinal.language` is the canonical language
selected by the service.

When upgrading, `TranscriptionUpdate` now includes `TranscriptionDelta`. Update
exhaustive handlers to handle `kind: "delta"`, even if you keep the default
revised-hypotheses contract. Only finals have `language`: check
`update.kind === "final"` before reading it, rather than assuming every
non-partial update is final. Append deltas exactly; replace text with partials
or the confirmed final.

The source is not consumed until the server accepts the configuration. Audio
upload and transcript reading run concurrently. Decoding currently uses cumulative
two-second audio windows, so a partial is not promised for each audio packet.
Silence can finish successfully with empty final text and no partials.

Exhausting the source half-closes gRPC input. The SDK continues reading and emits
one final result only after a matching `done` and successful gRPC completion.
EOF, an error status, or cancellation cannot turn a partial into a final result.
No audio is automatically replayed after a connection failure.

## Lifetime and limits

Construction is lazy: starting iteration begins work. Consume each stream with
one reader. Breaking a `for await` loop calls the iterator's `return` and cancels
unfinished work. `await transcript.cancel()` and async disposal also cancel it.
Pass an `AbortSignal` through the stream's `signal` option to cancel just that
utterance. Client close cancels all STT, TTS and realtime operations; cancelling
one stream leaves its siblings running.

The SDK bounds the transcript queue to 16 updates. In revised-hypotheses mode,
each replacement update is limited to 1 MiB of UTF-8 text; sizes are not summed
across revisions. In append-fragments mode, the concatenated text across all
deltas is limited to 1 MiB. The final transcript is also limited to 1 MiB in both
modes. gRPC responses are bounded to 1 MiB plus 64 KiB for the protobuf envelope.
Slow consumers apply backpressure.
Internal connection and acceptance waits are 10 seconds; completion after source
exhaustion is bounded to 120 seconds. The completion timer stops after successful
gRPC completion. The optional overall timeout remains active until the final
result is consumed. An outstanding source `next()` cannot be forcibly stopped by
JavaScript; the SDK stops waiting for it and attempts `return()` for at most two
seconds. Sources that own devices should support cancellation themselves.

Server input-idle, utterance-size, request-duration and model-context limits still
apply. The current wire limit is 16 MiB per utterance; model context can impose a
smaller limit depending on audio and recognition terms. There is no SDK VAD,
server endpointing, word timing, diarization, or file-format detection.

Failures use the shared SDK error hierarchy: caller/input errors, cancellation,
authentication, permission, resource limits, timeouts, transient availability and
protocol failures stay distinct. `requestId` is retained on streams and failures
when the server supplies it. Keep it when reporting a problem.
