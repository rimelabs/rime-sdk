# Rime SDK for Go

Generate speech with Coda or Mist v3 and transcribe audio with streaming STT
through the `rime.Client`. Your application handles audio capture and playback.

## Installation

Requires **Go 1.24 or later**; cgo is not required. Install the module from the
public `rimelabs/rime-sdk` repository:

```sh
mkdir rime-quickstart
cd rime-quickstart
go mod init example.com/rime-quickstart
go get github.com/rimelabs/rime-sdk/go@latest
export RIME_API_KEY="your-api-key"
```

For an existing module, skip the first three commands. Get an API key from the
[Rime dashboard](https://app.rime.ai/). A key is required:
`rime.NewClient(rime.Config{})` reads `RIME_API_KEY`, or you can set `Config.APIKey`
to override it. The SDK does not load `.env` files. Shell commands below assume
macOS or Linux.

## TTS

Use `client.TTS.Synthesize(ctx, text, options)` for a complete string, or
`client.TTS.Stream(ctx, source, options)` for incremental text. Both return
`(*AudioStream, error)`; read audio with `Recv()` until `io.EOF`. For complete
audio in memory, `client.TTS.SynthesizeToBuffer(ctx, text, options)` returns
`([]byte, error)`. Text is required and must contain non-whitespace characters.
Pass a non-nil `context.Context` to every operation.

### Client options

These optional `rime.Config` fields configure TTS. They do not configure STT.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Model` | Select `"coda"` or `"mistv3"`. | No | `"coda"` when empty. |
| `Endpoint` | Override the TTS TLS hostname and optional port, such as `"coda.api.rime.ai:443"`; no scheme or path. | No | `coda.api.rime.ai:443` for Coda; `mist.api.rime.ai:443` for Mist v3. |
| `Timeout` | Overall TTS timeout as a `time.Duration`, such as `60 * time.Second`; zero disables it. | No | `0` |

### Synthesis options

All three TTS methods accept `rime.SynthesisOptions{}` with these optional fields.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Voice` | Voice name for the selected model, such as `"clementine"` or `"astra"`. | No | `"clementine"` for Coda; `"astra"` for Mist v3. |
| `Language` | Spoken language code, such as `"en"` or `"es"`; the service validates support. | No | `"en"` when empty. |
| `AudioFormat` | Output encoding: `rime.PCM`, `rime.WAV`, `rime.MP3`, `rime.PCM24000`, or `rime.MULAW8000`. | No | Raw PCM16 at 24 kHz. |
| `SampleRate` | Requested output rate in Hz: `8000`, `16000`, or `24000`; zero selects the format's default. | No | `24000`; `8000` for `MULAW8000`. |
| `TimeScaleFactor` | `*float64` duration multiplier from `0.4` to `2.5`; below `1` speeds speech up, above `1` slows it down. | No | `nil`; normal duration (`1`). |
| `Timestamps` | `true` requests word timings; supported only with Mist v3. | No | `false` |
| `CustomLexicon` | `[]rime.PronunciationEntry` with `Spelling` and space-separated X-SAMPA `Pronunciation` strings for Coda. | No | Empty slice. |
| `Timeout` | `*time.Duration` override; a pointer to zero disables the overall timeout. Negative values are invalid. | No | `nil`; inherits the client timeout. |

Discover valid values with `client.Voices.List(ctx, rime.VoiceListOptions{Language: "en"})`
and `client.Languages.List(ctx, rime.DiscoveryOptions{})`. Both return
`([]string, error)`. Omit the voice language filter to list all voices for the
selected model. Both options structs accept `Timeout`; discovery has an
additional ten-second maximum.

### Example: save an IVR greeting

Save as `tts.go`. This writes a complete 16 kHz WAV file, ready for playback or upload.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := rime.NewClient(rime.Config{Timeout: 60 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()

	factor := 0.9
	audio, err := client.TTS.SynthesizeToBuffer(context.Background(),
		"Thank you for calling. Your appointment is confirmed for tomorrow.",
		rime.SynthesisOptions{
			AudioFormat:     rime.WAV,
			SampleRate:      16000,
			TimeScaleFactor: &factor,
		},
	)
	if err != nil {
		return err
	}
	if err := os.WriteFile("greeting.wav", audio, 0600); err != nil {
		return err
	}
	fmt.Println("Saved greeting.wav")
	return nil
}
```

Run `go run tts.go`, then open `greeting.wav` in an audio player.

### Audio and stream behavior

All output is mono. PCM is signed 16-bit little-endian audio without a header;
WAV contains PCM16; MP3 is encoded by the service. PCM/WAV rates are requested
from the server. MP3 headers determine the actual encoded rate. The fixed
`PCM24000` and `MULAW8000` presets reject conflicting sample rates; mu-law is
converted locally from 24 kHz PCM. Read `stream.SampleRate()` for the requested rate.

The buffer helper waits for successful completion and finalizes WAV lengths.
Its fixed limit is **64 MiB per call**, including headers; exceeding it returns
`ErrResourceLimit` and nil audio. WAV/MP3 stream chunks are fragments of one file.
Use the buffer helper when you need a complete WAV.

For incremental text, supply a `rime.TextSource`, a
`func(context.Context) (string, error)` that returns `io.EOF` when finished.
Send fragments as they arrive; the service combines them into sentences.
Consume streaming audio until `io.EOF` to confirm success, since an error can
follow earlier audio. Defer `stream.Close()` and `client.Close()` for cleanup.
A client supports concurrent operations; each stream supports one reader.
Sources must honor cancellation. Synthesis is not retried automatically, and
overall timeouts include pauses by the caller; connection and progress limits
also apply. A caller context can impose an earlier deadline.

For timestamps, enable `Timestamps: true`, consume audio through `io.EOF`, then
call `stream.Timestamps()`. The result has `Status` and `Spans` with word text and
start/end seconds. Nonzero alignment status does not fail the audio. The buffer
helper returns only bytes, so use an audio stream to access timings. Custom
lexicons apply to the whole request; the service validates pronunciations and
model/language support.

## STT

Use `client.STT.Stream(ctx, source, options)` to transcribe one utterance.
The required `rime.AudioSource` is a `func(context.Context) ([]byte, error)`
supplying headerless signed PCM16 little-endian audio. Return `io.EOF` to end
the utterance; bytes returned with `io.EOF` are sent first. Your application
decides when to stop.

STT uses streaming gRPC over TLS to `stt.api.rime.ai:443`, with the client's API
key. There is no STT model, endpoint, or protocol selector. TTS client settings,
including its timeout, do not apply to STT.

### Transcription options

Pass these fields in `rime.TranscriptionOptions`.

| Option | Purpose and values | Required | Default |
| --- | --- | --- | --- |
| `Language` | Spoken BCP-47 language tag, such as `"en"` or `"es"`; passed to the service. | Yes | None; no automatic detection. |
| `Mode` | `rime.TranscriptionWritten` normalizes written text; `rime.TranscriptionVerbatim` prefers spoken wording. | No | `TranscriptionWritten` when empty. |
| `ContextTerms` | `[]string` recognition hints, such as `[]string{"Rime"}`; hints do not force a match. | No | Empty slice. |
| `InputFormat` | `rime.PCMFormat{SampleRate: 16000, Channels: 1}`: rates `8000`, `16000`, `24000`, `48000`; channels `1` or `2`. | No | 16 kHz mono PCM16; zero fields select these defaults. |
| `Timeout` | `*time.Duration` overall budget, including source/consumer pauses; `nil` or a pointer to zero disables it. Negative values are invalid. | No | `nil`; independent of the client timeout. |

### Example: transcribe a recording

Save a short recording as `utterance.pcm`: **16 kHz, mono, signed PCM16
little-endian, with no WAV header**. Save this as `stt.go`.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	file, err := os.Open("utterance.pcm")
	if err != nil {
		return err
	}
	defer file.Close()
	client, err := rime.NewClient(rime.Config{})
	if err != nil {
		return err
	}
	defer client.Close()

	buffer := make([]byte, 3200)
	source := func(ctx context.Context) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := file.Read(buffer)
		return buffer[:n], err
	}
	budget := 120 * time.Second
	transcript, err := client.STT.Stream(context.Background(), source,
		rime.TranscriptionOptions{Language: "en", Timeout: &budget})
	if err != nil {
		return err
	}
	defer transcript.Close()
	for {
		update, err := transcript.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch value := update.(type) {
		case rime.TranscriptionPartial:
			fmt.Println("partial:", value.Text)
		case rime.TranscriptionFinal:
			fmt.Println("final:", value.Text)
		}
	}
}
```

Run `go run stt.go`. The TTS and STT examples are separate programs; run each
by filename. Each partial update replaces the **complete current transcript**;
do not append snapshots. A final update confirms successful completion and may
be empty for silence. Errors do not turn partials into finals.

The SDK converts supported input formats to 16 kHz mono. Stereo must be
interleaved. WAV headers, MP3, and mu-law input are not accepted. Chunks may split
sample frames, but an incomplete final frame fails the request. The SDK does not
provide microphone capture, automatic end-of-speech detection, word timings, or
diarization. Recognition is not retried automatically.

The first `Recv()` starts network work and the overall timeout. Source calls are
serial; returned bytes must remain unchanged until the next call. Cancel the
context or close the stream to stop one utterance. Closing the client cancels
all its operations. Sources must honor their context; the SDK cannot stop a
blocked source function. Connection, acceptance, and completion limits still
apply when the overall timeout is disabled.

## Help and licenses

See the [examples](https://github.com/rimelabs/rime-sdk/tree/main/go/examples)
for streaming text, transcription, and a microphone conversation loop.
Visit [Rime documentation](https://docs.rime.ai/) for service information, or
report SDK issues on [GitHub](https://github.com/rimelabs/rime-sdk/issues).
Use `errors.Is` to check categories such as `rime.ErrTimeout`, and `errors.As`
with `*rime.Error` to read `Message` and `RequestID`. Include the SDK version and
request ID when reporting a failure; omit API keys.

SDK code uses the [MIT license](LICENSE).
