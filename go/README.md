# Rime SDK for Go

Use `client.TTS.Synthesize` for complete text and `client.TTS.Stream` for an
incremental text source. Both return streaming Coda and Mist v3 audio.
The SDK handles authentication, sentence detection, gRPC, audio conversion,
deadlines, and cancellation. It also streams speech recognition from raw PCM audio
to partial and final transcripts. Your application owns audio capture and playback.

Go 1.24 or later is required. The SDK does not require cgo.

Install a published version from the public `rimelabs/rime-sdk` repository:

```sh
go get github.com/rimelabs/rime-sdk/go@latest
```

The module is in the repository's `go/` directory. Release tags use `go/v…`.
To select an exact release, pass its version without the directory prefix, for
example `go get github.com/rimelabs/rime-sdk/go@v0.1.0-alpha.2`.
The repository must be public and the release tag must exist before installation.
See [the release guide](../RELEASING.md).

Set `RIME_API_KEY` in the application environment. The SDK does not load `.env` files.

## Save speech

```go
package main

import (
    "context"
    "io"
    "log"
    "os"

    rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
    if err := save(); err != nil { log.Fatal(err) }
}

func save() error {
    client, err := rime.NewClient(rime.Config{})
    if err != nil { return err }
    defer client.Close()
    stream, err := client.TTS.Synthesize(context.Background(), "Hello. This is Rime.", rime.SynthesisOptions{})
    if err != nil { return err }
    defer stream.Close()
    file, err := os.Create("speech.pcm")
    if err != nil { return err }
    defer file.Close()
    for {
        chunk, err := stream.Recv()
        if err == io.EOF { return file.Close() }
        if err != nil { return err }
        if _, err := file.Write(chunk); err != nil { return err }
    }
}
```

`speech.pcm` contains raw 16-bit little-endian mono PCM at 24 kHz. It has no WAV
header. `examples/save` in this module writes a WAV file and supports streaming
text input.

## Transcribe speech

`client.STT.Stream(ctx, source, options)` transcribes one caller-ended utterance.
The caller supplies raw audio and decides when the utterance ends. The SDK handles
the gRPC connection, authentication, input conversion and transcript validation.
It does not capture microphone audio, detect end of speech, or retry recognition.

### Read transcripts

```go
stream, err := client.STT.Stream(ctx, source, rime.TranscriptionOptions{
    Language: "en",
    Mode: rime.TranscriptionWritten,
    ContextTerms: []string{"Rime"},
    InputFormat: rime.PCMFormat{SampleRate: 24000, Channels: 1},
})
if err != nil { return err }
defer stream.Close()
for {
    update, err := stream.Recv()
    if err == io.EOF { break }
    if err != nil { return err }
    switch value := update.(type) {
    case rime.TranscriptionPartial:
        fmt.Println("partial:", value.Text)
    case rime.TranscriptionFinal:
        fmt.Println("final:", value.Text, "language:", value.Language)
    }
}
```

Every partial is the **complete current transcript**. Replace the previous text;
do not append snapshots. A final is emitted exactly once, after input exhaustion,
a consistent final service response, and successful gRPC termination. Silence
may produce an empty final. Failed operations never produce a final; partial text
already delivered remains provisional. After the final, `Recv` returns `io.EOF`.

Run the file example from the Go module directory:

```sh
go run ./examples/transcribe -language en -sample-rate 24000 audio.pcm
```

Use a raw PCM file with no WAV header. The example opens the file only after the
service accepts the request, prints snapshots and the request ID, and handles
Ctrl+C. Set `RIME_API_KEY` in your environment first.

For live microphone testing, `examples/voice` provides an Enter-to-talk terminal
loop and speaks the final transcript through TTS. Install SoX, then run
`go run ./examples/voice --language en`. Press Enter to start, Enter to finish,
and Ctrl+C to cancel. `--mode`, repeatable `--term` and `--voice` select recognition
and playback options. To avoid opening audio devices, use
`--input recording.wav --output reply.wav` for one recorded turn.
Microphone mode supports macOS and Linux. On Windows, use `--input`; microphone
mode is rejected because the recorder cannot be stopped with a POSIX interrupt.

### Supply audio

An `AudioSource` is `func(context.Context) ([]byte, error)`. Calls are serial and
start only after service acceptance. Return `io.EOF` to finish the utterance;
bytes returned alongside `io.EOF` are sent first. A different error cancels the
operation without committing input. The source owns its file or device and must
honor its context. Returned bytes must remain unchanged until the next call.

Input is headerless signed PCM16 little-endian, at 8, 16, 24 or 48 kHz, with one
or two channels. Stereo samples are interleaved. The zero `PCMFormat` means mono
16 kHz. The SDK downmixes and resamples to mono 16 kHz using streaming linear
interpolation. State is independent for each utterance. This lightweight converter
does not provide anti-alias filtering; applications needing that control can
supply prepared 16 kHz mono PCM.

Source chunks can split frames at any byte boundary. The SDK keeps incomplete
frames until the next chunk and rejects an incomplete frame at EOF with
`ErrAudioFormat`. Conversion processes at most 16 KiB of source bytes at a time,
and wire messages contain at most 64 KiB of complete PCM frames. Backpressure
slows source reads. Empty chunks are allowed but do not finish the utterance.

### Options and routing

| Field | Default | Meaning |
| --- | --- | --- |
| `Language` | None | Explicit spoken language, such as `en` or `es`; validated by the service |
| `Mode` | `TranscriptionWritten` | Written normalization or `TranscriptionVerbatim` for spoken wording |
| `ContextTerms` | None | Recognition hints, copied in order without trimming or deduplication |
| `InputFormat` | Mono PCM16, 16 kHz | Sample rate and channel count of supplied raw audio |
| `Timeout` | Disabled | Optional `*time.Duration` overall budget from the first `Recv` |

Language and terms pass unchanged to the service. Neither is inferred from TTS
configuration. Written/verbatim formatting depends on the model; a hint does not
guarantee a word will appear in the transcript.

`Config.STTEndpoint` selects the STT TLS hostname and optional port; see
[Configuration](#configuration) for the default and accepted format.
`Config.Model`, `Endpoint`, and `Timeout` configure TTS and do not change STT.
Both services share credentials
and error categories but use separate connections. STT sends the API key as a
Bearer authorization header over TLS. Protocol types remain internal.

### Lifetime, limits and errors

Construction validates local option types and values. The first `Recv` starts
network work and the optional overall timeout. A nil or zero timeout disables
that budget; negative durations are invalid. The caller's context applies
immediately, including before the first read and while either source or consumer
is paused.

Connection and acceptance each have a 10-second limit. Once source EOF is reached,
completion has a 120-second limit. The completion timer stops after successful
gRPC completion. The optional overall timeout remains active until the final
result is consumed. Transcript snapshots are limited to 64 KiB of UTF-8 text,
incoming gRPC messages to 256 KiB, and the output queue to 16 snapshots.
The queue preserves entire updates. Slow consumers apply backpressure.

An audio source error becomes `ErrInput`, preserving the original error as its
cause. Invalid server sequencing becomes `ErrStream`; resource limits become
`ErrResourceLimit`. Service errors preserve their category and the request ID
when available. Read it with `stream.RequestID()` or `errors.As` to `*rime.Error`.
Recognition is never automatically replayed, even before the first partial.

Cancelling recognition discards unread updates, including an unread final.
See [Errors and cleanup](#errors-and-cleanup) for shared error handling and
stream lifetime rules.

## Configuration

| Config field | Default | Meaning |
| --- | --- | --- |
| `APIKey` | `RIME_API_KEY` | An explicit nonempty key overrides the environment |
| `Model` | `coda` | `coda` or `mistv3` |
| `Endpoint` | Model endpoint | TLS hostname with optional port; no scheme or path |
| `Timeout` | `0` | Overall TTS operation duration; zero disables it |
| `STTEndpoint` | `stt.api.rime.ai:443` | Independent STT TLS hostname with optional port; no scheme or path |

Coda uses `coda.api.rime.ai:443` and voice `clementine`. Mist v3 uses
`mist.api.rime.ai:443` and voice `astra`. Both use language `en` by default.

`SynthesisOptions` accepts `Voice`, `Language`, `AudioFormat`, and `Timeout`.
Empty voice and language values select defaults. Audio formats are `PCM24000`
and `MULAW8000`. Both are raw mono audio. `MULAW8000` contains G.711 mu-law at
8 kHz. The SDK converts the service's PCM locally. Chunks contain complete frames.
Use `stream.Format().Encoding()`, `SampleRate()`, and `Channels()` for metadata.

Each operation accepts a `context.Context`. Its cancellation and deadline remain
active even while the application pauses reads. `Timeout` in operation options
for TTS is a `*time.Duration`: nil inherits the client setting, a pointer to zero disables
that setting, and a positive value overrides it. A caller's context deadline
still applies. Negative durations are invalid.

Connection establishment has a 10-second limit. First audio has a 30-second
progress limit. Later progress has a 60-second limit. Waiting for text input or
for the application to consume queued output does not count as a service stall.
Discovery takes at most 10 seconds, including retries.

## Incremental input

Use `client.TTS.Stream(ctx, source, options)`. A `rime.TextSource` is:

```go
func(ctx context.Context) (string, error)
```

Return each text fragment as valid UTF-8. The SDK calls the source serially and
detects complete sentences. Return `io.EOF` when input ends. The last call may
return both final text and `io.EOF`. Other errors stop the operation.

The source must honor its context; see [Errors and cleanup](#errors-and-cleanup)
for cancellation and cleanup limits.

TTS streams start work when `Synthesize` or `Stream` returns, without waiting for
the first `Recv`. Output buffering is bounded to 96,000 bytes, with chunks of
at most 9,600 bytes. Each sentence has a 65,536-byte UTF-8 limit.

## Discovery

```go
voices, err := client.Voices.List(ctx, rime.VoiceListOptions{Language: "en"})
languages, err := client.Languages.List(ctx, rime.DiscoveryOptions{})
```

Both methods return `[]string`. Omit the voice language to disable filtering.
Discovery retries `UNAVAILABLE` up to twice within the same deadline.

## Errors and cleanup

Use `errors.Is(err, rime.ErrTimeout)` to check a category. Categories are
`ErrAuthentication`, `ErrPermission`, `ErrInput`, `ErrResourceLimit`,
`ErrUnavailable`, `ErrTimeout`, `ErrAudioFormat`, `ErrCancelled`, and `ErrStream`.
Use `errors.As` with `*rime.Error` to read `RequestID` and the original `Cause`.
Caller context errors remain available through error unwrapping.

Partial audio can precede an error. Output is complete only after `Recv` returns
`io.EOF`. The SDK does not retry synthesis. Buffered audio is discarded on failure.

Call `stream.Close()` when stopping early. It cancels one operation.
`client.Close()` cancels all client operations and closes both service connections. Both
are safe to repeat. Clients support concurrent operations. Each stream permits
one concurrent reader. Always release both the stream and the client.

Stream cleanup waits at most two seconds. The SDK cannot force an application
source function to return. A source that ignores its context can retain its
goroutine and memory after `Close` returns.

## Development

To use a local checkout from another Go project before publication:

```sh
go mod edit -require=github.com/rimelabs/rime-sdk/go@v0.1.0-alpha.1
go mod edit -replace=github.com/rimelabs/rime-sdk/go=/absolute/path/to/rime-sdk/go
go mod tidy
```

Run these checks from the SDK's `go/` directory:


```sh
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build ./...
```

Tests use a local gRPC service and need no credentials. `testdata/` contains copies
of the repository's shared TTS, PCM input and STT fixtures. CI verifies that those copies match.
The versioned `github.com/rimelabs/rime-api/go` dependency supplies the protocol
types and gRPC clients. Dependabot checks for API releases daily and opens a PR;
CI and review are required before the SDK is released.

The embedded BlingFire binary includes its source
details and licenses in `internal/`.
