# Rime SDK for Go

Stream Coda and Mist v3 speech from complete text or an incremental text source.
The SDK handles authentication, sentence detection, gRPC, audio conversion,
deadlines, and cancellation. Your application owns playback.

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
    stream, err := client.TTS.Stream(context.Background(), "Hello. This is Rime.", rime.SynthesisOptions{})
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

## Configuration

| Config field | Default | Meaning |
| --- | --- | --- |
| `APIKey` | `RIME_API_KEY` | An explicit nonempty key overrides the environment |
| `Model` | `coda` | `coda` or `mistv3` |
| `Endpoint` | Model endpoint | TLS hostname with optional port; no scheme or path |
| `Timeout` | `0` | Overall operation duration; zero disables it |

Coda uses `coda.api.rime.ai:443` and voice `clementine`. Mist v3 uses
`mist.api.rime.ai:443` and voice `astra`. Both use language `en` by default.

`SynthesisOptions` accepts `Voice`, `Language`, `AudioFormat`, and `Timeout`.
Empty voice and language values select defaults. Audio formats are `PCM24000`
and `MULAW8000`. Both are raw mono audio. `MULAW8000` contains G.711 mu-law at
8 kHz. The SDK converts the service's PCM locally. Chunks contain complete frames.
Use `stream.Format().Encoding()`, `SampleRate()`, and `Channels()` for metadata.

Each operation accepts a `context.Context`. Its cancellation and deadline remain
active even while the application pauses reads. `Timeout` in operation options
is a `*time.Duration`: nil inherits the client setting, a pointer to zero disables
that setting, and a positive value overrides it. A caller's context deadline
still applies. Negative durations are invalid.

Connection establishment has a 10-second limit. First audio has a 30-second
progress limit. Later progress has a 60-second limit. Waiting for text input or
for the application to consume queued output does not count as a service stall.
Discovery takes at most 10 seconds, including retries.

## Incremental input

Use `client.TTS.StreamSource(ctx, source, options)`. A `rime.TextSource` is:

```go
func(ctx context.Context) (string, error)
```

Return each text fragment as valid UTF-8. The SDK calls the source serially and
detects complete sentences. Return `io.EOF` when input ends. The last call may
return both final text and `io.EOF`. Other errors stop the operation.

The source must honor its context. The SDK cannot force an application function
to return. A source that ignores cancellation can retain its goroutine and memory
after `Close` returns. Stream cleanup waits at most two seconds.

Streams start work when `Stream` or `StreamSource` returns, without waiting for
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
`client.Close()` cancels all client operations and closes the connection. Both
are safe to repeat. Clients support concurrent operations. Each stream permits
one concurrent reader. Always release both the stream and the client.

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
of the repository's shared TTS fixtures. CI verifies that those copies match.
Generated protocol code and the embedded BlingFire binary include their source
details and licenses in `internal/`.
