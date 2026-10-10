// Command transcribe prints replacement transcript snapshots for a raw PCM file.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	language := flag.String("language", "", "spoken language, such as en or es (required)")
	mode := flag.String("mode", "written", "written or verbatim")
	sampleRate := flag.Int("sample-rate", 16000, "input frames per second: 8000, 16000, 24000 or 48000")
	channels := flag.Int("channels", 1, "input channels: one or two")
	flag.Parse()
	if flag.NArg() != 1 || *language == "" {
		return fmt.Errorf("usage: transcribe -language en [-sample-rate 16000] [-channels 1] [-mode written|verbatim] audio.pcm")
	}
	client, err := rime.NewClient(rime.Config{})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	var file *os.File
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	buffer := make([]byte, 3200)
	source := func(ctx context.Context) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if file == nil {
			var err error
			file, err = os.Open(flag.Arg(0))
			if err != nil {
				return nil, err
			}
			info, err := file.Stat()
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("input must be a regular PCM file")
			}
		}
		count, err := file.Read(buffer)
		return buffer[:count], err
	}
	budget := 120 * time.Second
	stream, err := client.STT.Stream(ctx, source, rime.TranscriptionOptions{
		Language: *language, Mode: rime.TranscriptionMode(*mode),
		InputFormat: rime.PCMFormat{SampleRate: *sampleRate, Channels: *channels}, Timeout: &budget,
	})
	if err != nil {
		return err
	}
	defer stream.Close()
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch value := update.(type) {
		case rime.TranscriptionPartial:
			fmt.Printf("partial: %s\n", value.Text)
		case rime.TranscriptionFinal:
			fmt.Printf("final [%s]: %s\n", value.Language, value.Text)
		}
	}
	fmt.Printf("request_id: %s\n", stream.RequestID())
	return nil
}
