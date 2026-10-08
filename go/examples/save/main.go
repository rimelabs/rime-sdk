// Command save writes TTS output to a WAV or raw mu-law file.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
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
	model := flag.String("model", "coda", "coda or mistv3")
	text := flag.String("text", "Hello. This is the Rime Go SDK.", "text to speak")
	output := flag.String("out", "speech.wav", "output file")
	format := flag.String("format", "pcm", "pcm or mulaw")
	streaming := flag.Bool("stream", false, "supply text in small fragments")
	flag.Parse()
	profile := rime.PCM24000
	if *format == "mulaw" {
		profile = rime.MULAW8000
	} else if *format != "pcm" {
		return fmt.Errorf("format must be pcm or mulaw")
	}
	client, err := rime.NewClient(rime.Config{Model: *model})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var audio *rime.AudioStream
	options := rime.SynthesisOptions{AudioFormat: profile}
	if *streaming {
		runes := []rune(*text)
		audio, err = client.TTS.StreamSource(ctx, func(ctx context.Context) (string, error) {
			if len(runes) == 0 {
				return "", io.EOF
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}
			n := min(7, len(runes))
			fragment := string(runes[:n])
			runes = runes[n:]
			return fragment, nil
		}, options)
	} else {
		audio, err = client.TTS.Stream(ctx, *text, options)
	}
	if err != nil {
		return err
	}
	defer audio.Close()
	file, err := os.Create(*output)
	if err != nil {
		return err
	}
	defer file.Close()
	if profile == rime.PCM24000 {
		if _, err := file.Write(make([]byte, 44)); err != nil {
			return err
		}
	}
	bytes := 0
	for {
		chunk, err := audio.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		n, err := file.Write(chunk)
		if err != nil {
			return err
		}
		bytes += n
	}
	if bytes == 0 {
		return fmt.Errorf("service returned no audio")
	}
	if profile == rime.PCM24000 {
		if uint64(bytes) > uint64(^uint32(0))-36 {
			return fmt.Errorf("audio exceeds WAV size limit")
		}
		header := make([]byte, 44)
		copy(header, "RIFF")
		binary.LittleEndian.PutUint32(header[4:], uint32(bytes+36))
		copy(header[8:], "WAVEfmt ")
		binary.LittleEndian.PutUint32(header[16:], 16)
		binary.LittleEndian.PutUint16(header[20:], 1)
		binary.LittleEndian.PutUint16(header[22:], 1)
		binary.LittleEndian.PutUint32(header[24:], 24000)
		binary.LittleEndian.PutUint32(header[28:], 48000)
		binary.LittleEndian.PutUint16(header[32:], 2)
		binary.LittleEndian.PutUint16(header[34:], 16)
		copy(header[36:], "data")
		binary.LittleEndian.PutUint32(header[40:], uint32(bytes))
		if _, err := file.WriteAt(header, 0); err != nil {
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	fmt.Printf("Saved %d audio bytes to %s; model=%s request_id=%s\n", bytes, *output, strings.ToLower(*model), audio.RequestID())
	return nil
}
