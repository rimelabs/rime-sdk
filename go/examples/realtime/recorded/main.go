// Command recorded streams a raw 16 kHz mono PCM16 recording to Prism.
package main

import (
	"context"
	"fmt"
	rime "github.com/rimelabs/rime-sdk/go"
	"io"
	"log"
	"os"
	"time"
)

func send(ctx context.Context, s *rime.RealtimeSession, path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	for {
		data := make([]byte, 1280)
		n, e := io.ReadFull(f, data)
		if n > 0 {
			if err := s.SendAudio(ctx, rime.AudioChunk{Data: data[:n]}); err != nil {
				return err
			}
		}
		if e == io.EOF || e == io.ErrUnexpectedEOF {
			break
		}
		if e != nil {
			return e
		}
		select {
		case <-time.After(40 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 25; i++ {
		if e := s.SendAudio(ctx, rime.AudioChunk{Data: make([]byte, 1280)}); e != nil {
			return e
		}
		select {
		case <-time.After(40 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("supply a raw 16 kHz mono PCM16 file")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, e := rime.NewClient(rime.Config{})
	if e != nil {
		return e
	}
	defer client.Close()
	s, e := client.Realtime.Connect(ctx, rime.RealtimeConnectOptions{Endpoint: os.Getenv("PRISM_URL")})
	if e != nil {
		return e
	}
	sent := make(chan error, 1)
	go func() {
		e := send(ctx, s, os.Args[1])
		sent <- e
		if e != nil {
			_ = s.Close()
		}
	}()
	for {
		event, e := s.Recv(ctx)
		if e != nil {
			select {
			case sendErr := <-sent:
				if sendErr != nil {
					return sendErr
				}
			default:
			}
			return e
		}
		switch p := event.Payload.(type) {
		case rime.TranscriptFinal:
			fmt.Println("You:", p.Text)
		case rime.TextDelta:
			fmt.Print(p.Delta)
		case rime.ResponseEnded:
			if p.Status != "completed" {
				return fmt.Errorf("response %s: %s", p.Status, p.Reason)
			}
			return <-sent
		case rime.FaultEvent:
			if p.Fault.Scope == "event" {
				switch p.Fault.Code {
				case "typed_turn_busy", "instructions_busy", "proactive_unavailable":
					continue
				}
			}
			return p.Fault
		case rime.TranscriptFailed:
			return p.Fault
		}
	}
}
func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
