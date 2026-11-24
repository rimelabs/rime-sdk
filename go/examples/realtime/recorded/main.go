// Command recorded streams a raw 16 kHz mono PCM16 recording to Prism, then
// silence until the reply ends.
package main

import (
	"context"
	"errors"
	"fmt"
	rime "github.com/rimelabs/rime-sdk/go"
	"log"
	"os"
	"time"
)

// chunkBytes is 40 ms of 16 kHz mono PCM16.
const chunkBytes = 1280

// send streams the recording at capture speed, then silence until ctx ends.
// Prism finds the end of a turn in the audio that it receives, as from a live
// microphone.
func send(ctx context.Context, s *rime.RealtimeSession, recording []byte) error {
	for offset := 0; ; offset += chunkBytes {
		data := make([]byte, chunkBytes)
		if offset < len(recording) {
			copy(data, recording[offset:])
		}
		if e := s.SendAudio(ctx, rime.AudioChunk{Data: data}); e != nil {
			return e
		}
		select {
		case <-time.After(40 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("supply a raw 16 kHz mono PCM16 file")
	}
	recording, e := os.ReadFile(os.Args[1])
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	sending, stopSending := context.WithCancel(ctx)
	defer stopSending()
	sent := make(chan error, 1)
	go func() {
		e := send(sending, s, recording)
		sent <- e
		if !errors.Is(e, context.Canceled) {
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
			stopSending()
			if e := <-sent; !errors.Is(e, context.Canceled) {
				return e
			}
			return nil
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
