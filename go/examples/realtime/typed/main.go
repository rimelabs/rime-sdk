// Command typed sends one typed Prism turn. Set RIME_API_KEY.
package main

import (
	"context"
	"fmt"
	rime "github.com/rimelabs/rime-sdk/go"
	"log"
	"os"
	"time"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, e := rime.NewClient(rime.Config{})
	if e != nil {
		return e
	}
	defer client.Close()
	session, e := client.Realtime.Connect(ctx, rime.RealtimeConnectOptions{Endpoint: os.Getenv("PRISM_URL")})
	if e != nil {
		return e
	}
	sent := make(chan error, 1)
	go func() {
		_, e := session.SendText(ctx, "Give me one short greeting.")
		sent <- e
		if e != nil {
			_ = session.Close()
		}
	}()
	for {
		event, e := session.Recv(ctx)
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
		case rime.TextDelta:
			fmt.Print(p.Delta)
		case rime.ResponseEnded:
			if p.Status != "completed" {
				return fmt.Errorf("response %s: %s", p.Status, p.Reason)
			}
			fmt.Println()
			return <-sent
		case rime.FaultEvent:
			if p.Fault.Scope == "event" {
				switch p.Fault.Code {
				case "typed_turn_busy", "instructions_busy", "proactive_unavailable":
					// The session retries after the next readiness event.
					continue
				}
			}
			return p.Fault
		}
	}
}
func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
