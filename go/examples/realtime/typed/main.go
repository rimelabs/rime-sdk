// Command typed sends one typed Prism turn. Set RIME_API_KEY.
package main

import (
	"context"
	"fmt"
	rime "github.com/rimelabs/rime-sdk/go"
	"log"
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
	session, e := client.Realtime.Connect(ctx, rime.RealtimeConnectOptions{})
	if e != nil {
		return e
	}
	sent := make(chan error, 1)
	go func() { _, e := session.SendText(ctx, "Give me one short greeting."); sent <- e }()
	for {
		event, e := session.Recv(ctx)
		if e != nil {
			return e
		}
		switch p := event.Payload.(type) {
		case rime.TextDelta:
			fmt.Print(p.Delta)
		case rime.ResponseEnded:
			fmt.Println()
			return <-sent
		}
	}
}
func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
