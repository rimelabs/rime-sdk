// Command tools executes an application function and continues its Prism reply.
package main

import (
	"context"
	"fmt"
	rime "github.com/rimelabs/rime-sdk/go"
	"log"
	"time"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, e := rime.NewClient(rime.Config{})
	if e != nil {
		return e
	}
	defer c.Close()
	s, e := c.Realtime.Connect(ctx, rime.RealtimeConnectOptions{Tools: []rime.ToolDefinition{{Name: "store_hours", Description: "Return today's store hours", Parameters: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}, PrismFiller: &rime.PrismFiller{Type: "text", Text: "I will check the hours."}}}})
	if e != nil {
		return e
	}
	failures := make(chan error, 128)
	go func() {
		_, e := s.SendText(ctx, "Use store_hours to tell me when the store opens.")
		if e != nil {
			failures <- e
			_ = s.Close()
		}
	}()
	parent := ""
	for {
		event, e := s.Recv(ctx)
		if e != nil {
			select {
			case e = <-failures:
			default:
			}
			return e
		}
		switch p := event.Payload.(type) {
		case rime.ToolCall:
			parent = p.Call.ResponseID()
			go func() {
				if e := s.SubmitToolResult(ctx, p.Call, "The store opens at 09:00 and closes at 17:00."); e != nil {
					failures <- e
					_ = s.Close()
				}
			}()
		case rime.ResponseEnded:
			if p.Response.ResponseID() == parent {
				parent = ""
				go func() {
					if _, e := s.ContinueReply(ctx, p.Response); e != nil {
						failures <- e
						_ = s.Close()
					}
				}()
			} else {
				return nil
			}
		case rime.TextDelta:
			fmt.Print(p.Delta)
		}
	}
}
func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
