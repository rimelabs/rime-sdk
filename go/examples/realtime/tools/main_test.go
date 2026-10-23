package main

import (
	"errors"
	"strings"
	"testing"

	rime "github.com/rimelabs/rime-sdk/go"
	"github.com/rimelabs/rime-sdk/go/examples/realtime/internal/testpeer"
)

func TestResponseFaultReturnsPromptly(t *testing.T) {
	p := testpeer.New(t)
	done := testpeer.Run(run)
	request := p.Next("response.create")
	p.Send(testpeer.Object{"type": "response.created", "prism_request_event_id": request["event_id"], "response": testpeer.Object{"id": "reply"}})
	p.Send(testpeer.Object{"type": "error", "error": testpeer.Object{"code": "tool_failed", "message": "tool failed", "scope": "response", "response_id": "reply"}})
	var fault *rime.RealtimeFault
	if err := testpeer.Result(t, done); !errors.As(err, &fault) || fault.Code != "tool_failed" {
		t.Fatalf("error = %v, want tool fault", err)
	}
}

func TestUnsuccessfulToolResponseStops(t *testing.T) {
	for _, status := range []string{"failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			p := testpeer.New(t)
			done := testpeer.Run(run)
			request := p.Next("response.create")
			p.Send(testpeer.Object{"type": "response.created", "prism_request_event_id": request["event_id"], "response": testpeer.Object{"id": "reply"}})
			p.Send(testpeer.Object{"type": "response.function_call_arguments.done", "response_id": "reply", "item_id": "tool-item", "call_id": "call", "name": "store_hours", "arguments": "{}"})
			request = p.Next("conversation.item.create")
			p.Send(testpeer.Object{"type": "conversation.item.created", "prism_request_event_id": request["event_id"], "item": testpeer.Object{"type": "function_call_output", "call_id": "call"}})
			p.Send(testpeer.Object{"type": "response.done", "response": testpeer.Object{"id": "reply", "status": status}})
			if err := testpeer.Result(t, done); err == nil || !strings.Contains(err.Error(), status) {
				t.Fatalf("error = %v, want %s response", err, status)
			}
		})
	}
}
