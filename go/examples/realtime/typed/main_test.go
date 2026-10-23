package main

import (
	"errors"
	"strings"
	"testing"

	rime "github.com/rimelabs/rime-sdk/go"
	"github.com/rimelabs/rime-sdk/go/examples/realtime/internal/testpeer"
)

func TestRequestFaultReturnsPromptly(t *testing.T) {
	p := testpeer.New(t)
	done := testpeer.Run(run)
	request := p.Next("response.create")
	p.Send(testpeer.Object{"type": "error", "error": testpeer.Object{"code": "busy", "message": "try later", "scope": "request", "event_id": request["event_id"]}})
	var fault *rime.RealtimeFault
	if err := testpeer.Result(t, done); !errors.As(err, &fault) || fault.Code != "busy" {
		t.Fatalf("error = %v, want busy fault", err)
	}
}

func TestFailedResponseReturnsError(t *testing.T) {
	p := testpeer.New(t)
	done := testpeer.Run(run)
	request := p.Next("response.create")
	p.Send(testpeer.Object{"type": "response.created", "prism_request_event_id": request["event_id"], "response": testpeer.Object{"id": "reply"}})
	p.Send(testpeer.Object{"type": "response.done", "response": testpeer.Object{"id": "reply", "status": "failed"}})
	if err := testpeer.Result(t, done); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("error = %v, want failed response", err)
	}
}
