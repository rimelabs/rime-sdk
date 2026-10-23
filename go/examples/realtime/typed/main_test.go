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
	p.Send(testpeer.Object{"type": "error", "error": testpeer.Object{"code": "invalid_request", "message": "request refused", "scope": "event", "owner": testpeer.Object{"kind": "event", "event_id": request["event_id"]}}})
	var fault *rime.RealtimeFault
	if err := testpeer.Result(t, done); !errors.As(err, &fault) || fault.Code != "invalid_request" || fault.RequestID != request["event_id"] {
		t.Fatalf("error = %v, want request fault", err)
	}
}

func TestBusyRequestRetriesAfterReadiness(t *testing.T) {
	for _, code := range []string{"typed_turn_busy", "instructions_busy", "proactive_unavailable"} {
		t.Run(code, func(t *testing.T) {
			p := testpeer.New(t)
			done := testpeer.Run(run)
			request := p.Next("response.create")
			p.Send(testpeer.Object{"type": "error", "error": testpeer.Object{"code": code, "scope": "event", "owner": testpeer.Object{"kind": "event", "event_id": request["event_id"]}}})
			p.Send(testpeer.Object{"type": "prism.typed_input.ready"})
			retry := p.Next("response.create")
			if retry["event_id"] == request["event_id"] {
				t.Fatal("retry reused the refused request ID")
			}
			p.Send(testpeer.Object{"type": "response.created", "prism_request_event_id": retry["event_id"], "response": testpeer.Object{"id": "reply"}})
			p.Send(testpeer.Object{"type": "response.done", "response": testpeer.Object{"id": "reply", "status": "completed"}})
			if err := testpeer.Result(t, done); err != nil {
				t.Fatalf("busy request did not complete after readiness: %v", err)
			}
		})
	}
}

func TestBusyCodeOutsideEventScopeRemainsFatal(t *testing.T) {
	p := testpeer.New(t)
	done := testpeer.Run(run)
	request := p.Next("response.create")
	p.Send(testpeer.Object{"type": "response.created", "prism_request_event_id": request["event_id"], "response": testpeer.Object{"id": "reply"}})
	p.Send(testpeer.Object{"type": "error", "error": testpeer.Object{"code": "typed_turn_busy", "scope": "response", "owner": testpeer.Object{"kind": "response", "response_id": "reply"}}})
	var fault *rime.RealtimeFault
	if err := testpeer.Result(t, done); !errors.As(err, &fault) || fault.Code != "typed_turn_busy" || fault.Scope != "response" {
		t.Fatalf("error = %v, want response fault", err)
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
