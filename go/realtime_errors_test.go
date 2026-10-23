package rime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestRealtimeFaultRetryClassification(t *testing.T) {
	raw, err := os.ReadFile("testdata/prism/error-retry.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Code      string
		Retryable bool
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Code, func(t *testing.T) {
			raw, err := json.Marshal(rtObject{"type": "error", "error": rtObject{"code": c.Code, "retryable": !c.Retryable}})
			if err != nil {
				t.Fatal(err)
			}
			event, err := decodeRealtime(raw, "session-1")
			if err != nil {
				t.Fatal(err)
			}
			fault := event.payload.(FaultEvent).Fault
			if fault.Code != c.Code || fault.Retryable() != c.Retryable {
				t.Fatalf("fault=%+v, retryable=%v, want %v", fault, fault.Retryable(), c.Retryable)
			}
		})
	}
}

func TestRealtimeResponseFaultWithoutReplay(t *testing.T) {
	const overflow = "the prompt is 35401 tokens; with 512 for the reply it is longer than the context of 32768"
	for _, c := range []struct {
		code      string
		retryable bool
	}{
		{"capacity_exceeded", true}, {"unavailable", true}, {"response_timeout", true},
		{"context_length_exceeded", false}, {"future_error", false},
	} {
		t.Run(c.code, func(t *testing.T) {
			p := newRealtimePeer(t)
			_, s := p.setup(t)
			r := p.turn(s, "reply-1")
			message := "reply failed"
			if c.code == "context_length_exceeded" {
				message = overflow
			}
			p.send(rtObject{"type": "error", "error": rtObject{
				"code": c.code, "message": message, "scope": "response",
				"owner":          rtObject{"kind": "response", "response_id": r.ResponseID()},
				"correlation_id": "diagnostic-1",
			}})
			var fault *RealtimeFault
			for fault == nil {
				if payload, ok := rtRecv(t, s).Payload.(FaultEvent); ok {
					fault = payload.Fault
				}
			}
			if fault.Code != c.code || fault.Retryable() != c.retryable || fault.Message != message || fault.Scope != "response" || fault.ResponseID != r.ResponseID() || fault.CorrelationID != "diagnostic-1" {
				t.Fatalf("lost fault details: %+v", fault)
			}
			p.send(rtObject{"type": "response.done", "response": rtObject{"id": r.ResponseID(), "status": "failed", "metadata": rtObject{"prism_drain_token": "drain-1"}}})
			if _, ok := rtRecv(t, s).Payload.(ResponseEnded); !ok {
				t.Fatal("missing response end")
			}
			p.send(rtObject{"type": "prism.typed_input.ready"})
			if _, ok := rtRecv(t, s).Payload.(InputReady); !ok {
				t.Fatal("missing readiness")
			}
			_ = s.Close()
			select {
			case request := <-p.requests:
				t.Fatalf("unexpected replay: %v", request)
			default:
			}
		})
	}
}

func TestRealtimeRequestErrorRetainsFault(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.RequestReply(ctx, ReplyOptions{Instruction: "Say hello"})
		done <- err
	}()
	request := p.next("response.create")
	p.send(rtObject{"type": "error", "error": rtObject{
		"code": "invalid_instruction", "message": "instruction refused", "scope": "event",
		"owner": rtObject{"kind": "event", "event_id": request["event_id"]},
		"param": "response.prism_instruction", "correlation_id": "diagnostic-1",
	}})
	err := <-done
	if !errors.Is(err, ErrRealtime) || errors.Is(err, ErrStream) {
		t.Fatalf("service fault has the wrong kind: %v", err)
	}
	var fault *RealtimeFault
	if !errors.As(err, &fault) {
		t.Fatalf("missing fault: %v", err)
	}
	if fault.Retryable() || fault.Code != "invalid_instruction" || fault.Message != "instruction refused" || fault.RequestID != request["event_id"] || fault.Parameter != "response.prism_instruction" || fault.CorrelationID != "diagnostic-1" {
		t.Fatalf("lost fault details: %+v", fault)
	}
	for {
		if payload, ok := rtRecv(t, s).Payload.(FaultEvent); ok {
			if payload.Fault != fault {
				t.Fatal("request error and event have different faults")
			}
			break
		}
	}
	// A refused request leaves the session usable.
	p.turn(s, "after-refusal")
}

func TestRealtimeSessionFaultIsTerminal(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.AddMessage(ctx, "user", "history")
		done <- err
	}()
	p.next("conversation.item.create")
	p.send(rtObject{"type": "error", "error": rtObject{"scope": "session", "code": "session_expired"}})
	_, received := s.Recv(ctx)
	_, later := s.AddMessage(ctx, "user", "after failure")
	for _, err := range []error{<-done, received, later} {
		var fault *RealtimeFault
		if !errors.Is(err, ErrRealtime) || errors.Is(err, ErrStream) || !errors.As(err, &fault) || fault.Scope != "session" || fault.Code != "session_expired" {
			t.Fatalf("lost terminal service fault: %v", err)
		}
	}
}
