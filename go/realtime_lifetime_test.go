package rime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func rtObserved(t *testing.T, p *realtimePeer, s *RealtimeSession, id string, tool bool) (ResponseRef, ToolCallRef) {
	t.Helper()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": id}})
	ref := rtRecv(t, s).Payload.(ResponseStarted).Response
	var call ToolCallRef
	if tool {
		p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": id, "item_id": "item-" + id, "call_id": "call-" + id, "name": "lookup", "arguments": "{}"})
		call = rtRecv(t, s).Payload.(ToolCall).Call
	}
	p.ended(id)
	if _, ok := rtRecv(t, s).Payload.(ResponseEnded); !ok {
		t.Fatal("missing completion")
	}
	return ref, call
}
func rtAge(t *testing.T, p *realtimePeer, s *RealtimeSession, prefix string) {
	t.Helper()
	for i := 0; i < 140; i++ {
		rtObserved(t, p, s, fmt.Sprintf("%s-%d", prefix, i), false)
	}
}
func rtNoWrites(t *testing.T, p *realtimePeer) {
	t.Helper()
	select {
	case q := <-p.requests:
		t.Fatalf("unexpected write: %v", q)
	case <-time.After(20 * time.Millisecond):
	}
}
func rtHold(t *testing.T, s *RealtimeSession, ref ResponseRef) *ResponseHold {
	t.Helper()
	h, e := s.HoldResponse(context.Background(), ref)
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func TestRealtimeResponseHoldsLifetime(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx := context.Background()
	ref, _ := rtObserved(t, p, s, "held", false)
	a, b := rtHold(t, s, ref), rtHold(t, s, ref)
	ordinary, _ := rtObserved(t, p, s, "ordinary", false)
	a.Release()
	a.Release()
	rtAge(t, p, s, "later")
	for _, invalid := range []ResponseRef{ordinary, {}, {"other", "held"}} {
		if _, err := s.HoldResponse(ctx, invalid); !errors.Is(err, ErrInput) {
			t.Fatalf("invalid hold: %v", err)
		}
	}
	rtNoWrites(t, p)
	p.ended("held")
	p.ended("ordinary")
	p.send(rtObject{"type": "prism.typed_input.ready"})
	if _, ok := rtRecv(t, s).Payload.(InputReady); !ok {
		t.Fatal("duplicate completion")
	}
	if err := s.ReportPlayback(ctx, PlaybackFinished{Response: ref}); err != nil {
		t.Fatal(err)
	}
	if p.next("prism.playback.drained")["response_id"] != "held" {
		t.Fatal("wrong report")
	}
	b.Release()
	b.Release()
	if _, err := s.HoldResponse(ctx, ref); !errors.Is(err, ErrInput) {
		t.Fatalf("last release did not expire: %v", err)
	}
	s.Close()
	b.Release()
}
func TestRealtimeUnresolvedToolLifetime(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx := context.Background()
	ref, call := rtObserved(t, p, s, "tool", true)
	rtAge(t, p, s, "later")
	hold := rtHold(t, s, call.ResponseRef)
	done := make(chan error, 1)
	go func() { done <- s.SubmitToolResult(ctx, call, "done") }()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": "function_call_output", "call_id": call.callID}})
	rtWait(t, done)
	if err := s.SubmitToolResult(ctx, call, "done"); err != nil {
		t.Fatal(err)
	}
	if err := s.SubmitToolResult(ctx, call, "different"); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := s.ContinueReply(ctx, ref); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	rtAge(t, p, s, "after-result")
	rtNoWrites(t, p)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	go func() { _, err := s.RequestReply(ctx, ReplyOptions{ToolCall: &call}); done <- err }()
	q = p.next("response.create")
	p.accept(q, "report")
	rtWait(t, done)
	hold.Release()
	if _, err := s.HoldResponse(ctx, ref); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}
func TestRealtimeHoldProtectedCapacity(t *testing.T) {
	for _, lateTool := range []bool{false, true} {
		t.Run(fmt.Sprint(lateTool), func(t *testing.T) {
			p := newRealtimePeer(t)
			_, s := p.setup(t)
			ctx := context.Background()
			ordinary, _ := rtObserved(t, p, s, "ordinary", false)
			holds := []*ResponseHold{}
			var last ResponseRef
			for i := 0; i < 128; i++ {
				last, _ = rtObserved(t, p, s, fmt.Sprint(i), false)
				holds = append(holds, rtHold(t, s, last))
			}
			if _, err := s.HoldResponse(ctx, ordinary); !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			nested := rtHold(t, s, last)
			nested.Release()
			nested.Release()
			rtNoWrites(t, p)
			if err := s.SetResponseMetadata(ctx, map[string]string{}); err != nil {
				t.Fatal(err)
			}
			if lateTool {
				p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "ordinary", "item_id": "overflow", "call_id": "overflow", "name": "lookup", "arguments": "{}"})
			} else {
				p.send(rtObject{"type": "response.created", "response": rtObject{"id": "overflow"}})
			}
			_, err := s.Recv(ctx)
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			s.Close()
			for _, hold := range holds {
				hold.Release()
				hold.Release()
			}
		})
	}
}

// Keep the real public invocation and worker admission, but gate delivery to
// exercise cancellation after acquisition without relying on scheduler timing.
func TestRealtimeHoldCancelledDeliveryAndFullQueueRelease(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			s := newRealtimeSession(nil, RealtimeTimeouts{Ready: time.Second})
			ref := ResponseRef{"session-1", "held"}
			w := rtWorker{s: s, sessionID: "session-1", responses: map[string]*rtResponse{"held": {ref: ref, done: true}}, order: []string{"held"}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan *ResponseHold, 1)
			failed := make(chan error, 1)
			go func() { h, e := s.HoldResponse(ctx, ref); result <- h; failed <- e }()
			command := <-s.commands
			err := w.acquireHold(command)
			if err != nil {
				t.Fatal(err)
			}
			if cancelled {
				cancel()
			}
			w.finish(command, rtResult{err: err})
			hold := <-result
			err = <-failed
			if cancelled {
				if !errors.Is(err, ErrCancelled) || hold != nil {
					t.Fatalf("cancelled delivery: %v %v", hold, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for len(s.commands) < cap(s.commands) {
					s.commands <- &rtCommand{}
				}
				if _, err := s.HoldResponse(context.Background(), ref); !errors.Is(err, ErrResourceLimit) {
					t.Fatal(err)
				}
				released := make(chan struct{})
				go func() { hold.Release(); hold.Release(); close(released) }()
				select {
				case <-released:
				case <-time.After(time.Second):
					t.Fatal("release blocked on full queue")
				}
			}
			if err := w.pruneResponses(); err != nil {
				t.Fatal(err)
			}
			if w.protectedCount() != 0 || len(w.responses["held"].holds) != 0 {
				t.Fatal("leaked hold")
			}
			close(s.done)
			if hold != nil {
				hold.Release()
			}
		})
	}
}

func TestRealtimePendingReportSurvivesLastExternalHold(t *testing.T) {
	s := newRealtimeSession(nil, RealtimeTimeouts{Ready: time.Second})
	ref := ResponseRef{"session-1", "held"}
	w := rtWorker{s: s, sessionID: "session-1", responses: map[string]*rtResponse{"held": {ref: ref, done: true, token: "drain"}}, order: []string{"held"}}
	acquired := make(chan *ResponseHold, 1)
	go func() {
		h, err := s.HoldResponse(context.Background(), ref)
		if err != nil {
			t.Error(err)
		}
		acquired <- h
	}()
	command := <-s.commands
	w.finish(command, rtResult{err: w.acquireHold(command)})
	hold := <-acquired
	completed := make(chan error, 1)
	go func() { completed <- s.ReportPlayback(context.Background(), PlaybackFinished{Response: ref}) }()
	command = <-s.commands
	if err := w.admitCommand(command); err != nil {
		t.Fatal(err)
	}
	w.pending = append(w.pending, command)
	hold.Release()
	for i := 0; i < 140; i++ {
		id := fmt.Sprint(i)
		w.responses[id] = &rtResponse{ref: ResponseRef{"session-1", id}, done: true}
		w.order = append(w.order, id)
		if err := w.pruneResponses(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.response(ref); err != nil {
		t.Fatal("pending operation lost its response", err)
	}
	writes := make(chan rtWrite, 1)
	if err := w.advance(writes); err != nil {
		t.Fatal(err)
	}
	write := <-writes
	if len(write.frames) != 1 {
		t.Fatal("report was not submitted exactly once")
	}
	if err := w.written(rtWritten{id: write.id}); err != nil {
		t.Fatal(err)
	}
	rtWait(t, completed)
	if err := w.advance(writes); err != nil {
		t.Fatal(err)
	}
	if _, err := w.response(ref); !errors.Is(err, ErrInput) {
		t.Fatal("last protection did not end", err)
	}
	select {
	case <-writes:
		t.Fatal("accepted report replayed")
	default:
	}
	close(s.done)
}

func TestRealtimeActiveGenerationProtectsAfterHoldRelease(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "active"}})
	ref := rtRecv(t, s).Payload.(ResponseStarted).Response
	rtHold(t, s, ref).Release()
	rtAge(t, p, s, "later")
	rtHold(t, s, ref).Release()
	p.ended("active")
	rtRecv(t, s)
	if _, err := s.HoldResponse(context.Background(), ref); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	rtNoWrites(t, p)
}
