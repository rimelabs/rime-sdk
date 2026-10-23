package rime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func rtSubmissionWorker() (*rtWorker, chan rtWrite) {
	s := &RealtimeSession{op: &operation{ctx: context.Background()}, timeouts: RealtimeTimeouts{Ready: time.Second, Request: time.Second}, events: make(chan SessionEvent, 16)}
	return &rtWorker{s: s, sessionID: "session-1", ready: true, responses: map[string]*rtResponse{}}, make(chan rtWrite, 1)
}
func rtQueueText(w *rtWorker, ctx context.Context) *rtCommand {
	c := &rtCommand{ctx: ctx, kind: "text", body: rtObject{"response": rtObject{"prism_input_text": "hello", "metadata": rtObject{"prism_cause": "user_text"}}}, deadline: time.Now().Add(time.Second), result: make(chan rtResult, 1)}
	c.admissionDeadline = c.deadline
	w.pending = append(w.pending, c)
	return c
}
func rtDispatch(t *testing.T, w *rtWorker, value rtObject) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decodeRealtime(b, w.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.dispatch(e); err != nil {
		t.Fatal(err)
	}
}
func TestRealtimeSubmissionCancellationBeforeWriterEntry(t *testing.T) {
	w, writes := rtSubmissionWorker()
	ctx, cancel := context.WithCancel(context.Background())
	c := rtQueueText(w, ctx)
	if err := w.advance(writes); err != nil {
		t.Fatal(err)
	}
	job := <-writes
	cancel()
	if err := w.advance(writes); err != nil {
		t.Fatal(err)
	}
	if c.finished || w.ready {
		t.Fatal("tracking released before writer confirmed skip")
	}
	sent := 0
	result := rtWriteFrames(context.Background(), job, time.Second, func(context.Context, []byte) error { sent++; return nil })
	if !result.skipped || sent != 0 {
		t.Fatal("cancelled queued request was sent")
	}
	if err := w.written(result); err != nil {
		t.Fatal(err)
	}
	if !c.finished || !w.ready {
		t.Fatal("known skip did not release readiness")
	}
	rtQueueText(w, context.Background())
	if err := w.advance(writes); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writes:
	default:
		t.Fatal("unrelated request did not proceed")
	}
}
func TestRealtimeAcknowledgmentBeforeWriteCompletionCancelsOnce(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		name := "ack-before-cancel"
		if cancelFirst {
			name = "cancel-before-ack"
		}
		t.Run(name, func(t *testing.T) {
			w, writes := rtSubmissionWorker()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := rtQueueText(w, ctx)
			if err := w.advance(writes); err != nil {
				t.Fatal(err)
			}
			job := <-writes
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan rtWritten, 1)
			go func() {
				done <- rtWriteFrames(context.Background(), job, time.Second, func(context.Context, []byte) error { close(entered); <-release; return nil })
			}()
			<-entered
			if cancelFirst {
				cancel()
				if err := w.advance(writes); err != nil {
					t.Fatal(err)
				}
			}
			rtDispatch(t, w, rtObject{"type": "response.created", "prism_request_event_id": c.id, "response": rtObject{"id": "accepted"}})
			if c.ack == nil || c.finished {
				t.Fatal("early acknowledgment was not retained")
			}
			if !cancelFirst {
				cancel()
			}
			if err := w.advance(writes); err != nil {
				t.Fatal(err)
			}
			if !w.responses["accepted"].abandoned {
				t.Fatal("accepted reply was not abandoned")
			}
			close(release)
			if err := w.written(<-done); err != nil {
				t.Fatal(err)
			}
			if err := w.advance(writes); err != nil {
				t.Fatal(err)
			}
			cancellation := <-writes
			if len(cancellation.frames) != 1 {
				t.Fatal("expected one cancellation frame")
			}
			var frame rtObject
			if err := json.Unmarshal(cancellation.frames[0], &frame); err != nil {
				t.Fatal(err)
			}
			if frame["type"] != "response.cancel" || frame["response_id"] != "accepted" {
				t.Fatal(frame)
			}
			if err := w.written(rtWritten{id: cancellation.id}); err != nil {
				t.Fatal(err)
			}
			rtDispatch(t, w, rtObject{"type": "response.done", "response": rtObject{"id": "accepted", "status": "cancelled"}})
			if err := w.advance(writes); err != nil {
				t.Fatal(err)
			}
			select {
			case extra := <-writes:
				t.Fatal("duplicate cancellation", extra.id)
			default:
			}
			abandoned := 0
			for len(w.s.events) > 0 {
				if _, ok := (<-w.s.events).Payload.(ResponseAbandoned); ok {
					abandoned++
				}
			}
			if abandoned != 1 {
				t.Fatalf("abandonment events=%d", abandoned)
			}
		})
	}
}
