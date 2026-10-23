package rime

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"
)

func TestRealtimeTextHistoryAndPlayback(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	r := p.turn(s, "reply-1")
	if r.SessionID() != "session-1" {
		t.Fatal(r)
	}
	if _, ok := rtRecv(t, s).Payload.(InputReady); !ok {
		t.Fatal("missing readiness")
	}
	if _, ok := rtRecv(t, s).Payload.(ResponseStarted); !ok {
		t.Fatal("missing acceptance")
	}
	done := make(chan error, 1)
	go func() {
		item, e := s.AddMessage(context.Background(), "assistant", "history")
		if e == nil && item.ItemID() != "history-1" {
			t.Error(item)
		}
		done <- e
	}()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"id": "history-1", "type": "message"}})
	rtWait(t, done)
	p.send(rtObject{"type": "response.output_item.added", "response_id": "reply-1", "output_index": 0, "item": rtObject{"type": "message", "id": "message-1"}})
	out := rtRecv(t, s).Payload.(MessageStarted).Output
	go func() { done <- s.ReportPlayback(context.Background(), PlaybackInterrupted{out, 12}) }()
	q = p.next("conversation.item.truncate")
	if q["audio_end_ms"] != float64(12) {
		t.Fatal(q)
	}
	p.send(rtObject{"type": "conversation.item.truncated", "item_id": "message-1", "content_index": 0})
	rtWait(t, done)
	p.ended("reply-1")
	_ = rtRecv(t, s)
	go func() { done <- s.ReportPlayback(context.Background(), PlaybackFinished{Response: r}) }()
	q = p.next("prism.playback.drained")
	if q["drain_token"] != "token-reply-1" {
		t.Fatal(q)
	}
	rtWait(t, done)
}
func TestRealtimeAudioAndClear(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { done <- s.SendAudio(context.Background(), AudioChunk{Data: make([]byte, 2560)}) }()
	for i := 0; i < 2; i++ {
		q := p.next("input_audio_buffer.append")
		b, e := base64.StdEncoding.DecodeString(q["audio"].(string))
		if e != nil || len(b) != 1280 {
			t.Fatal(len(b), e)
		}
	}
	rtWait(t, done)
	go func() { done <- s.ClearAudio(context.Background()) }()
	_ = p.next("input_audio_buffer.clear")
	p.send(rtObject{"type": "input_audio_buffer.cleared"})
	rtWait(t, done)
}
func TestRealtimeToolContinuationAndLateAcknowledgment(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	parent := p.turn(s, "reply-1")
	_ = rtRecv(t, s)
	_ = rtRecv(t, s)
	p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "reply-1", "item_id": "tool-item", "call_id": "call-1", "name": "lookup", "arguments": "{}"})
	call := rtRecv(t, s).Payload.(ToolCall).Call
	p.ended("reply-1")
	_ = rtRecv(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.SubmitToolResult(ctx, call, "result") }()
	q := p.next("conversation.item.create")
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	continued := make(chan error, 1)
	go func() { _, e := s.ContinueReply(context.Background(), parent); continued <- e }()
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": "function_call_output", "call_id": "call-1"}})
	q = p.next("response.create")
	p.accept(q, "reply-2")
	rtWait(t, continued)
	if e := s.SubmitToolResult(context.Background(), call, "result"); e != nil {
		t.Fatal(e)
	}
	if e := s.SubmitToolResult(context.Background(), call, "changed"); !errors.Is(e, ErrInput) {
		t.Fatal(e)
	}
}

func TestRealtimeRequestIDAndResponseMetadata(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setupWith(t, RealtimeConnectOptions{RequestID: "client-7", ResponseMetadata: map[string]string{"step": "1"}})
	if p.requestID != "client-7" {
		t.Fatalf("x-request-id %q", p.requestID)
	}
	if got := p.update()["prism_response_metadata"]; !reflect.DeepEqual(got, map[string]any{"step": "1"}) {
		t.Fatalf("initial labels %v", got)
	}
	if info := s.Info(); info.RequestID == nil || *info.RequestID != "client-7" || !reflect.DeepEqual(info.ResponseMetadata, map[string]string{"step": "1"}) {
		t.Fatalf("info %+v", info)
	}
	if e := s.SetResponseMetadata(context.Background(), map[string]string{"step": "2"}); e != nil {
		t.Fatal(e)
	}
	if got := p.update(); !reflect.DeepEqual(got, rtObject{"prism_response_metadata": map[string]any{"step": "2"}}) {
		t.Fatalf("update %v", got)
	}
	if got := s.Info().ResponseMetadata; !reflect.DeepEqual(got, map[string]string{"step": "2"}) {
		t.Fatalf("labels %v", got)
	}
	other := newRealtimePeer(t)
	if _, s = other.setup(t); other.requestID != "" || *s.Info().RequestID != "server-made" {
		t.Fatalf("x-request-id %q, info %+v", other.requestID, s.Info())
	}
}

func TestRealtimeRefusedLabelsFailOnlyTheCall(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setupWith(t, RealtimeConnectOptions{ResponseMetadata: map[string]string{"step": "1"}})
	p.refuse.Store(true)
	var fault *RealtimeFault
	if e := s.SetResponseMetadata(context.Background(), map[string]string{"step": "2"}); !errors.As(e, &fault) || fault.Code != "invalid_session" {
		t.Fatalf("refusal %v", e)
	}
	if got := s.Info().ResponseMetadata; !reflect.DeepEqual(got, map[string]string{"step": "1"}) {
		t.Fatalf("labels %v", got)
	}
	if e := s.SetResponseMetadata(context.Background(), map[string]string{"\xff": "x"}); !errors.Is(e, ErrInput) {
		t.Fatalf("invalid UTF-8 %v", e)
	}
	p.turn(s, "reply-1")
}
