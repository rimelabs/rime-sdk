package rime

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func rtAcknowledgeUpdate(p *realtimePeer, q rtObject, revision uint64) {
	p.send(rtObject{"type": "session.updated", "prism_request_event_id": q["event_id"],
		"session": rtObject{"id": "session-1", "voice": "astra", "request_id": "trace-update", "prism_response_metadata": rtObject{"step": "billing"}, "prism_effective_interrupt_response": false, "prism_settings_revision": revision}})
}

func TestRealtimePartialUpdateAndClearing(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	original := s.Info()
	instructions := "Handle billing."
	results := make(chan SessionInfo, 1)
	done := make(chan error, 1)
	go func() {
		info, err := s.Update(context.Background(), RealtimeUpdateOptions{Instructions: &instructions,
			Tools: []ToolDefinition{{Name: "refund", Parameters: map[string]any{"type": "object"}, PrismFiller: &PrismFiller{Type: "none"}}}})
		results <- info
		done <- err
	}()
	q := p.next("session.update")
	want := rtObject{"instructions": instructions, "tools": []any{rtObject{"type": "function", "function": rtObject{"name": "refund", "description": "", "parameters": rtObject{"type": "object"}}, "prism_filler": rtObject{"type": "none"}}}}
	if !reflect.DeepEqual(q["session"], want) {
		t.Fatalf("got %v, want %v", q["session"], want)
	}
	if !reflect.DeepEqual(s.Info(), original) {
		t.Fatal("settings changed before acknowledgment")
	}
	rtAcknowledgeUpdate(p, q, 2)
	rtWait(t, done)
	info := <-results
	if info.SettingsRevision == nil || *info.SettingsRevision != 2 || info.Voice != "astra" || info.InterruptOnSpeech {
		t.Fatal(info)
	}
	if info.RequestID == nil || *info.RequestID != "trace-update" || info.ResponseMetadata["step"] != "billing" {
		t.Fatal(info)
	}
	*info.SettingsRevision = 99
	if *s.Info().SettingsRevision != 2 {
		t.Fatal("returned settings alias stored settings")
	}
	empty := ""
	interrupt := false
	go func() {
		_, err := s.Update(context.Background(), RealtimeUpdateOptions{Instructions: &empty, Tools: []ToolDefinition{}, Voice: &empty, InterruptOnSpeech: &interrupt})
		done <- err
	}()
	q = p.next("session.update")
	want = rtObject{"instructions": "", "tools": []any{}, "voice": "", "turn_detection": rtObject{"interrupt_response": false}}
	if !reflect.DeepEqual(q["session"], want) {
		t.Fatal(q)
	}
	rtAcknowledgeUpdate(p, q, 3)
	rtWait(t, done)
	for _, tools := range [][]ToolDefinition{
		{{Name: "same", Parameters: map[string]any{}}, {Name: "same", Parameters: map[string]any{}}},
		{{Name: "bad", Parameters: map[string]any{"n": math.NaN()}}},
	} {
		if _, err := s.Update(context.Background(), RealtimeUpdateOptions{Tools: tools}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	}
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "early-guess", "prism_settings_revision": 2}})
	if started := rtRecv(t, s).Payload.(ResponseStarted); started.SettingsRevision == nil || *started.SettingsRevision != 2 {
		t.Fatal(started)
	}
	if *s.Info().SettingsRevision != 3 {
		t.Fatal(s.Info())
	}

}

func TestRealtimeRejectedConcurrentAndCancelledUpdates(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	missing := "missing"
	go func() { _, err := s.Update(context.Background(), RealtimeUpdateOptions{Voice: &missing}); done <- err }()
	q := p.next("session.update")
	p.send(rtObject{"type": "error", "error": rtObject{"code": "invalid_voice", "message": "unknown voice", "scope": "event", "owner": rtObject{"event_id": q["event_id"]}}})
	if err := <-done; err == nil {
		t.Fatal("accepted invalid voice")
	}
	if s.Info().Voice != "test" {
		t.Fatal(s.Info())
	}
	first := make(chan SessionInfo, 1)
	second := make(chan SessionInfo, 1)
	go func() {
		info, err := s.Update(context.Background(), RealtimeUpdateOptions{Instructions: &missing})
		first <- info
		done <- err
	}()
	q1 := p.next("session.update")
	go func() {
		info, err := s.Update(context.Background(), RealtimeUpdateOptions{Tools: []ToolDefinition{}})
		second <- info
		done <- err
	}()
	q2 := p.next("session.update")
	rtAcknowledgeUpdate(p, rtObject{"event_id": "unrelated"}, 99)
	rtAcknowledgeUpdate(p, q1, 2)
	rtWait(t, done)
	if info := <-first; info.SettingsRevision == nil || *info.SettingsRevision != 2 {
		t.Fatal(info)
	}
	rtAcknowledgeUpdate(p, q2, 3)
	rtWait(t, done)
	if info := <-second; info.SettingsRevision == nil || *info.SettingsRevision != 3 {
		t.Fatal(info)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, err := s.Update(ctx, RealtimeUpdateOptions{Instructions: &missing}); done <- err }()
	q = p.next("session.update")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	rtAcknowledgeUpdate(p, q, 4)
	// The next acknowledgment is a barrier after the cancelled update.
	go func() { _, err := s.AddMessage(context.Background(), "user", "history"); done <- err }()
	message := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": message["event_id"], "item": rtObject{"id": "history", "type": "message"}})
	rtWait(t, done)
	if *s.Info().SettingsRevision != 4 {
		t.Fatal(s.Info())
	}
}

func TestRealtimeUpdatePreservesToolRoundAndReplyRevision(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	parent := p.turn(s, "reply-1")
	rtRecv(t, s)
	rtRecv(t, s)
	p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "reply-1", "item_id": "tool-1", "call_id": "call-1", "name": "lookup", "arguments": "{}"})
	call := rtRecv(t, s).Payload.(ToolCall).Call
	done := make(chan error, 1)
	instructions := "Billing"
	go func() {
		_, err := s.Update(context.Background(), RealtimeUpdateOptions{Instructions: &instructions, Tools: []ToolDefinition{}})
		done <- err
	}()
	rtAcknowledgeUpdate(p, p.next("session.update"), 2)
	rtWait(t, done)
	p.send(rtObject{"type": "response.done", "response": rtObject{"id": "reply-1", "status": "completed", "prism_settings_revision": 1}})
	ended := rtRecv(t, s).Payload.(ResponseEnded)
	if ended.SettingsRevision == nil || *ended.SettingsRevision != 1 || *s.Info().SettingsRevision != 2 {
		t.Fatal(ended, s.Info())
	}
	go func() { done <- s.SubmitToolResult(context.Background(), call, "transferred") }()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": "function_call_output", "call_id": "call-1"}})
	rtWait(t, done)
	go func() { _, err := s.ContinueReply(context.Background(), parent); done <- err }()
	q = p.next("response.create")
	p.send(rtObject{"type": "response.created", "prism_request_event_id": q["event_id"], "response": rtObject{"id": "reply-2", "prism_settings_revision": 2, "metadata": q["response"].(map[string]any)["metadata"]}})
	rtWait(t, done)
	if started := rtRecv(t, s).Payload.(ResponseStarted); started.SettingsRevision == nil || *started.SettingsRevision != 2 {
		t.Fatal(started)
	}
}

func TestRealtimeReplyLabelsKeepSettingsRevision(t *testing.T) {
	p := newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test", "prism_settings_revision": uint64(2), "prism_effective_interrupt_response": false})
	_, s := p.setup(t)
	if err := s.SetResponseMetadata(context.Background(), map[string]string{"step": "billing"}); err != nil {
		t.Fatal(err)
	}
	info := s.Info()
	if info.SettingsRevision == nil || *info.SettingsRevision != 2 || info.InterruptOnSpeech || info.ResponseMetadata["step"] != "billing" {
		t.Fatal(info)
	}
}
