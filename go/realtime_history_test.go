package rime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRealtimeHistory(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	items := []HistoryItem{{Type: HistoryMessage, ID: "note", Role: "system", Text: " Transferred. "}, {Type: HistoryFunctionCall, ID: "past", CallID: "old-call", Name: "lookup", Arguments: ` {"n": 1} `}, {Type: HistoryFunctionResult, ID: "result", CallID: "old-call", Output: " raw output "}}
	done := make(chan error, 1)
	var wires []any
	for _, item := range items {
		wire, _ := rtHistoryItem(item)
		wires = append(wires, wire)
		anchor := "root"
		if item.Type == HistoryFunctionResult {
			anchor = ""
		}
		go func() {
			ref, err := s.CreateItem(context.Background(), item, anchor)
			if err == nil && ref.ItemID() != item.ID {
				t.Error(ref)
			}
			done <- err
		}()
		q := p.next("conversation.item.create")
		if !reflect.DeepEqual(q["item"], wire) {
			t.Fatal(q)
		}
		p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": wire})
		rtWait(t, done)
	}
	result := make(chan []HistoryItem, 1)
	go func() { items, err := s.GetHistory(context.Background()); result <- items; done <- err }()
	q := p.next("prism.conversation.get")
	p.send(rtObject{"type": "prism.conversation", "prism_request_event_id": q["event_id"], "items": wires})
	rtWait(t, done)
	if got := <-result; !reflect.DeepEqual(got, items) {
		t.Fatal(got)
	}
	for i := 0; i < 2; i++ {
		go func() { done <- s.DeleteItem(context.Background(), "past") }()
		q = p.next("conversation.item.delete")
		p.send(rtObject{"type": "conversation.item.deleted", "prism_request_event_id": q["event_id"], "item_id": "past"})
		rtWait(t, done)
	}
	go func() { done <- s.DeleteItem(context.Background(), "missing") }()
	q = p.next("conversation.item.delete")
	p.send(rtObject{"type": "error", "error": rtObject{"code": "item_not_found", "scope": "event", "owner": rtObject{"event_id": q["event_id"]}}})
	if err := <-done; !errors.Is(err, ErrRealtime) {
		t.Fatal(err)
	}
}
func TestRealtimeHistoryResultCancellation(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	parent := p.turn(s, "reply-1")
	rtRecv(t, s)
	rtRecv(t, s)
	p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "reply-1", "item_id": "tool", "call_id": "call-1", "name": "lookup", "arguments": "{}"})
	rtRecv(t, s)
	p.ended("reply-1")
	rtRecv(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.CreateItem(ctx, HistoryItem{Type: HistoryFunctionResult, CallID: "call-1", Output: "result"}, "")
		done <- err
	}()
	q := p.next("conversation.item.create")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": "function_call_output", "id": "result", "call_id": "call-1"}})
	go func() { _, err := s.ContinueReply(context.Background(), parent); done <- err }()
	q = p.next("response.create")
	p.accept(q, "reply-2")
	rtWait(t, done)
}
func TestRealtimeInvalidHistory(t *testing.T) {
	for _, item := range []HistoryItem{{Type: HistoryMessage, Role: "bad"}, {Type: HistoryFunctionCall}, {Type: HistoryFunctionResult}, {Type: HistoryMessage, Role: "user", Text: string([]byte{0xff})}} {
		if _, err := rtHistoryItem(item); !errors.Is(err, ErrInput) {
			t.Fatal(item, err)
		}
	}
}

func TestRealtimeHistoryDeletionIdentity(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { done <- s.DeleteItem(context.Background(), "right") }()
	q := p.next("conversation.item.delete")
	p.send(rtObject{"type": "conversation.item.deleted", "prism_request_event_id": q["event_id"], "item_id": "wrong"})
	if err := <-done; !errors.Is(err, ErrStream) {
		t.Fatal(err)
	}
}

func TestRealtimeReusedCallIDKeepsRecordedResponse(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	_ = p.turn(s, "reply-1")
	rtRecv(t, s)
	rtRecv(t, s)
	p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "reply-1", "item_id": "old-call-item", "call_id": "shared", "name": "lookup", "arguments": "{}"})
	call := rtRecv(t, s).Payload.(ToolCall).Call
	p.ended("reply-1")
	rtRecv(t, s)

	done := make(chan error, 1)
	go func() { done <- s.SubmitToolResult(context.Background(), call, "original") }()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": "function_call_output", "call_id": "shared"}})
	rtWait(t, done)

	go func() { done <- s.DeleteItem(context.Background(), "old-call-item") }()
	q = p.next("conversation.item.delete")
	p.send(rtObject{"type": "conversation.item.deleted", "prism_request_event_id": q["event_id"], "item_id": "old-call-item"})
	rtWait(t, done)

	for _, item := range []HistoryItem{
		{Type: HistoryFunctionCall, ID: "imported-call", CallID: "shared", Name: "lookup", Arguments: "{}"},
		{Type: HistoryFunctionResult, ID: "imported-result", CallID: "shared", Output: "imported"},
	} {
		go func() {
			_, err := s.CreateItem(context.Background(), item, "")
			done <- err
		}()
		q = p.next("conversation.item.create")
		p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"type": string(item.Type), "id": item.ID, "call_id": "shared"}})
		rtWait(t, done)
	}

	if err := s.SubmitToolResult(context.Background(), call, "original"); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-p.requests:
		t.Fatalf("cached result sent another request: %v", request)
	default:
	}
}
