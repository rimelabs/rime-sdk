package rime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestRealtimeSharedProtocol(t *testing.T) {
	raw, err := os.ReadFile("testdata/prism/protocol.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name              string          `json:"name"`
		Event             json.RawMessage `json:"event"`
		Invalid           bool            `json:"invalid"`
		Expected          string          `json:"expected_kind"`
		Usage             json.RawMessage `json:"expected_usage"`
		InstructionItemID *string         `json:"expected_instruction_item_id"`
		IdleTimeoutS      *float64        `json:"expected_idle_timeout_s"`
		KeptText          *string         `json:"expected_kept_text"`
		Truncation        *struct {
			ItemsLeftOut    uint64  `json:"items_left_out"`
			FirstKeptItemID *string `json:"first_kept_item_id"`
		} `json:"expected_truncation"`
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			e, err := decodeRealtime(c.Event, "session-1")
			if c.Invalid {
				if err == nil {
					t.Fatal("accepted malformed event")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			kind := "ignored"
			if e != nil {
				kind = e.kind
			}
			if kind != c.Expected {
				t.Fatalf("got %s, want %s", kind, c.Expected)
			}
			if kind == "item.truncated" {
				payload, ok := e.payload.(ItemTruncated)
				if !ok || !reflect.DeepEqual(payload.KeptText, c.KeptText) || payload.ItemID != "retained-item" || payload.ContentIndex != 0 || payload.AudioEndMS != 1200 {
					t.Fatalf("unexpected truncation: %#v", e.payload)
				}
			}
			if kind == "response.ended" {
				var want *ResponseTruncation
				if c.Truncation != nil {
					want = &ResponseTruncation{ItemsLeftOut: c.Truncation.ItemsLeftOut, FirstKeptItemID: c.Truncation.FirstKeptItemID}
				}
				if !reflect.DeepEqual(e.payload.(ResponseEnded).Truncation, want) {
					t.Fatalf("unexpected response truncation: %#v", e.payload)
				}
				if c.Usage != nil {
					var expected *ResponseUsage
					if err := json.Unmarshal(c.Usage, &expected); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(e.payload.(ResponseEnded).Usage, expected) {
						t.Fatalf("unexpected response usage: %#v", e.payload)
					}
				}
			}
			if c.InstructionItemID != nil {
				if e.payload.(ResponseStarted).InstructionItemID != *c.InstructionItemID {
					t.Fatalf("unexpected instruction ID: %#v", e.payload)
				}
			}
			if c.IdleTimeoutS != nil && e.idleTimeoutS != *c.IdleTimeoutS {
				t.Fatalf("unexpected idle timeout: %#v", e)
			}
		})
	}
}

func TestRealtimeNudgeInstructionAndUsage(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	result := make(chan ResponseRef, 1)
	done := make(chan error, 1)
	go func() {
		ref, err := s.RequestReply(context.Background(), ReplyOptions{Instruction: "Check in."})
		result <- ref
		done <- err
	}()
	q := p.next("response.create")
	p.send(rtObject{"type": "response.created", "prism_request_event_id": q["event_id"], "response": rtObject{"id": "reply-1", "metadata": rtObject{"prism_cause": "proactive", "prism_instruction_item_id": "item-instruction"}}})
	rtWait(t, done)
	rtRecv(t, s)
	started := rtRecv(t, s).Payload.(ResponseStarted)
	if started.Response != <-result || started.InstructionItemID != "item-instruction" {
		t.Fatal(started)
	}
	p.send(rtObject{"type": "response.done", "response": rtObject{"id": "reply-1", "status": "completed", "usage": rtObject{"input_tokens": 10, "output_tokens": 3, "total_tokens": 13, "context_window": 65536}}})
	ended := rtRecv(t, s).Payload.(ResponseEnded)
	if !reflect.DeepEqual(ended.Usage, &ResponseUsage{InputTokens: 10, OutputTokens: 3, TotalTokens: 13, ContextWindow: 65536}) {
		t.Fatal(ended)
	}
	go func() { done <- s.DeleteItem(context.Background(), started.InstructionItemID) }()
	q = p.next("conversation.item.delete")
	if q["item_id"] != "item-instruction" {
		t.Fatal(q)
	}
	p.send(rtObject{"type": "conversation.item.deleted", "prism_request_event_id": q["event_id"], "item_id": "item-instruction"})
	rtWait(t, done)
}

func TestRealtimeIdleTimeoutIsTerminal(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.AddMessage(context.Background(), "user", "history")
		done <- err
	}()
	p.next("conversation.item.create")
	p.send(rtObject{"type": "session.idle_timeout", "reason": "idle_timeout", "idle_timeout_s": 300})
	err := <-done
	var idle *RealtimeIdleTimeout
	if !errors.Is(err, ErrTimeout) || !errors.As(err, &idle) || idle.IdleTimeoutS != 300 {
		t.Fatal(err)
	}
	_, err = s.Recv(context.Background())
	if !errors.As(err, &idle) || idle.IdleTimeoutS != 300 {
		t.Fatal(err)
	}
}

func TestRealtimeDuplicateToolNamesDoNotConnect(t *testing.T) {
	p := newRealtimePeer(t)
	client, err := NewClient(Config{APIKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Realtime.Connect(context.Background(), RealtimeConnectOptions{
		Endpoint: "ws" + strings.TrimPrefix(p.server.URL, "http") + "/v1/realtime",
		Tools: []ToolDefinition{
			{Name: "lookup", Parameters: map[string]any{"type": "object"}},
			{Name: "lookup", Parameters: map[string]any{"type": "string"}},
		},
	})
	if !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	select {
	case <-p.conn:
		t.Fatal("invalid options opened a connection")
	default:
	}
}

func TestRealtimeOptionsValidateBeforeConnection(t *testing.T) {
	client, e := NewClient(Config{APIKey: "test"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	cases := []RealtimeConnectOptions{
		{Endpoint: "ws://example.com/v1/realtime"}, {Endpoint: "wss://example.com/wrong"}, {Endpoint: "wss://user:password@example.com/v1/realtime"},
		{TurnDetection: TurnDetection{Eagerness: "invalid"}}, {TurnDetection: TurnDetection{EndOfTurnTimeoutMS: 999}},
		{Tools: []ToolDefinition{{Name: "tool", Parameters: map[string]any{}, PrismFiller: &PrismFiller{Type: "text", Text: " "}}}},
		{Tools: []ToolDefinition{{Name: "tool", Parameters: map[string]any{}, PrismFiller: &PrismFiller{Type: "none", Text: "invalid"}}}},
		{RequestID: "  "}, {RequestID: "line\nbreak"}, {RequestID: "é"},
		{ResponseMetadata: map[string]string{"step": "\xff"}},
	}
	for _, options := range cases {
		if _, e := client.Realtime.Connect(context.Background(), options); !errors.Is(e, ErrInput) {
			t.Fatalf("%+v: %v", options, e)
		}
	}
}

func TestRealtimeResponseStartedLabelsAndInputs(t *testing.T) {
	started := func(response rtObject) ResponseStarted {
		t.Helper()
		response["id"] = "reply-1"
		b, _ := json.Marshal(rtObject{"type": "response.created", "response": response})
		w, e := decodeRealtime(b, "session-1")
		if e != nil {
			t.Fatal(e)
		}
		return w.payload.(ResponseStarted)
	}
	r := started(rtObject{"metadata": rtObject{"prism_cause": "user_text", "prism_input_item_id": "item-3", "step": "3", "nested": rtObject{"a": 1}}})
	if !reflect.DeepEqual(r.Metadata, map[string]string{"step": "3"}) || !reflect.DeepEqual(r.InputItemIDs, []string{"item-3"}) {
		t.Fatalf("%+v", r)
	}
	if r = started(rtObject{"prism_input_item_ids": []string{"item-1", "item-2"}}); !reflect.DeepEqual(r.InputItemIDs, []string{"item-1", "item-2"}) {
		t.Fatalf("%+v", r)
	}
	if r = started(rtObject{}); len(r.Metadata) != 0 || len(r.InputItemIDs) != 0 {
		t.Fatalf("%+v", r)
	}
}
