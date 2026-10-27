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
		Name       string          `json:"name"`
		Event      json.RawMessage `json:"event"`
		Invalid    bool            `json:"invalid"`
		Expected   string          `json:"expected_kind"`
		KeptText   *string         `json:"expected_kept_text"`
		Truncation *struct {
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
			}
		})
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
