package rime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestRealtimeSharedProtocol(t *testing.T) {
	raw, err := os.ReadFile("testdata/prism/protocol.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name     string          `json:"name"`
		Event    json.RawMessage `json:"event"`
		Invalid  bool            `json:"invalid"`
		Expected string          `json:"expected_kind"`
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
	}
	for _, options := range cases {
		if _, e := client.Realtime.Connect(context.Background(), options); !errors.Is(e, ErrInput) {
			t.Fatalf("%+v: %v", options, e)
		}
	}
}
