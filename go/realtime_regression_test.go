package rime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRealtimeToolDescriptionRejectsInvalidUTF8(t *testing.T) {
	p := newRealtimePeer(t)
	client, err := NewClient(Config{APIKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Realtime.Connect(context.Background(), RealtimeConnectOptions{
		Endpoint: "ws" + strings.TrimPrefix(p.server.URL, "http") + "/v1/realtime",
		Tools:    []ToolDefinition{{Name: "test", Description: string([]byte{0xff}), Parameters: map[string]any{"type": "object"}}},
	})
	if !errors.Is(err, ErrInput) {
		t.Fatalf("invalid UTF-8 accepted: %v", err)
	}
	select {
	case <-p.conn:
		t.Fatal("invalid description opened a connection")
	default:
	}
}

func TestRealtimeToolArgumentsPreserveNumbers(t *testing.T) {
	for _, argument := range []string{`{"id":9007199254740993}`, `{"value":1.234567890123456789}`, `{"nested":[{"id":9007199254740993}]}`} {
		raw, err := json.Marshal(rtObject{"type": "response.function_call_arguments.done", "response_id": "r", "call_id": "c", "item_id": "i", "name": "test", "arguments": argument})
		if err != nil {
			t.Fatal(err)
		}
		w, err := decodeRealtime(raw, "s")
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(w.payload.(ToolCall).Arguments)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != argument {
			t.Fatalf("arguments changed: got %s, want %s", got, argument)
		}
	}
	for _, argument := range []string{`{} {}`, `{} trailing`, `null`, `[]`, `1`, `{"nested":[{"value":1e999}]}`} {
		raw, _ := json.Marshal(rtObject{"type": "response.function_call_arguments.done", "response_id": "r", "call_id": "c", "item_id": "i", "name": "test", "arguments": argument})
		if _, err := decodeRealtime(raw, "s"); !errors.Is(err, ErrStream) {
			t.Fatalf("accepted invalid arguments %q: %v", argument, err)
		}
	}
}

func TestRealtimeSettingsPreserveSchemaNumbers(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "integer", "const": int64(9007199254740993)}}}
	_, settings, _, err := rtSettings(RealtimeConnectOptions{Tools: []ToolDefinition{{Name: "test", Parameters: schema}}})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(settings["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"])
	want, _ := json.Marshal(schema)
	if string(got) != string(want) {
		t.Fatalf("schema changed: got %s, want %s", got, want)
	}
}

func TestRealtimeRecvTerminalPrecedesAvailableEvent(t *testing.T) {
	// A failure can be stored before worker cleanup closes done. Recv must not
	// deliver an event in this interval, including when it already waits for one.
	s := &RealtimeSession{events: make(chan SessionEvent, 1), done: make(chan struct{}), terminal: failure(ErrStream, "terminal failure")}
	s.events <- SessionEvent{}
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrStream) {
		t.Fatalf("queued event returned despite terminal error: %v", err)
	}
}

func TestRealtimeCancellationWinsBufferedInitializationError(t *testing.T) {
	for i := 0; i < 32; i++ {
		p := newRealtimePeer(t)
		_, s := p.setup(t)
		s.initialized <- failure(ErrStream, "initialization failed")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := s.awaitInitialization(ctx)
		if result != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("session=%v error=%v", result, err)
		}
	}
}

func TestRealtimeLifetimeCancellationPreservesCause(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			for i := 0; i < 64; i++ {
				p := newRealtimePeer(t)
				_, s := p.setup(t)
				s.op.cancel(cause)
				select {
				case <-s.done:
				case <-time.After(3 * time.Second):
					t.Fatal("session did not stop")
				}
				if !errors.Is(s.failure(), cause) {
					t.Fatalf("lost cancellation cause: got %v, want %v", s.failure(), cause)
				}
			}
		})
	}
}
