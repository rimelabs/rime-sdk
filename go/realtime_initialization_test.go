package rime

import (
	"context"
	"errors"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRealtimeDisconnectDuringInitialization(t *testing.T) {
	for _, created := range []bool{false, true} {
		name := "before-created"
		if created {
			name = "before-updated"
		}
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				if created {
					_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"session.created","session":{"id":"session-1"}}`))
					_, _, _ = c.Read(r.Context())
				}
			}))
			defer server.Close()
			client, err := NewClient(Config{APIKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = client.Realtime.Connect(ctx, RealtimeConnectOptions{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"})
			if !errors.Is(err, ErrStream) || ctx.Err() != nil {
				t.Fatalf("error=%v context=%v", err, ctx.Err())
			}
		})
	}
}

func TestRealtimeCancellationWinsBufferedInitialization(t *testing.T) {
	// Exercise the actual Connect result-delivery path with both cases ready.
	// Each iteration uses a live session so Close must confirm real worker cleanup.
	for i := 0; i < 32; i++ {
		p := newRealtimePeer(t)
		_, s := p.setup(t)
		s.initialized <- nil
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := s.awaitInitialization(ctx)
		if result != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: session=%v error=%v", i, result, err)
		}
		select {
		case <-s.done:
		default:
			t.Fatal("initialization cancellation returned before cleanup")
		}
	}
}
