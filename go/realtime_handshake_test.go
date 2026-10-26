package rime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRealtimeHandshakeRefusal(t *testing.T) {
	cases := []struct {
		status int
		body   string
		kind   ErrorKind
		detail string
	}{
		{401, "Authentication failed", ErrAuthentication, "Authentication failed"},
		{403, "Forbidden", ErrAuthentication, "Forbidden"},
		{429, "Currently at websocket limit", ErrResourceLimit, "websocket limit"},
		{429, "", ErrResourceLimit, "connection or rate limit reached"},
		{503, "conversation capacity reached", ErrUnavailable, "capacity reached"},
		{503, "engine is draining", ErrUnavailable, "engine is draining"},
		{503, `{"error":{"message":"graph is not ready"}}`, ErrUnavailable, "graph is not ready"},
		{503, "<html>proxy error</html>", ErrUnavailable, ""},
		{503, `{"error":`, ErrUnavailable, ""},
		{503, `{"error":null}`, ErrUnavailable, ""},
		{503, "\xff", ErrUnavailable, ""},
		{503, "bad\x00message", ErrUnavailable, ""},
		{429, strings.Repeat("x", 2048), ErrResourceLimit, ""},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var attempts atomic.Int32
			released := make(chan struct{}, 4)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.Header().Set("x-request-id", "server-request")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateIdle || state == http.StateClosed {
					released <- struct{}{}
				}
			}
			server.Start()
			defer server.Close()
			client, err := NewClient(Config{APIKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for attempt := 0; attempt < 2; attempt++ {
				session, err := client.Realtime.Connect(ctx, RealtimeConnectOptions{
					Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime",
				})
				var sdkError *Error
				if session != nil || !errors.Is(err, tc.kind) || !errors.As(err, &sdkError) {
					t.Fatalf("session=%v error=%v", session, err)
				}
				if !strings.Contains(sdkError.Message, fmt.Sprintf("HTTP %d", tc.status)) || sdkError.RequestID != "server-request" {
					t.Fatalf("diagnostics: %+v", sdkError)
				}
				if tc.detail != "" {
					if !strings.Contains(sdkError.Message, tc.detail) {
						t.Fatal(sdkError.Message)
					}
				} else if strings.Contains(sdkError.Message, ": ") {
					t.Fatal(sdkError.Message)
				}
				select {
				case <-released:
				case <-ctx.Done():
					t.Fatal("refused HTTP request stayed active")
				}
			}
			if attempts.Load() != 2 {
				t.Fatalf("unexpected attempts: %d", attempts.Load())
			}
		})
	}
}

func TestRealtimeHandshakeUnfinishedBody(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(429)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	client, err := NewClient(Config{APIKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Realtime.Connect(ctx, RealtimeConnectOptions{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime",
		Timeouts: RealtimeTimeouts{Connect: 100 * time.Millisecond},
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("unfinished body kept connection open")
	}
}
