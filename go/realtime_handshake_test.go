package rime

import (
	"context"
	"errors"
	"fmt"
	"github.com/coder/websocket"
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
				if code, ok := sdkError.StatusCode(); !ok || code != tc.status {
					t.Fatalf("status=%d present=%v", code, ok)
				}
				if _, ok := sdkError.CloseCode(); ok {
					t.Fatal("HTTP refusal has a close code")
				}
				if _, ok := sdkError.RetryAfter(); ok {
					t.Fatal("missing header has retry timing")
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

func TestRealtimeConnectionRetryAfter(t *testing.T) {
	cases := []struct {
		values   []string
		expected time.Duration
		present  bool
	}{
		{[]string{"3"}, 3 * time.Second, true}, {[]string{"0"}, 0, true},
		{[]string{"-1"}, 0, false}, {[]string{"1.5"}, 0, false},
		{[]string{"nan"}, 0, false}, {[]string{"inf"}, 0, false},
		{[]string{"bad date"}, 0, false}, {[]string{strings.Repeat("9", 400)}, 0, false},
		{[]string{"1", "2"}, 0, false}, {[]string{"Wed, 21 Oct 2015 07:28:00 GMT"}, 0, true},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header()["Retry-After"] = tc.values
				w.WriteHeader(503)
			}))
			defer server.Close()
			client, err := NewClient(Config{APIKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = client.Realtime.Connect(ctx, RealtimeConnectOptions{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"})
			var sdkError *Error
			if !errors.As(err, &sdkError) {
				t.Fatalf("error=%v", err)
			}
			if delay, ok := sdkError.RetryAfter(); delay != tc.expected || ok != tc.present {
				t.Fatalf("delay=%v present=%v", delay, ok)
			}
		})
	}
	date := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	delay := rtRetryAfter([]string{date})
	if delay == nil || *delay < 28*time.Second || *delay > 30*time.Second {
		t.Fatalf("date delay=%v", delay)
	}
}

func TestRealtimeConnectionCloseCode(t *testing.T) {
	for _, code := range []int{1000, 1001, 1008, 1013, 0} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				if code == 0 {
					return
				}
				conn.Close(websocket.StatusCode(code), "")
			}))
			defer server.Close()
			client, err := NewClient(Config{APIKey: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = client.Realtime.Connect(ctx, RealtimeConnectOptions{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"})
			var sdkError *Error
			if !errors.As(err, &sdkError) {
				t.Fatalf("error=%v", err)
			}
			if got, ok := sdkError.CloseCode(); ok != (code != 0) || got != code {
				t.Fatalf("close=%d present=%v", got, ok)
			}
			if _, ok := sdkError.StatusCode(); ok {
				t.Fatal("close has HTTP status")
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
