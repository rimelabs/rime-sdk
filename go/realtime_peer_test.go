package rime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type realtimePeer struct {
	t        *testing.T
	server   *httptest.Server
	conn     chan *websocket.Conn
	socket   *websocket.Conn
	requests chan rtObject
	// The upgrade's x-request-id.
	requestID string
	// Settings from automatically acknowledged initialization and label updates.
	// Other session updates go to requests for manual acknowledgment.
	updates chan rtObject
	// Refuse automatically handled updates with invalid_session.
	refuse atomic.Bool
}

func newRealtimePeer(t *testing.T) *realtimePeer { return newRealtimePeerWithPause(t, false) }
func newRealtimePeerWithPause(t *testing.T, pause bool) *realtimePeer {
	return newRealtimePeerWithSession(t, pause, nil)
}
func newRealtimePeerWithSession(t *testing.T, pause bool, initial rtObject) *realtimePeer {
	if initial == nil {
		initial = rtObject{"id": "session-1", "voice": "test"}
	}
	if _, err := rtRequestSchemas(); err != nil {
		t.Fatal(err)
	}
	resume := make(chan struct{})
	p := &realtimePeer{t: t, conn: make(chan *websocket.Conn, 1), requests: make(chan rtObject, 512), updates: make(chan rtObject, 16)}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("missing bearer credentials")
		}
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		p.requestID = r.Header.Get("x-request-id")
		requestID := p.requestID
		if requestID == "" {
			requestID = "server-made"
		}
		p.conn <- c
		send := func(v rtObject) error { b, _ := json.Marshal(v); return c.Write(r.Context(), websocket.MessageText, b) }
		_ = send(rtObject{"type": "session.created", "session": rtObject{"id": "session-1"}})
		initialized := false
		for {
			_, b, e := c.Read(r.Context())
			if e != nil {
				return
			}
			var v rtObject
			if e = json.Unmarshal(b, &v); e != nil {
				t.Error(e)
				return
			}
			if err := rtValidateRequest(v); err != nil {
				t.Errorf("request %v violates Prism schema: %v", v["type"], err)
				return
			}
			settings, _ := v["session"].(map[string]any)
			_, labelsUpdate := settings["prism_response_metadata"]
			if v["type"] == "session.update" && (!initialized || labelsUpdate) {
				initialized = true
				session := rtObject{}
				for key, value := range initial {
					session[key] = value
				}
				session["request_id"] = requestID
				if labels, ok := settings["prism_response_metadata"]; ok {
					session["prism_response_metadata"] = labels
				}
				select {
				case p.updates <- settings:
				default:
				}
				if p.refuse.Load() {
					_ = send(rtObject{"type": "error", "error": rtObject{"code": "invalid_session", "scope": "event", "owner": rtObject{"event_id": v["event_id"]}}})
					continue
				}
				_ = send(rtObject{"type": "session.updated", "prism_request_event_id": v["event_id"], "session": session})
				if pause {
					<-resume
				}
			} else {
				select {
				case p.requests <- v:
				case <-r.Context().Done():
					return
				}
			}
		}
	}))
	t.Cleanup(func() {
		close(resume)
		if p.socket != nil {
			_ = p.socket.CloseNow()
		}
		p.server.Close()
	})
	return p
}
func (p *realtimePeer) setup(t *testing.T) (*Client, *RealtimeSession) {
	return p.setupWith(t, RealtimeConnectOptions{})
}
func (p *realtimePeer) setupWith(t *testing.T, o RealtimeConnectOptions) (*Client, *RealtimeSession) {
	return p.setupWithTransport(t, o, nil)
}
func (p *realtimePeer) setupWithTransport(t *testing.T, o RealtimeConnectOptions, transport http.RoundTripper) (*Client, *RealtimeSession) {
	c, e := NewClient(Config{APIKey: "test", Timeout: time.Nanosecond})
	if e != nil {
		t.Fatal(e)
	}
	c.realtimeTransport = transport
	t.Cleanup(func() { _ = c.Close() })
	o.Endpoint = "ws" + strings.TrimPrefix(p.server.URL, "http") + "/v1/realtime"
	if o.Timeouts.Connect == 0 {
		o.Timeouts.Connect = time.Second
	}
	if o.Timeouts.Ready == 0 {
		o.Timeouts.Ready = time.Second
	}
	if o.Timeouts.Request == 0 {
		o.Timeouts.Request = time.Second
	}
	s, e := c.Realtime.Connect(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	p.socket = <-p.conn
	return c, s
}
func (p *realtimePeer) send(v rtObject) {
	p.t.Helper()
	b, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e := p.socket.Write(ctx, websocket.MessageText, b); e != nil {
		p.t.Fatal(e)
	}
}
func (p *realtimePeer) next(kind string) rtObject {
	p.t.Helper()
	select {
	case v := <-p.requests:
		if v["type"] != kind {
			p.t.Fatalf("got %v, want %s", v, kind)
		}
		return v
	case <-time.After(3 * time.Second):
		p.t.Fatalf("no %s request", kind)
		return nil
	}
}
func (p *realtimePeer) update() rtObject {
	p.t.Helper()
	select {
	case v := <-p.updates:
		return v
	case <-time.After(3 * time.Second):
		p.t.Fatal("no session.update")
		return nil
	}
}
func (p *realtimePeer) accept(request rtObject, id string) {
	p.send(rtObject{"type": "response.created", "prism_request_event_id": request["event_id"], "response": rtObject{"id": id}})
}
func (p *realtimePeer) ended(id string) {
	p.send(rtObject{"type": "response.done", "response": rtObject{"id": id, "status": "completed", "metadata": rtObject{"prism_drain_token": "token-" + id}}})
}
func (p *realtimePeer) turn(s *RealtimeSession, id string) ResponseRef {
	p.t.Helper()
	p.send(rtObject{"type": "prism.typed_input.ready"})
	done := make(chan ResponseRef, 1)
	go func() {
		r, e := s.SendText(context.Background(), "hello")
		if e != nil {
			p.t.Error(e)
		}
		done <- r
	}()
	request := p.next("response.create")
	p.accept(request, id)
	select {
	case r := <-done:
		return r
	case <-time.After(3 * time.Second):
		p.t.Fatal("turn did not complete")
		return ResponseRef{}
	}
}
func rtRecv(t *testing.T, s *RealtimeSession) SessionEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e, err := s.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func rtWait(t *testing.T, c <-chan error) {
	t.Helper()
	select {
	case e := <-c:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
	}
}
