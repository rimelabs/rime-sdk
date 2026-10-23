// Package testpeer supplies a local Prism peer for example tests.
package testpeer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type Object = map[string]any

type Peer struct {
	t        *testing.T
	ctx      context.Context
	conn     chan *websocket.Conn
	socket   *websocket.Conn
	requests chan Object
}

func New(t *testing.T) *Peer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	p := &Peer{t: t, ctx: ctx, conn: make(chan *websocket.Conn, 1), requests: make(chan Object, 128)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		p.conn <- c
		send := func(v Object) {
			data, _ := json.Marshal(v)
			_ = c.Write(ctx, websocket.MessageText, data)
		}
		send(Object{"type": "session.created", "session": Object{"id": "test-session"}})
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var request Object
			if err := json.Unmarshal(data, &request); err != nil {
				t.Error(err)
				return
			}
			if request["type"] == "session.update" {
				send(Object{"type": "session.updated", "prism_request_event_id": request["event_id"], "session": Object{"id": "test-session"}})
				send(Object{"type": "prism.typed_input.ready"})
			} else {
				select {
				case p.requests <- request:
				case <-ctx.Done():
					return
				}
			}
		}
	}))
	t.Cleanup(func() { cancel(); server.Close() })
	t.Setenv("RIME_API_KEY", "test")
	t.Setenv("PRISM_URL", "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/realtime")
	return p
}

func (p *Peer) Send(v Object) {
	p.t.Helper()
	if p.socket == nil {
		select {
		case p.socket = <-p.conn:
		case <-p.ctx.Done():
			p.t.Fatal("example did not connect")
		}
	}
	data, err := json.Marshal(v)
	if err == nil {
		err = p.socket.Write(p.ctx, websocket.MessageText, data)
	}
	if err != nil {
		p.t.Fatal(err)
	}
}

func (p *Peer) Next(kind string) Object {
	p.t.Helper()
	select {
	case request := <-p.requests:
		if request["type"] != kind {
			p.t.Fatalf("request = %v, want %s", request, kind)
		}
		return request
	case <-p.ctx.Done():
		p.t.Fatalf("example did not send %s", kind)
		return nil
	}
}

func (p *Peer) NoRequest() {
	p.t.Helper()
	select {
	case request := <-p.requests:
		p.t.Fatalf("request sent before readiness: %v", request)
	case <-time.After(30 * time.Millisecond):
	case <-p.ctx.Done():
		p.t.Fatal("peer expired while waiting for no request")
	}
}

func Run(run func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- run() }()
	return done
}

func Result(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("example did not return promptly")
		return nil
	}
}
