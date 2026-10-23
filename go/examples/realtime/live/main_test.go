package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

func testBridge(t *testing.T) (*bridge, net.Conn, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	b, err := openBridge(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := b.Open(rime.OutputRef{}); err != nil {
		t.Fatal(err)
	}
	var opened message
	if err := json.NewDecoder(peer).Decode(&opened); err != nil {
		t.Fatal(err)
	}
	return b, peer, opened.ID
}

func TestMalformedPlaybackReceiptFailsBridge(t *testing.T) {
	for _, receipt := range []string{
		`{"type":"played","played_ms":0}`,
		`{"type":"played","id":"%s"}`,
		`{"type":"played","id":"%s","played_ms":null}`,
		`{"type":"played","id":"%s","played_ms":-1}`,
		`{"type":"played","id":"unknown","played_ms":1}`,
	} {
		t.Run(receipt, func(t *testing.T) {
			b, peer, id := testBridge(t)
			if _, err := fmt.Fprintln(peer, strings.ReplaceAll(receipt, "%s", id)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-b.ctx.Done():
				if err := context.Cause(b.ctx); !strings.Contains(err.Error(), "played receipt") {
					t.Fatalf("error = %v, want receipt error", err)
				}
			case <-time.After(time.Second):
				t.Fatal("invalid playback receipt did not fail the bridge")
			}
		})
	}
}

func TestPlaybackReceiptAcceptsMeasuredZero(t *testing.T) {
	b, peer, id := testBridge(t)
	b.mu.Lock()
	o := b.outputs[id]
	b.mu.Unlock()
	if _, err := fmt.Fprintf(peer, "{\"type\":\"played\",\"id\":%q,\"played_ms\":0}\n", id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if played, err := o.Completion(ctx); err != nil || played != 0 {
		t.Fatalf("completion = %v, %v; want zero playback", played, err)
	}
}
