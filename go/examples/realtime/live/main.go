// Command live connects application-owned audio adapters over a local TCP bridge.
// The bridge must supply device playback measurements. See the SDK README.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"

	rime "github.com/rimelabs/rime-sdk/go"
)

type message struct {
	Type     string   `json:"type"`
	ID       string   `json:"id,omitempty"`
	IDs      []string `json:"ids,omitempty"`
	Data     []byte   `json:"data,omitempty"`
	PlayedMS *float64 `json:"played_ms,omitempty"`
}
type bridge struct {
	conn    net.Conn
	ctx     context.Context
	cancel  context.CancelCauseFunc
	send    chan message
	audio   chan []byte
	stopped chan struct{}
	mu      sync.Mutex
	outputs map[string]*output
	tasks   sync.WaitGroup
}
type output struct {
	bridge  *bridge
	id      string
	receipt chan float64
}

func (b *bridge) queue(v message) error {
	select {
	case <-b.ctx.Done():
		return context.Cause(b.ctx)
	default:
	}
	select {
	case b.send <- v:
		return nil
	default:
		return fmt.Errorf("audio bridge output queue is full")
	}
}
func (b *bridge) Read(ctx context.Context) (rime.AudioChunk, error) {
	select {
	case data, ok := <-b.audio:
		if !ok {
			return rime.AudioChunk{}, io.EOF
		}
		return rime.AudioChunk{Data: data}, nil
	case <-ctx.Done():
		return rime.AudioChunk{}, ctx.Err()
	case <-b.ctx.Done():
		return rime.AudioChunk{}, context.Cause(b.ctx)
	}
}
func (b *bridge) Open(ref rime.OutputRef) (rime.ConversationOutput, error) {
	id := fmt.Sprintf("%s/%s/%d", ref.Response().ResponseID(), ref.ItemID(), ref.OutputIndex())
	o := &output{b, id, make(chan float64, 1)}
	b.mu.Lock()
	b.outputs[id] = o
	b.mu.Unlock()
	return o, b.queue(message{Type: "open", ID: id})
}
func (b *bridge) Interrupt(refs []rime.OutputRef) error {
	ids := make([]string, len(refs))
	for i, r := range refs {
		ids[i] = fmt.Sprintf("%s/%s/%d", r.Response().ResponseID(), r.ItemID(), r.OutputIndex())
	}
	return b.queue(message{Type: "interrupt", IDs: ids})
}
func (b *bridge) Stop(ctx context.Context) error {
	if e := b.queue(message{Type: "stop"}); e != nil {
		return e
	}
	select {
	case <-b.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return context.Cause(b.ctx)
	}
}
func (o *output) Write(chunk rime.AudioChunk) error {
	return o.bridge.queue(message{Type: "audio", ID: o.id, Data: chunk.Data})
}
func (o *output) End() error { return o.bridge.queue(message{Type: "end", ID: o.id}) }
func (o *output) Completion(ctx context.Context) (float64, error) {
	select {
	case n := <-o.receipt:
		return n, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-o.bridge.ctx.Done():
		return 0, context.Cause(o.bridge.ctx)
	}
}
func openBridge(ctx context.Context, address string) (*bridge, error) {
	conn, e := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if e != nil {
		return nil, e
	}
	lifetime, cancel := context.WithCancelCause(context.Background())
	b := &bridge{conn: conn, ctx: lifetime, cancel: cancel, send: make(chan message, 128), audio: make(chan []byte, 128), stopped: make(chan struct{}, 1), outputs: map[string]*output{}}
	b.tasks.Add(2)
	go func() {
		defer b.tasks.Done()
		writer := json.NewEncoder(conn)
		for {
			select {
			case v := <-b.send:
				if e := writer.Encode(v); e != nil {
					cancel(e)
					return
				}
			case <-lifetime.Done():
				return
			}
		}
	}()
	go func() {
		defer b.tasks.Done()
		scanner := bufio.NewScanner(conn)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		captureEOF := false
		for scanner.Scan() {
			var v message
			if e := json.Unmarshal(scanner.Bytes(), &v); e != nil {
				cancel(e)
				return
			}
			switch v.Type {
			case "audio":
				if captureEOF {
					cancel(fmt.Errorf("audio after capture EOF"))
					return
				}
				if len(v.Data) > 192000 || len(v.Data)%2 != 0 {
					cancel(fmt.Errorf("invalid capture chunk"))
					return
				}
				select {
				case b.audio <- v.Data:
				default:
					cancel(fmt.Errorf("capture queue is full"))
					return
				}
			case "played":
				if v.ID == "" || v.PlayedMS == nil || *v.PlayedMS < 0 {
					cancel(fmt.Errorf("played receipt requires an id and nonnegative played_ms"))
					return
				}
				b.mu.Lock()
				o := b.outputs[v.ID]
				delete(b.outputs, v.ID)
				b.mu.Unlock()
				if o == nil {
					cancel(fmt.Errorf("played receipt has unknown output id %q", v.ID))
					return
				}
				o.receipt <- *v.PlayedMS
			case "stopped":
				select {
				case b.stopped <- struct{}{}:
				default:
				}
			case "eof":
				if !captureEOF {
					close(b.audio)
					captureEOF = true
				}
			default:
				cancel(fmt.Errorf("unknown bridge message %q", v.Type))
				return
			}
		}
		e := scanner.Err()
		if e == nil {
			e = io.ErrUnexpectedEOF
		}
		cancel(e)
	}()
	return b, nil
}
func (b *bridge) Close() { b.cancel(context.Canceled); _ = b.conn.Close(); b.tasks.Wait() }
func run() error {
	address := "127.0.0.1:8766"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	b, e := openBridge(ctx, address)
	if e != nil {
		return e
	}
	defer b.Close()
	client, e := rime.NewClient(rime.Config{})
	if e != nil {
		return e
	}
	defer client.Close()
	session, e := client.Realtime.Connect(ctx, rime.RealtimeConnectOptions{Endpoint: os.Getenv("PRISM_URL")})
	if e != nil {
		return e
	}
	runner := rime.ConversationRunner{Session: session, Source: b, Playback: b, OnEvent: func(e rime.SessionEvent) error {
		if p, ok := e.Payload.(rime.TextDelta); ok {
			fmt.Fprint(os.Stderr, p.Delta)
		}
		return nil
	}}
	return runner.Run(ctx)
}
func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
