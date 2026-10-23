package rime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRealtimeCancelledBeforeSubmission(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := s.SendText(ctx, "hello"); done <- e }()
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	_ = rtRecv(t, s)
	select {
	case q := <-p.requests:
		t.Fatal("cancelled operation sent", q)
	case <-time.After(30 * time.Millisecond):
	}
}
func TestRealtimeLateReplyIsCancelled(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := s.SendText(ctx, "hello"); done <- e }()
	q := p.next("response.create")
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	p.accept(q, "late")
	q = p.next("response.cancel")
	if q["response_id"] != "late" {
		t.Fatal(q)
	}
	p.ended("late")
	for {
		e := rtRecv(t, s)
		if _, ok := e.Payload.(ResponseAbandoned); ok {
			break
		}
	}
}
func TestRealtimeParentCloseAndEOF(t *testing.T) {
	p := newRealtimePeer(t)
	c, s := p.setup(t)
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Recv(context.Background()); !errors.Is(e, io.EOF) {
		t.Fatal(e)
	}
}
func TestRealtimeOverflow(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	for i := 0; i < 257; i++ {
		p.send(rtObject{"type": "prism.typed_input.ready"})
	}
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("overflow did not stop session")
	}
	if _, e := s.Recv(context.Background()); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
}
func TestRealtimeMalformedAndConnectionLoss(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{true: "malformed", false: "lost"}[malformed], func(t *testing.T) {
			p := newRealtimePeer(t)
			_, s := p.setup(t)
			done := make(chan error, 1)
			go func() { _, e := s.AddMessage(context.Background(), "user", "history"); done <- e }()
			_ = p.next("conversation.item.create")
			if malformed {
				p.send(rtObject{"type": "response.text.delta"})
			} else {
				_ = p.socket.CloseNow()
			}
			select {
			case e := <-done:
				var fault *RealtimeFault
				if !errors.Is(e, ErrStream) || errors.Is(e, ErrRealtime) || errors.As(e, &fault) {
					t.Fatal(e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("pending request not released")
			}
			if _, e := s.Recv(context.Background()); !errors.Is(e, ErrStream) {
				t.Fatal(e)
			}
		})
	}
}
func TestRealtimeFaultPreservesErrorsAs(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { _, e := s.AddMessage(context.Background(), "user", "history"); done <- e }()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "error", "error": rtObject{"scope": "event", "code": "invalid", "message": "bad history", "owner": rtObject{"event_id": q["event_id"]}}})
	e := <-done
	var f *RealtimeFault
	if !errors.Is(e, ErrRealtime) || errors.Is(e, ErrStream) || !errors.As(e, &f) || f.Code != "invalid" {
		t.Fatal(e)
	}
}

func TestRealtimeAdmissionDoesNotBlockControls(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	text := make(chan error, 1)
	go func() { _, e := s.SendText(ctx, "waiting for readiness"); text <- e }()
	clear := make(chan error, 1)
	go func() { clear <- s.ClearAudio(context.Background()) }()
	p.next("input_audio_buffer.clear")
	p.send(rtObject{"type": "input_audio_buffer.cleared"})
	rtWait(t, clear)
	cancel()
	<-text
}
func TestRealtimeLateClearAcknowledgmentCannotSettleNextClear(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ClearAudio(ctx) }()
	p.next("input_audio_buffer.clear")
	cancel()
	<-done
	go func() { done <- s.ClearAudio(context.Background()) }()
	select {
	case q := <-p.requests:
		t.Fatal("second clear sent before late acknowledgment", q)
	case <-time.After(30 * time.Millisecond):
	}
	p.send(rtObject{"type": "input_audio_buffer.cleared"})
	p.next("input_audio_buffer.clear")
	select {
	case e := <-done:
		t.Fatal("second clear settled with first acknowledgment", e)
	default:
	}
	p.send(rtObject{"type": "input_audio_buffer.cleared"})
	rtWait(t, done)
}
func TestRealtimeUnknownAcknowledgmentOutcomeClosesWithoutReplay(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { _, e := s.AddMessage(context.Background(), "user", "history"); done <- e }()
	p.next("conversation.item.create")
	select {
	case e := <-done:
		if !errors.Is(e, ErrTimeout) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request did not time out")
	}
	select {
	case q := <-p.requests:
		t.Fatal("request replayed", q)
	default:
	}
}
func TestRealtimeBusyWaitsForNewReadiness(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	done := make(chan error, 1)
	go func() { _, e := s.SendText(context.Background(), "hello"); done <- e }()
	q := p.next("response.create")
	p.send(rtObject{"type": "error", "error": rtObject{"scope": "event", "code": "typed_turn_busy", "owner": rtObject{"event_id": q["event_id"]}}})
	select {
	case q := <-p.requests:
		t.Fatal("retried before readiness", q)
	case <-time.After(30 * time.Millisecond):
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	q = p.next("response.create")
	p.accept(q, "retry")
	rtWait(t, done)
}
func TestRealtimeRejectsMismatchedAcknowledgment(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { _, e := s.AddMessage(context.Background(), "user", "history"); done <- e }()
	q := p.next("conversation.item.create")
	p.accept(q, "wrong")
	e := <-done
	if !errors.Is(e, ErrStream) {
		t.Fatal(e)
	}
}

func TestRealtimeBlockedWriteStillProcessesEvents(t *testing.T) {
	p := newRealtimePeerWithPause(t, true)
	_, s := p.setup(t)
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	rtRecv(t, s)
	p.send(rtObject{"type": "response.function_call_arguments.done", "response_id": "r1", "item_id": "tool", "call_id": "call-1", "name": "tool", "arguments": "{}"})
	call := rtRecv(t, s).Payload.(ToolCall).Call
	done := make(chan error, 1)
	go func() { done <- s.SubmitToolResult(context.Background(), call, strings.Repeat("x", 32*1024*1024)) }()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatal("large write completed without a reader", err)
	default:
	}
	p.send(rtObject{"type": "prism.input_audio_buffer.speech_hint", "audio_start_ms": 12})
	if _, ok := rtRecv(t, s).Payload.(SpeechHint); !ok {
		t.Fatal("socket write blocked event processing")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrStream) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unknown write outcome did not close session")
	}
}

func TestRealtimeUnrelatedFaultDoesNotRejectWaitingText(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	done := make(chan error, 1)
	go func() { _, err := s.SendText(context.Background(), "waiting"); done <- err }()
	// A history acknowledgment is a barrier while text waits for readiness.
	history := make(chan error, 1)
	go func() { _, err := s.AddMessage(context.Background(), "user", "barrier"); history <- err }()
	q := p.next("conversation.item.create")
	p.send(rtObject{"type": "conversation.item.created", "prism_request_event_id": q["event_id"], "item": rtObject{"id": "history"}})
	rtWait(t, history)
	p.send(rtObject{"type": "error", "error": rtObject{"scope": "response", "code": "unrelated", "owner": rtObject{"response_id": "other"}}})
	if _, ok := rtRecv(t, s).Payload.(FaultEvent); !ok {
		t.Fatal("missing fault event")
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	q = p.next("response.create")
	p.accept(q, "accepted")
	rtWait(t, done)
}

func TestRealtimeCancelledLateRepliesReleaseCapacity(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	// Consume events so this test measures command capacity, not event overflow.
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		for {
			if _, err := s.Recv(context.Background()); err != nil {
				return
			}
		}
	}()
	defer func() { _ = s.Close(); <-eventsDone }()
	for i := 0; i < 260; i++ {
		p.send(rtObject{"type": "prism.typed_input.ready"})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := s.SendText(ctx, "cancel me"); done <- err }()
		q := p.next("response.create")
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		id := fmt.Sprintf("late-%d", i)
		p.accept(q, id)
		p.next("response.cancel")
		p.ended(id)
	}
	p.turn(s, "after-cancellations")
}

func TestRealtimeTerminalErrorPrecedesQueuedEvents(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	p.send(rtObject{"type": "prism.typed_input.ready"})
	// A malformed event terminates the same ordered socket stream.
	p.send(rtObject{"type": "response.text.delta"})
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not fail")
	}
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrStream) {
		t.Fatal(err)
	}
	if _, err := s.AddMessage(context.Background(), "user", "after failure"); !errors.Is(err, ErrStream) {
		t.Fatal(err)
	}
}

func TestRealtimeQueuedAudioCancellationPreservesOtherWork(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	clear := make(chan error, 1)
	go func() { clear <- s.ClearAudio(context.Background()) }()
	p.next("input_audio_buffer.clear")
	ctx, cancel := context.WithCancel(context.Background())
	audio := make(chan error, 1)
	go func() { audio <- s.SendAudio(ctx, AudioChunk{Data: make([]byte, 2560)}) }()
	cancel()
	if err := <-audio; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.send(rtObject{"type": "input_audio_buffer.cleared"})
	rtWait(t, clear)
	p.turn(s, "unrelated")
}

func TestRealtimeActiveAudioCancellationClosesSession(t *testing.T) {
	p := newRealtimePeerWithPause(t, true)
	connections := make(chan *realtimeAudioWriteConn, 1)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		controlled := &realtimeAudioWriteConn{Conn: conn, started: make(chan struct{}), closed: make(chan struct{})}
		connections <- controlled
		return controlled, nil
	}}
	t.Cleanup(transport.CloseIdleConnections)
	// The write deadline must outlast the test: only cancellation should close it.
	_, s := p.setupWithTransport(t, RealtimeConnectOptions{Timeouts: RealtimeTimeouts{Request: time.Minute}}, transport)
	conn := <-connections
	// Initialization uses the real socket. Hold the next write until cancellation
	// closes it, so the test observes transport entry without filling OS buffers.
	conn.hold.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- s.SendAudio(ctx, AudioChunk{Data: make([]byte, 2560)}) }()
	select {
	case <-conn.started:
	case err := <-done:
		t.Fatalf("audio returned before transport entry: %v", err)
	case <-wait.Done():
		t.Fatal("audio did not enter the transport")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-wait.Done():
		t.Fatal("audio did not return after cancellation")
	}
	if _, err := s.Recv(wait); !errors.Is(err, ErrStream) {
		t.Fatalf("active cancellation did not fail the session: %v", err)
	}
	if _, err := s.AddMessage(wait, "user", "after cancellation"); !errors.Is(err, ErrStream) {
		t.Fatalf("later operation lost the terminal failure: %v", err)
	}
}

type realtimeAudioWriteConn struct {
	net.Conn
	hold      atomic.Bool
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (c *realtimeAudioWriteConn) Write(data []byte) (int, error) {
	if c.hold.Load() {
		c.startOnce.Do(func() { close(c.started) })
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(data)
}

func (c *realtimeAudioWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
