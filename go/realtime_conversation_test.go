package rime

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rtControlledSource struct{ stopped chan struct{} }

func (s *rtControlledSource) Read(ctx context.Context) (AudioChunk, error) {
	<-ctx.Done()
	close(s.stopped)
	return AudioChunk{}, ctx.Err()
}

type rtControlledOutput struct {
	ref     OutputRef
	receipt chan float64
	writes  atomic.Int32
	sealed  atomic.Bool
}

func (o *rtControlledOutput) Write(AudioChunk) error { o.writes.Add(1); return nil }
func (o *rtControlledOutput) End() error             { o.sealed.Store(true); return nil }
func (o *rtControlledOutput) Completion(ctx context.Context) (float64, error) {
	select {
	case value := <-o.receipt:
		return value, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type rtControlledPlayback struct {
	opened  chan *rtControlledOutput
	mu      sync.Mutex
	outputs map[OutputRef]*rtControlledOutput
	stopped atomic.Bool
}

func (p *rtControlledPlayback) Open(ref OutputRef) (ConversationOutput, error) {
	o := &rtControlledOutput{ref: ref, receipt: make(chan float64, 1)}
	p.mu.Lock()
	p.outputs[ref] = o
	p.mu.Unlock()
	p.opened <- o
	return o, nil
}
func (p *rtControlledPlayback) Interrupt(refs []OutputRef) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ref := range refs {
		select {
		case p.outputs[ref].receipt <- 12:
		default:
		}
	}
	return nil
}
func (p *rtControlledPlayback) Stop(context.Context) error { p.stopped.Store(true); return nil }
func rtOutput(t *testing.T, p *rtControlledPlayback) *rtControlledOutput {
	t.Helper()
	select {
	case o := <-p.opened:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("output did not open")
		return nil
	}
}
func rtEmitOutput(p *realtimePeer, response, item string) {
	p.send(rtObject{"type": "response.output_item.added", "response_id": response, "output_index": 0, "item": rtObject{"type": "message", "id": item}})
	p.send(rtObject{"type": "response.audio.delta", "response_id": response, "item_id": item, "output_index": 0, "content_index": 0, "delta": "AAA="})
}
func TestRealtimeRunnerWaitsForMeasuredPlaybackInOrder(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	source := &rtControlledSource{stopped: make(chan struct{})}
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- (&ConversationRunner{Session: s, Source: source, Playback: playback}).Run(ctx) }()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	rtEmitOutput(p, "r1", "i1")
	o1 := rtOutput(t, playback)
	p.ended("r1")
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r2"}})
	rtEmitOutput(p, "r2", "i2")
	o2 := rtOutput(t, playback)
	p.ended("r2")
	o2.receipt <- 20
	select {
	case q := <-p.requests:
		t.Fatal("reported before earlier receipt", q)
	case <-time.After(30 * time.Millisecond):
	}
	o1.receipt <- 10
	for _, id := range []string{"r1", "r2"} {
		q := p.next("prism.playback.drained")
		if q["response_id"] != id {
			t.Fatal(q)
		}
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, ErrCancelled) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}
	if !playback.stopped.Load() {
		t.Fatal("playback not stopped")
	}
	select {
	case <-source.stopped:
	default:
		t.Fatal("capture still running")
	}
}
func TestRealtimeRunnerInterruptsAndRejectsLateAudio(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	source := &rtControlledSource{stopped: make(chan struct{})}
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan SessionEvent, 16)
	done := make(chan error, 1)
	go func() {
		done <- (&ConversationRunner{Session: s, Source: source, Playback: playback, OnEvent: func(e SessionEvent) error { seen <- e; return nil }}).Run(ctx)
	}()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	rtEmitOutput(p, "r1", "i1")
	o := rtOutput(t, playback)
	p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
	q := p.next("conversation.item.truncate")
	if q["audio_end_ms"] != float64(12) {
		t.Fatal(q)
	}
	p.send(rtObject{"type": "conversation.item.truncated", "item_id": "i1", "content_index": 0})
	p.send(rtObject{"type": "response.audio.delta", "response_id": "r1", "item_id": "i1", "output_index": 0, "content_index": 0, "delta": "AAA="})
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "child", "metadata": rtObject{"prism_cause": "tool_continuation", "prism_parent_response_id": "r1"}}})
	q = p.next("response.cancel")
	if q["response_id"] != "child" {
		t.Fatal(q)
	}
	p.ended("child")
	if o.writes.Load() != 1 {
		t.Fatal("late audio was played", o.writes.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}
}

type rtFailingReleaseSource struct{ failure error }

func (s rtFailingReleaseSource) Read(ctx context.Context) (AudioChunk, error) {
	<-ctx.Done()
	return AudioChunk{}, s.failure
}
func TestRealtimeRunnerPreservesCaptureCleanupFailure(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	expected := errors.New("capture release failed")
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	done := make(chan error, 1)
	go func() {
		done <- (&ConversationRunner{Session: s, Source: rtFailingReleaseSource{expected}, Playback: playback}).Run(context.Background())
	}()
	_ = s.Close()
	select {
	case err := <-done:
		if !errors.Is(err, expected) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not finish")
	}
}
func TestRealtimeRunnerNormalCloseWithPendingReport(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	source := &rtControlledSource{stopped: make(chan struct{})}
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	done := make(chan error, 1)
	go func() {
		done <- (&ConversationRunner{Session: s, Source: source, Playback: playback}).Run(context.Background())
	}()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	rtEmitOutput(p, "r1", "i1")
	rtOutput(t, playback)
	p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
	p.next("conversation.item.truncate")
	_ = s.Close()
	rtWait(t, done)
}

// This adapter deliberately violates the cancellation contract. The test releases
// it explicitly so it demonstrates the wait without leaking a goroutine.
type rtUncooperativeSource struct{ entered, release chan struct{} }

func (s rtUncooperativeSource) Read(context.Context) (AudioChunk, error) {
	close(s.entered)
	<-s.release
	return AudioChunk{}, io.EOF
}
func TestRealtimeRunnerWaitsForUncooperativeAdapterCleanup(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	source := rtUncooperativeSource{make(chan struct{}), make(chan struct{})}
	release := sync.OnceFunc(func() { close(source.release) })
	defer release()
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- (&ConversationRunner{Session: s, Source: source, Playback: playback}).Run(ctx) }()
	<-source.entered
	cancel()
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatal("session cleanup blocked")
	}
	if !playback.stopped.Load() {
		t.Fatal("playback did not stop")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before adapter cleanup: %v", err)
	default:
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not finish after adapter release")
	}
}

type rtConcurrentReceipt struct {
	entered, ended, interrupted chan struct{}
	measured                    chan float64
	writes                      atomic.Int32
}

func (o *rtConcurrentReceipt) Write(AudioChunk) error { o.writes.Add(1); return nil }
func (o *rtConcurrentReceipt) End() error             { close(o.ended); return nil }
func (o *rtConcurrentReceipt) Completion(ctx context.Context) (float64, error) {
	close(o.entered)
	select {
	case value := <-o.measured:
		return value, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

type rtConcurrentPlayback struct{ output *rtConcurrentReceipt }

func (p rtConcurrentPlayback) Open(OutputRef) (ConversationOutput, error) { return p.output, nil }
func (p rtConcurrentPlayback) Interrupt([]OutputRef) error                { close(p.output.interrupted); return nil }
func (p rtConcurrentPlayback) Stop(context.Context) error                 { return nil }
func TestRealtimeReceiptWaitOverlapsWriteEndAndInterrupt(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	output := &rtConcurrentReceipt{entered: make(chan struct{}), ended: make(chan struct{}), interrupted: make(chan struct{}), measured: make(chan float64, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&ConversationRunner{Session: s, Source: &rtControlledSource{stopped: make(chan struct{})}, Playback: rtConcurrentPlayback{output}}).Run(ctx)
	}()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	p.send(rtObject{"type": "response.output_item.added", "response_id": "r1", "output_index": 0, "item": rtObject{"type": "message", "id": "i1"}})
	select {
	case <-output.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("receipt wait did not start")
	}
	p.send(rtObject{"type": "response.audio.delta", "response_id": "r1", "item_id": "i1", "output_index": 0, "content_index": 0, "delta": "AAA="})
	p.send(rtObject{"type": "response.audio.done", "response_id": "r1", "item_id": "i1", "output_index": 0, "content_index": 0})
	select {
	case <-output.ended:
	case <-time.After(3 * time.Second):
		t.Fatal("receipt wait blocked End")
	}
	if output.writes.Load() != 1 {
		t.Fatal("receipt wait blocked Write")
	}
	p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
	select {
	case <-output.interrupted:
	case <-time.After(3 * time.Second):
		t.Fatal("receipt wait blocked Interrupt")
	}
	// Only the device measurement releases completion; receipt waiting stayed
	// pending throughout Write, End, and Interrupt.
	output.measured <- 12
	p.next("conversation.item.truncate")
	p.send(rtObject{"type": "conversation.item.truncated", "item_id": "i1", "content_index": 0})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not finish")
	}
}
