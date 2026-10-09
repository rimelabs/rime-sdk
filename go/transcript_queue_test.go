package rime

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// Done is evaluated when put reaches its full-queue select, after unlocking.
type transcriptWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *transcriptWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestTranscriptQueueBackpressureAndCancellation(t *testing.T) {
	queue := newTranscriptQueue(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, text := range []string{"first", "replacement"} {
		if err := queue.put(ctx, TranscriptionPartial{Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	waiting := &transcriptWaitContext{Context: ctx, waiting: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		finished <- queue.put(waiting, TranscriptionFinal{Text: "replacement", Language: "en"})
	}()
	awaitSignal(t, waiting.waiting)
	select {
	case err := <-finished:
		t.Fatalf("full queue accepted update: %v", err)
	default:
	}
	if update, err := queue.get(ctx); err != nil || update != (TranscriptionPartial{Text: "first"}) {
		t.Fatalf("%v %v", update, err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not resume after queue space was available")
	}
	queue.fail(ErrCancelled)
	if update, err := queue.get(ctx); update != nil || !errors.Is(err, ErrCancelled) {
		t.Fatalf("queued final survived cancellation: %v %v", update, err)
	}
}

func TestTranscriptQueueCancellationWakesProducer(t *testing.T) {
	queue := newTranscriptQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := queue.put(ctx, TranscriptionPartial{Text: "partial"}); err != nil {
		t.Fatal(err)
	}
	waiting := &transcriptWaitContext{Context: ctx, waiting: make(chan struct{})}
	finished := make(chan error, 1)
	go func() { finished <- queue.put(waiting, TranscriptionPartial{Text: "replacement"}) }()
	awaitSignal(t, waiting.waiting)
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not resume after cancellation")
	}
}

func TestTranscriptQueueFinalConsumptionIsTerminal(t *testing.T) {
	queue := newTranscriptQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	if err := queue.put(ctx, TranscriptionFinal{Language: "en"}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.get(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	queue.fail(ErrCancelled)
	if _, err := queue.get(ctx); err != io.EOF {
		t.Fatal(err)
	}
}
