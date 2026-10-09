package rime

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestTranscriptQueueBackpressureAndCancellation(t *testing.T) {
	queue := newTranscriptQueue(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, text := range []string{"first", "replacement"} {
		if err := queue.put(ctx, TranscriptionPartial{Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		finished <- queue.put(ctx, TranscriptionFinal{Text: "replacement", Language: "en"})
	}()
	awaitSignal(t, started)
	select {
	case err := <-finished:
		t.Fatalf("full queue accepted update: %v", err)
	default:
	}
	if update, err := queue.get(ctx); err != nil || update != (TranscriptionPartial{Text: "first"}) {
		t.Fatalf("%v %v", update, err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
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
	finished := make(chan error, 1)
	go func() { finished <- queue.put(ctx, TranscriptionPartial{Text: "replacement"}) }()
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
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
