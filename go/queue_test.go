package rime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestQueueFailureDropsAudioAndStopsBlockedWriter(t *testing.T) {
	q := newAudioQueue()
	done := make(chan error, 1)
	go func() { done <- q.put(context.Background(), make([]byte, 192000)) }()
	deadline := time.Now().Add(time.Second)
	for {
		q.mu.Lock()
		full := q.bytes == 96000
		q.mu.Unlock()
		if full {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue never filled")
		}
		time.Sleep(time.Millisecond)
	}
	if !q.pending() {
		t.Fatal("pending writer not counted")
	}
	q.fail(failure(ErrCancelled, "cancelled"))
	select {
	case err := <-done:
		if !errors.Is(err, ErrCancelled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer remained blocked")
	}
	if data, err := q.get(context.Background()); len(data) != 0 || !errors.Is(err, ErrCancelled) {
		t.Fatalf("%d %v", len(data), err)
	}
}

func TestQueueCopiesFramesAndPreservesObservedSuccess(t *testing.T) {
	q := newAudioQueue()
	input := []byte{1, 0}
	if err := q.put(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	input[0] = 9
	q.finish()
	data, err := q.get(context.Background())
	if err != nil || data[0] != 1 {
		t.Fatalf("%v %v", data, err)
	}
	if _, err := q.get(context.Background()); err != io.EOF {
		t.Fatal(err)
	}
	q.fail(failure(ErrCancelled, "late cancellation"))
	if _, err := q.get(context.Background()); err != io.EOF {
		t.Fatal(err)
	}
}
