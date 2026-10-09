package rime

import (
	"context"
	"io"
	"sync"
)

// Transcript snapshots are atomic queue entries, bounded by count rather than
// audio bytes. Cancellation discards queued updates, including an unread final.
type transcriptQueue struct {
	mu       sync.Mutex
	items    []TranscriptionUpdate
	limit    int
	err      error
	consumed bool
	changed  chan struct{}
}

func newTranscriptQueue(limit int) *transcriptQueue {
	return &transcriptQueue{limit: limit, changed: make(chan struct{})}
}
func (queue *transcriptQueue) wake() { close(queue.changed); queue.changed = make(chan struct{}) }
func (queue *transcriptQueue) fail(err error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.err != nil || queue.consumed {
		return
	}
	queue.err, queue.items = err, nil
	queue.wake()
}
func (queue *transcriptQueue) put(ctx context.Context, item TranscriptionUpdate) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for {
		if queue.err != nil {
			return queue.err
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if len(queue.items) < queue.limit {
			queue.items = append(queue.items, item)
			queue.wake()
			return nil
		}
		changed := queue.changed
		queue.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		queue.mu.Lock()
	}
}
func (queue *transcriptQueue) get(ctx context.Context) (TranscriptionUpdate, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for {
		if queue.err != nil {
			return nil, queue.err
		}
		if queue.consumed {
			return nil, io.EOF
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if len(queue.items) != 0 {
			item := queue.items[0]
			queue.items[0] = nil
			queue.items = queue.items[1:]
			if _, final := item.(TranscriptionFinal); final {
				queue.consumed = true
			}
			queue.wake()
			return item, nil
		}
		changed := queue.changed
		queue.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		queue.mu.Lock()
	}
}
