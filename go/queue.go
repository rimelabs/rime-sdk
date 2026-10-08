package rime

import (
	"context"
	"io"
	"sync"
)

// Audio is copied into bounded chunks so a small queued slice cannot retain a large RPC response.
type audioQueue struct {
	mu                 sync.Mutex
	items              [][]byte
	bytes              int
	writers            int
	finished, consumed bool
	err                error
	changed            chan struct{}
}

func newAudioQueue() *audioQueue { return &audioQueue{changed: make(chan struct{})} }
func (q *audioQueue) wake()      { close(q.changed); q.changed = make(chan struct{}) }
func (q *audioQueue) pending() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes != 0 || q.writers != 0
}
func (q *audioQueue) fail(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil || q.consumed {
		return
	}
	q.err = err
	q.items = nil
	q.bytes = 0
	q.finished = true
	q.wake()
}
func (q *audioQueue) finish() { q.mu.Lock(); defer q.mu.Unlock(); q.finished = true; q.wake() }
func (q *audioQueue) put(ctx context.Context, data []byte) error {
	q.mu.Lock()
	q.writers++
	q.mu.Unlock()
	defer func() { q.mu.Lock(); q.writers--; q.mu.Unlock() }()
	for len(data) > 0 {
		n := min(len(data), 9600)
		q.mu.Lock()
		for !q.finished && q.bytes+n > 96000 {
			changed := q.changed
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-changed:
			}
			q.mu.Lock()
		}
		if q.finished {
			err := q.err
			q.mu.Unlock()
			return err
		}
		q.items = append(q.items, append([]byte(nil), data[:n]...))
		q.bytes += n
		q.wake()
		q.mu.Unlock()
		data = data[n:]
	}
	return nil
}
func (q *audioQueue) get(ctx context.Context) ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.err != nil {
			return nil, q.err
		}
		if q.consumed {
			return nil, io.EOF
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if len(q.items) != 0 {
			item := q.items[0]
			q.items[0] = nil
			q.items = q.items[1:]
			q.bytes -= len(item)
			q.wake()
			return item, nil
		}
		if q.finished {
			q.consumed = true
			return nil, io.EOF
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		q.mu.Lock()
	}
}
