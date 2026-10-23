package rime

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

type rtRead struct {
	data []byte
	err  error
}
type rtWrite struct {
	id     string
	frames [][]byte
	caller context.Context
	audio  bool
}
type rtWritten struct {
	id      string
	err     error
	skipped bool
}

func rtDial(ctx context.Context, address, key string) (*websocket.Conn, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	c, response, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + key}}, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		if ctx.Err() != nil {
			return nil, operationError(context.Cause(ctx), "")
		}
		kind := ErrUnavailable
		if response != nil && (response.StatusCode == 401 || response.StatusCode == 403) {
			kind = ErrAuthentication
		}
		return nil, failure(kind, "realtime handshake failed")
	}
	c.SetReadLimit(1024 * 1024)
	return c, nil
}
func rtReadLoop(ctx context.Context, c *websocket.Conn, out chan<- rtRead) {
	for {
		kind, b, e := c.Read(ctx)
		if e == nil && kind != websocket.MessageText {
			e = failure(ErrStream, "expected a JSON text event")
		}
		if e != nil {
			if ctx.Err() != nil {
				e = operationError(context.Cause(ctx), "")
			} else {
				e = failure(ErrStream, "invalid event or lost realtime connection")
			}
		}
		select {
		case out <- rtRead{b, e}:
		case <-ctx.Done():
			return
		}
		if e != nil {
			return
		}
	}
}
func rtWriteLoop(ctx context.Context, c *websocket.Conn, in <-chan rtWrite, out chan<- rtWritten, budget time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-in:
			result := rtWriteFrames(ctx, job, budget, func(ctx context.Context, data []byte) error {
				return c.Write(ctx, websocket.MessageText, data)
			})
			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

// rtWriteFrames keeps acknowledgment-bearing requests alive after caller cancellation.
// Audio has no acknowledgment: canceling an active append makes its outcome unknown.
func rtWriteFrames(ctx context.Context, job rtWrite, budget time.Duration, write func(context.Context, []byte) error) rtWritten {
	result := rtWritten{id: job.id}
	// This check is the submission boundary. Once it succeeds, only a known
	// write outcome or session failure releases the worker's matching state.
	if job.caller.Err() != nil {
		result.skipped = true
		result.err = operationError(context.Cause(job.caller), job.id)
		return result
	}
	writeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if job.audio {
		stop := context.AfterFunc(job.caller, cancel)
		defer stop()
	}
	for _, frame := range job.frames {
		if err := write(writeCtx, frame); err != nil {
			result.err = failure(ErrStream, "realtime write failed; outcome unknown")
			break
		}
		if job.audio && job.caller.Err() != nil {
			result.err = failure(ErrStream, "audio submission cancelled; outcome unknown")
			break
		}
	}
	return result
}
