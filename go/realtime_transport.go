package rime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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

func rtDial(ctx context.Context, address, key, requestID string, transport http.RoundTripper) (*websocket.Conn, error) {
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	header := http.Header{"Authorization": []string{"Bearer " + key}}
	if requestID != "" {
		header.Set("x-request-id", requestID)
	}
	c, response, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, HTTPHeader: header, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		if ctx.Err() != nil {
			return nil, operationError(context.Cause(ctx), "")
		}
		if response != nil {
			return nil, rtHandshakeError(response)
		}
		return nil, failure(ErrUnavailable, "realtime handshake failed")
	}
	c.SetReadLimit(1024 * 1024)
	return c, nil
}

func rtHandshakeError(response *http.Response) *Error {
	kind, reason := ErrUnavailable, "realtime endpoint rejected the connection"
	switch response.StatusCode {
	case 401, 403:
		kind, reason = ErrAuthentication, "realtime endpoint rejected credentials"
	case 429:
		kind, reason = ErrResourceLimit, "realtime connection or rate limit reached"
	case 503:
		reason = "realtime endpoint is unavailable"
	}
	message := fmt.Sprintf("%s (HTTP %d)", reason, response.StatusCode)
	if response.Body != nil {
		defer response.Body.Close()
		// websocket.Dial returns at most 1024 bytes after a bounded body read.
		body, err := io.ReadAll(io.LimitReader(response.Body, 1024))
		if err == nil && len(body) < 1024 && utf8.Valid(body) {
			detail := strings.TrimSpace(string(body))
			if strings.HasPrefix(detail, "{") || strings.HasPrefix(detail, "[") {
				var envelope struct {
					Error struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				if json.Unmarshal(body, &envelope) == nil {
					detail = envelope.Error.Message
				} else {
					detail = ""
				}
			}
			invalid := strings.ContainsFunc(detail, func(c rune) bool {
				return c < 32 && c != '\r' && c != '\n' && c != '\t'
			})
			if !invalid && !strings.HasPrefix(strings.TrimSpace(detail), "<") {
				detail = strings.Join(strings.Fields(detail), " ")
				if detail != "" {
					message += ": " + detail
				}
			}
		}
	}
	return &Error{Kind: kind, Message: message, RequestID: response.Header.Get("x-request-id"), Cause: &connectionError{
		statusCode: &response.StatusCode, retryAfter: rtRetryAfter(response.Header.Values("Retry-After")),
	}}
}

func rtRetryAfter(values []string) *time.Duration {
	if len(values) != 1 || len(values[0]) > 256 {
		return nil
	}
	value := strings.TrimSpace(values[0])
	var delay time.Duration
	if value != "" && !strings.ContainsFunc(value, func(c rune) bool { return c < '0' || c > '9' }) {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64((1<<63-1)/time.Second) {
			return nil
		}
		delay = time.Duration(seconds) * time.Second
	} else {
		date, err := http.ParseTime(value)
		if err != nil {
			return nil
		}
		delay = max(0, time.Until(date))
	}
	return &delay
}

func rtStreamError(message string, cause error) *Error {
	err := failure(ErrStream, message)
	code := int(websocket.CloseStatus(cause))
	if code >= 1000 && code != 1005 && code != 1006 {
		err.Cause = &connectionError{closeCode: &code}
	}
	return err
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
				e = rtStreamError("invalid event or lost realtime connection", e)
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
			result.err = rtStreamError("realtime write failed; outcome unknown", err)
			break
		}
		if job.audio && job.caller.Err() != nil {
			result.err = failure(ErrStream, "audio submission cancelled; outcome unknown")
			break
		}
	}
	return result
}
