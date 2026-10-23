package rime

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// RealtimeService connects to Prism. It does not use TTS model or timeout defaults.
type RealtimeService struct{ client *Client }

func rtText(s string, limit int) error {
	if !utf8.ValidString(s) || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > limit {
		return failure(ErrInput, "text must be valid, nonblank UTF-8 within the character limit")
	}
	return nil
}
func rtTools(tools []ToolDefinition) ([]any, error) {
	ts := []any{}
	names := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		if rtText(tool.Name, 4000) != nil || tool.Parameters == nil {
			return nil, failure(ErrInput, "tools require a name and parameters object")
		}
		if !utf8.ValidString(tool.Description) {
			return nil, failure(ErrInput, "tool descriptions must be valid UTF-8")
		}
		if _, exists := names[tool.Name]; exists {
			return nil, failure(ErrInput, "tool names must be unique")
		}
		names[tool.Name] = struct{}{}
		v := rtObject{"type": "function", "function": rtObject{"name": tool.Name, "description": tool.Description, "parameters": tool.Parameters}}
		if f := tool.PrismFiller; f != nil {
			valid := false
			switch f.Type {
			case "none":
				valid = f.Text == "" && f.Instructions == ""
			case "text":
				valid = f.Instructions == "" && rtText(f.Text, 500) == nil
			case "auto":
				valid = f.Text == "" && (f.Instructions == "" || rtText(f.Instructions, 500) == nil)
			}
			if !valid {
				return nil, failure(ErrInput, "invalid tool filler")
			}
			v["prism_filler"] = f
		}
		ts = append(ts, v)
	}
	return ts, nil
}

func rtPatch(o RealtimeUpdateOptions) (rtObject, error) {
	settings := rtObject{}
	for k, v := range map[string]*string{"voice": o.Voice, "instructions": o.Instructions} {
		if v != nil {
			if !utf8.ValidString(*v) {
				return nil, failure(ErrInput, "invalid "+k)
			}
			settings[k] = *v
		}
	}
	if o.Tools != nil {
		tools, err := rtTools(o.Tools)
		if err != nil {
			return nil, err
		}
		settings["tools"] = tools
	}
	if o.InterruptOnSpeech != nil {
		settings["turn_detection"] = rtObject{"interrupt_response": *o.InterruptOnSpeech}
	}
	b, err := json.Marshal(settings)
	if err != nil {
		return nil, failure(ErrInput, "invalid tool schema")
	}
	result, err := rtJSONObject(b)
	if err != nil {
		return nil, failure(ErrInput, "invalid tool schema")
	}
	return result, nil
}

// rtLabelsValid refuses labels that JSON would silently alter.
func rtLabelsValid(labels map[string]string) error {
	for k, v := range labels {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return failure(ErrInput, "response metadata must be valid UTF-8")
		}
	}
	return nil
}
func rtSettings(o RealtimeConnectOptions) (string, rtObject, RealtimeTimeouts, error) {
	t := o.Timeouts
	for _, p := range []*time.Duration{&t.Connect, &t.Ready, &t.Request} {
		if *p < 0 {
			return "", nil, t, failure(ErrInput, "timeouts must be positive")
		}
	}
	if t.Connect == 0 {
		t.Connect = 10 * time.Second
	}
	if t.Ready == 0 {
		t.Ready = 30 * time.Second
	}
	if t.Request == 0 {
		t.Request = 10 * time.Second
	}
	address := o.Endpoint
	if address == "" {
		address = "wss://prism.api.rime.ai/v1/realtime"
	}
	u, e := url.Parse(address)
	if e != nil || u.Hostname() == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || strings.TrimRight(u.Path, "/") != "/v1/realtime" {
		return "", nil, t, failure(ErrInput, "endpoint must be a ws or wss URL ending in /v1/realtime")
	}
	if port := u.Port(); port != "" {
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return "", nil, t, failure(ErrInput, "invalid endpoint port")
		}
	}
	if u.Scheme == "ws" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return "", nil, t, failure(ErrInput, "use wss outside localhost")
	}
	interrupt := true
	if o.InterruptOnSpeech != nil {
		interrupt = *o.InterruptOnSpeech
	}
	d := rtObject{"interrupt_response": interrupt}
	if s := o.TurnDetection.Eagerness; s != "" {
		if rtEnum(s, "low", "medium", "high", "auto") == "unknown" {
			return "", nil, t, failure(ErrInput, "invalid turn eagerness")
		}
		d["eagerness"] = s
	}
	if n := o.TurnDetection.EndOfTurnTimeoutMS; n != 0 {
		if n < 1000 || n > 10000 {
			return "", nil, t, failure(ErrInput, "turn timeout must be 1000 to 10000 ms")
		}
		d["prism_end_of_turn_timeout_ms"] = n
	}
	ts, e := rtTools(o.Tools)
	if e != nil {
		return "", nil, t, e
	}
	if id := o.RequestID; id != "" && (strings.TrimSpace(id) == "" || strings.IndexFunc(id, func(r rune) bool { return r < 0x20 || r > 0x7e }) >= 0) {
		return "", nil, t, failure(ErrInput, "request ID must be nonblank printable ASCII")
	}
	settings := rtObject{"modalities": []string{"text", "audio"}, "input_audio_format": "pcm16", "turn_detection": d, "tools": ts}
	if e := rtLabelsValid(o.ResponseMetadata); e != nil {
		return "", nil, t, e
	}
	if o.ResponseMetadata != nil {
		settings["prism_response_metadata"] = o.ResponseMetadata
	}
	for k, v := range map[string]string{"voice": o.Voice, "instructions": o.Instructions} {
		if v != "" {
			if !utf8.ValidString(v) || strings.TrimSpace(v) == "" {
				return "", nil, t, failure(ErrInput, "invalid "+k)
			}
			settings[k] = v
		}
	}
	// Snapshot nested schemas before any network operation.
	b, e := json.Marshal(settings)
	if e != nil {
		return "", nil, t, failure(ErrInput, "invalid tool schema")
	}
	settings, e = rtJSONObject(b)
	if e != nil {
		return "", nil, t, failure(ErrInput, "invalid tool schema")
	}
	return address, settings, t, nil
}

// Connect authenticates, waits for session.created, and applies settings.
// ctx owns the connection lifetime. Each later operation has its own context.
func (r *RealtimeService) Connect(ctx context.Context, options RealtimeConnectOptions) (*RealtimeSession, error) {
	if ctx == nil {
		return nil, failure(ErrInput, "context must not be nil")
	}
	address, settings, t, err := rtSettings(options)
	if err != nil {
		return nil, err
	}
	zero := time.Duration(0)
	op, err := r.client.operation(ctx, &zero, 0)
	if err != nil {
		return nil, err
	}
	s := newRealtimeSession(op, t)
	r.client.mu.Lock()
	if r.client.closed {
		r.client.mu.Unlock()
		op.close()
		return nil, failure(ErrCancelled, "client is closed")
	}
	if r.client.realtimeSessions == nil {
		r.client.realtimeSessions = map[*RealtimeSession]struct{}{}
	}
	r.client.realtimeSessions[s] = struct{}{}
	key := r.client.key
	r.client.mu.Unlock()
	go s.run(address, key, options.RequestID, settings)
	return s.awaitInitialization(ctx)
}

func (s *RealtimeSession) awaitInitialization(ctx context.Context) (*RealtimeSession, error) {
	select {
	case result := <-s.initialized:
		// Initialization and cancellation can both be ready. Apply the same
		// cancellation precedence as operation result delivery.
		if ctx.Err() != nil {
			_ = s.Close()
			return nil, operationError(context.Cause(ctx), "")
		}
		if result != nil {
			_ = s.Close()
			return nil, result
		}
		return s, nil
	case <-ctx.Done():
		_ = s.Close()
		return nil, operationError(context.Cause(ctx), "")
	}
}
