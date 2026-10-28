// SPDX-License-Identifier: Apache-2.0
// Prism protocol behavior adapted from the existing SDKs; see NOTICE-PRISM.
package rime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var errRealtimeClosed = errors.New("realtime session closed")

const rtInterruptionPolicyLimit = 128

type rtInterruptionPolicy struct {
	revision  uint64
	interrupt bool
	unknown   bool
}

func realtimeClosedError() error {
	return &Error{Kind: ErrCancelled, Message: "realtime session closed", Cause: errRealtimeClosed}
}

// RealtimeSession owns one Prism connection. Consume Recv concurrently with requests.
// Close waits for the worker and both socket operations. References expire after
// 128 later responses. Overflow fails explicitly; no writes are replayed.
type RealtimeSession struct {
	normalClose          atomic.Bool
	op                   *operation
	timeouts             RealtimeTimeouts
	commands             chan *rtCommand
	wake                 chan struct{}
	events               chan SessionEvent
	initialized          chan error
	done                 chan struct{}
	receiving            atomic.Bool
	mu                   sync.Mutex
	info                 SessionInfo
	interruptionPolicies []rtInterruptionPolicy
	lastSettingsRevision *uint64
	terminal             error
}
type rtResult struct {
	history []HistoryItem
	info    SessionInfo
	ref     ResponseRef
	item    ItemRef
	err     error
}
type rtCommand struct {
	epoch                         uint64
	finished                      bool
	delivered                     atomic.Bool
	converter                     *pcmInput
	admissionDeadline             time.Time
	ctx                           context.Context
	kind                          string
	body                          rtObject
	ref                           ResponseRef
	call                          ToolCallRef
	output                        OutputRef
	audio                         AudioChunk
	report                        PlaybackFinished
	result                        chan rtResult
	stop                          func() bool
	deadline                      time.Time
	id, expected, target, itemID  string
	frames                        [][]byte
	submitted, written, abandoned bool
	accepted                      *ResponseRef
	ack                           *rtWire
	digest                        [32]byte
}
type rtResponse struct {
	ref                        ResponseRef
	done, continued, abandoned bool
	token                      string
	calls                      map[string]*[32]byte
	outputs                    map[OutputRef]bool
}
type rtWorker struct {
	epoch             uint64
	initialized       bool
	interrupt         bool
	s                 *RealtimeSession
	pending           []*rtCommand
	responses         map[string]*rtResponse
	order             []string
	ready             bool
	latest, sessionID string
	converter         *pcmInput
	sequence          uint64
	writeBusy         bool
}

func newRealtimeSession(op *operation, t RealtimeTimeouts) *RealtimeSession {
	return &RealtimeSession{op: op, timeouts: t, commands: make(chan *rtCommand, 128), wake: make(chan struct{}, 1), events: make(chan SessionEvent, 256), initialized: make(chan error, 1), done: make(chan struct{})}
}

// Info returns an independent copy of the effective settings.
func (s *RealtimeSession) Info() SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSessionInfo(s.info)
}

// setInfo records only policy changes, so ordinary prompt and tool updates do
// not discard settings needed by a reply prepared before an acknowledgment.
func (s *RealtimeSession) setInfo(info SessionInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	policies := s.interruptionPolicies
	if info.SettingsRevision != nil {
		latest := *info.SettingsRevision
		s.lastSettingsRevision = &latest
	}
	var next *rtInterruptionPolicy
	if len(policies) == 0 {
		revision := uint64(0)
		if info.SettingsRevision != nil {
			revision = *info.SettingsRevision
		}
		next = &rtInterruptionPolicy{revision: revision, interrupt: info.InterruptOnSpeech}
	} else if info.SettingsRevision == nil {
		if s.info.InterruptOnSpeech != info.InterruptOnSpeech && !policies[len(policies)-1].unknown {
			// The change happened after the last published acknowledgment, but
			// its precise revision is unknown. Do not apply an older policy there.
			revision := uint64(0)
			if s.lastSettingsRevision != nil {
				revision = *s.lastSettingsRevision + 1
			}
			next = &rtInterruptionPolicy{revision: revision, unknown: true}
		}
	} else if policies[len(policies)-1].unknown || policies[len(policies)-1].interrupt != info.InterruptOnSpeech {
		next = &rtInterruptionPolicy{revision: *info.SettingsRevision, interrupt: info.InterruptOnSpeech}
	}
	if next != nil {
		policies = append(policies, *next)
		if len(policies) > rtInterruptionPolicyLimit {
			copy(policies, policies[len(policies)-rtInterruptionPolicyLimit:])
			policies = policies[:rtInterruptionPolicyLimit]
		}
		s.interruptionPolicies = policies
	}
	s.info = info
}

func (s *RealtimeSession) interruptionPolicy(revision *uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision == nil || len(s.interruptionPolicies) == 0 {
		return s.info.InterruptOnSpeech, nil
	}
	for i := len(s.interruptionPolicies) - 1; i >= 0; i-- {
		policy := s.interruptionPolicies[i]
		if policy.revision <= *revision {
			if policy.unknown {
				return false, failure(ErrResourceLimit, "reply interruption policy has an unknown settings revision")
			}
			return policy.interrupt, nil
		}
	}
	return false, failure(ErrResourceLimit, "reply settings revision is older than retained interruption policies")
}

func cloneSessionInfo(info SessionInfo) SessionInfo {
	v := info
	if v.Truncation != nil {
		t := *v.Truncation
		v.Truncation = &t
	}
	if v.SettingsRevision != nil {
		n := *v.SettingsRevision
		v.SettingsRevision = &n
	}
	if v.Eagerness != nil {
		x := *v.Eagerness
		v.Eagerness = &x
	}
	if v.EndOfTurnTimeoutMS != nil {
		x := *v.EndOfTurnTimeoutMS
		v.EndOfTurnTimeoutMS = &x
	}
	if v.ToolResultTimeoutS != nil {
		x := *v.ToolResultTimeoutS
		v.ToolResultTimeoutS = &x
	}
	if v.ToolContinuationTimeoutS != nil {
		x := *v.ToolContinuationTimeoutS
		v.ToolContinuationTimeoutS = &x
	}
	if v.RequestID != nil {
		x := *v.RequestID
		v.RequestID = &x
	}
	v.ResponseMetadata = maps.Clone(v.ResponseMetadata)
	return v
}
func (s *RealtimeSession) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal != nil {
		return s.terminal
	}
	return realtimeClosedError()
}

// Close stops the connection and waits for cleanup. It is safe to repeat.
func (s *RealtimeSession) Close() error {
	s.normalClose.Store(true)
	s.op.cancel(context.Canceled)
	<-s.done
	return nil
}

// Recv permits one active consumer. Normal explicit closure returns io.EOF.
// Terminal failures take precedence over queued events and remain available to operations.
func (s *RealtimeSession) Recv(ctx context.Context) (SessionEvent, error) {
	if ctx == nil {
		return SessionEvent{}, failure(ErrInput, "context must not be nil")
	}
	if !s.receiving.CompareAndSwap(false, true) {
		return SessionEvent{}, failure(ErrInput, "only one event consumer is allowed")
	}
	defer s.receiving.Store(false)
	if ctx.Err() != nil {
		return SessionEvent{}, operationError(context.Cause(ctx), "")
	}
	select {
	case <-s.done:
		s.mu.Lock()
		err := s.terminal
		s.mu.Unlock()
		if err != nil {
			return SessionEvent{}, err
		}
	default:
	}
	select {
	case e, ok := <-s.events:
		if ok {
			s.mu.Lock()
			err := s.terminal
			s.mu.Unlock()
			if err != nil {
				return SessionEvent{}, err
			}
			return e, nil
		}
		s.mu.Lock()
		err := s.terminal
		s.mu.Unlock()
		if err != nil {
			return SessionEvent{}, err
		}
		return SessionEvent{}, io.EOF
	case <-ctx.Done():
		return SessionEvent{}, operationError(context.Cause(ctx), "")
	}
}
func (s *RealtimeSession) invoke(ctx context.Context, c *rtCommand) (rtResult, error) {
	if ctx == nil {
		return rtResult{}, failure(ErrInput, "context must not be nil")
	}
	if ctx.Err() != nil {
		return rtResult{}, operationError(context.Cause(ctx), "")
	}
	c.ctx = ctx
	c.result = make(chan rtResult, 1)
	result := c.result
	c.deadline = time.Now().Add(s.timeouts.Ready)
	c.admissionDeadline = c.deadline
	c.stop = context.AfterFunc(ctx, func() {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	})
	select {
	case <-s.done:
		c.stop()
		if ctx.Err() != nil {
			return rtResult{}, operationError(context.Cause(ctx), "")
		}
		return rtResult{}, s.failure()
	default:
	}
	select {
	case s.commands <- c:
	default:
		c.stop()
		return rtResult{}, failure(ErrResourceLimit, "too many realtime operations")
	}
	select {
	case r := <-result:
		if ctx.Err() != nil {
			select {
			case s.wake <- struct{}{}:
			default:
			}
			return rtResult{}, operationError(context.Cause(ctx), "")
		}
		c.delivered.Store(true)
		select {
		case s.wake <- struct{}{}:
		default:
		}
		return r, r.err
	case <-ctx.Done():
		return rtResult{}, operationError(context.Cause(ctx), "")
	case <-s.done:
		if ctx.Err() != nil {
			return rtResult{}, operationError(context.Cause(ctx), "")
		}
		return rtResult{}, s.failure()
	}
}

// Update applies supplied settings and waits for the server acknowledgment.
// Replies already started keep their settings. Cancellation after submission
// does not undo an accepted update.
func (s *RealtimeSession) Update(ctx context.Context, options RealtimeUpdateOptions) (SessionInfo, error) {
	settings, err := rtPatch(options)
	if err != nil {
		return SessionInfo{}, err
	}
	result, err := s.invoke(ctx, &rtCommand{kind: "update", body: rtObject{"session": settings}})
	return result.info, err
}

// SendText waits for admission and requests a reply to a typed user turn.
func (s *RealtimeSession) SendText(ctx context.Context, text string) (ResponseRef, error) {
	return s.SendTextWithOptions(ctx, text, ReplyRequestOptions{})
}

// SendTextWithOptions requests a typed reply with labels for this request.
func (s *RealtimeSession) SendTextWithOptions(ctx context.Context, text string, options ReplyRequestOptions) (ResponseRef, error) {
	if e := rtText(text, 4000); e != nil {
		return ResponseRef{}, e
	}
	metadata, err := rtRequestMetadata(options.Metadata, rtObject{"prism_cause": "user_text"})
	if err != nil {
		return ResponseRef{}, err
	}
	r, e := s.invoke(ctx, &rtCommand{kind: "text", body: rtObject{"response": rtObject{"prism_input_text": text, "metadata": metadata}}})
	return r.ref, e
}

// AddMessage adds history without requesting a response.
func (s *RealtimeSession) AddMessage(ctx context.Context, role, text string) (ItemRef, error) {
	if role != "user" && role != "assistant" {
		return ItemRef{}, failure(ErrInput, "history role must be user or assistant")
	}
	if e := rtText(text, 4000); e != nil {
		return ItemRef{}, e
	}
	return s.CreateItem(ctx, HistoryItem{Type: HistoryMessage, Role: role, Text: text}, "")
}
func (s *RealtimeSession) RequestReply(ctx context.Context, o ReplyOptions) (ResponseRef, error) {
	metadata, err := rtRequestMetadata(o.Metadata, rtObject{"prism_cause": "proactive"})
	if err != nil {
		return ResponseRef{}, err
	}
	b := rtObject{"metadata": metadata}
	if o.Instruction != "" {
		if e := rtText(o.Instruction, 4000); e != nil {
			return ResponseRef{}, e
		}
		b["prism_instruction"] = o.Instruction
	}
	c := &rtCommand{kind: "reply", body: rtObject{"response": b}}
	if o.ToolCall != nil {
		c.call = *o.ToolCall
		b["prism_call_id"] = c.call.callID
	}
	r, e := s.invoke(ctx, c)
	return r.ref, e
}
func (s *RealtimeSession) ContinueReply(ctx context.Context, parent ResponseRef) (ResponseRef, error) {
	return s.ContinueReplyWithOptions(ctx, parent, ReplyRequestOptions{})
}

// ContinueReplyWithOptions requests a tool continuation with labels for this reply.
func (s *RealtimeSession) ContinueReplyWithOptions(ctx context.Context, parent ResponseRef, options ReplyRequestOptions) (ResponseRef, error) {
	metadata, err := rtRequestMetadata(options.Metadata, rtObject{"prism_cause": "tool_continuation", "prism_parent_response_id": parent.responseID})
	if err != nil {
		return ResponseRef{}, err
	}
	r, e := s.invoke(ctx, &rtCommand{kind: "continue", ref: parent, body: rtObject{"response": rtObject{"metadata": metadata}}})
	return r.ref, e
}
func (s *RealtimeSession) SubmitToolResult(ctx context.Context, call ToolCallRef, output string) error {
	_, e := s.invoke(ctx, &rtCommand{kind: "tool", call: call, ref: call.ResponseRef, digest: sha256.Sum256([]byte(output)), body: rtObject{"item": rtObject{"type": "function_call_output", "call_id": call.callID, "output": output}}})
	return e
}
func (s *RealtimeSession) Cancel(ctx context.Context, ref ResponseRef) error {
	_, e := s.invoke(ctx, &rtCommand{kind: "cancel", ref: ref, body: rtObject{"response_id": ref.responseID}})
	return e
}

// SetResponseMetadata replaces the labels later replies carry; a reply already
// started keeps its own. It returns once Prism confirms the change.
func (s *RealtimeSession) SetResponseMetadata(ctx context.Context, labels map[string]string) error {
	if e := rtLabelsValid(labels); e != nil {
		return e
	}
	labels = maps.Clone(labels)
	if labels == nil {
		labels = map[string]string{}
	}
	_, e := s.invoke(ctx, &rtCommand{kind: "labels", body: rtObject{"session": rtObject{"prism_response_metadata": labels}}})
	return e
}
func (s *RealtimeSession) ClearAudio(ctx context.Context) error {
	_, e := s.invoke(ctx, &rtCommand{kind: "clear", body: rtObject{}})
	return e
}

// SendAudio converts and submits PCM16 input. Cancellation before submission sends
// nothing. Cancellation during socket submission closes the session without replay.
func (s *RealtimeSession) SendAudio(ctx context.Context, chunk AudioChunk) error {
	format, e := resolvePCMFormat(chunk.Format)
	if e != nil {
		return e
	}
	if len(chunk.Data) > 192000 || len(chunk.Data)%(2*format.Channels) != 0 {
		return failure(ErrAudioFormat, "audio must have complete PCM16 frames and at most 192000 bytes")
	}
	chunk.Format = format
	chunk.Data = append([]byte(nil), chunk.Data...)
	_, e = s.invoke(ctx, &rtCommand{kind: "audio", audio: chunk})
	return e
}
func (s *RealtimeSession) ReportPlayback(ctx context.Context, report PlaybackReport) error {
	var c *rtCommand
	switch r := report.(type) {
	case PlaybackInterrupted:
		c = &rtCommand{kind: "truncate", ref: r.Output.response, output: r.Output, body: rtObject{"item_id": r.Output.itemID, "content_index": r.Output.contentIndex, "audio_end_ms": r.AudioEndMS}}
	case PlaybackFinished:
		for _, p := range []*float64{r.PlayedMS, r.AudibleTailMS} {
			if p != nil && (math.IsNaN(*p) || math.IsInf(*p, 0) || *p < 0) {
				return failure(ErrInput, "playback times must be finite and nonnegative")
			}
		}
		if r.PlayedMS != nil {
			x := *r.PlayedMS
			r.PlayedMS = &x
		}
		if r.AudibleTailMS != nil {
			x := *r.AudibleTailMS
			r.AudibleTailMS = &x
		}
		c = &rtCommand{kind: "drain", ref: r.Response, report: r}
	default:
		return failure(ErrInput, "unknown playback report")
	}
	_, e := s.invoke(ctx, c)
	return e
}
func (s *RealtimeSession) run(address, key, requestID string, settings rtObject) {
	var terminal error
	var socketDone sync.WaitGroup
	ctx, cancel := context.WithCancel(s.op.ctx)
	defer func() {
		cancel()
		socketDone.Wait()
		s.mu.Lock()
		s.terminal = terminal
		s.mu.Unlock()
		select {
		case s.initialized <- s.failureOr(terminal):
		default:
		}
		s.op.close()
		s.op.client.mu.Lock()
		delete(s.op.client.realtimeSessions, s)
		s.op.client.mu.Unlock()
		close(s.events)
		close(s.done)
	}()
	connectionDeadline := time.Now().Add(s.timeouts.Connect)
	connectCtx, stop := context.WithDeadline(ctx, connectionDeadline)
	conn, err := rtDial(connectCtx, address, key, requestID, s.op.client.realtimeTransport)
	stop()
	if err != nil {
		terminal = err
		return
	}
	defer conn.CloseNow()
	reads := make(chan rtRead, 1)
	writes := make(chan rtWrite, 1)
	written := make(chan rtWritten, 1)
	socketDone.Add(2)
	go func() { defer socketDone.Done(); rtReadLoop(ctx, conn, reads) }()
	go func() { defer socketDone.Done(); rtWriteLoop(ctx, conn, writes, written, s.timeouts.Request) }()
	w := rtWorker{s: s, responses: map[string]*rtResponse{}, interrupt: settings["turn_detection"].(map[string]any)["interrupt_response"].(bool)}
	initDeadline := time.NewTimer(time.Until(connectionDeadline))
	defer initDeadline.Stop()
	tick := time.NewTimer(time.Hour)
	defer tick.Stop()
	defer func() {
		for _, c := range w.pending {
			w.finish(c, rtResult{err: s.failureOr(terminal)})
		}
		for {
			select {
			case c := <-s.commands:
				w.finish(c, rtResult{err: s.failureOr(terminal)})
			default:
				return
			}
		}
	}()
	for {
		if ctx.Err() != nil {
			if !s.normalClose.Load() {
				terminal = operationError(context.Cause(s.op.ctx), "")
			}
			return
		}
		if err := w.advance(writes); err != nil {
			terminal = err
			return
		}
		deadline := time.Now().Add(time.Hour)
		for _, c := range w.pending {
			if !c.finished && c.deadline.Before(deadline) {
				deadline = c.deadline
			}
		}
		if !tick.Stop() {
			select {
			case <-tick.C:
			default:
			}
		}
		tick.Reset(time.Until(deadline))

		select {
		case <-ctx.Done():
			if cause := context.Cause(s.op.ctx); !s.normalClose.Load() {
				terminal = operationError(cause, "")
			}
			return
		case <-initDeadline.C:
			if w.sessionID != "" {
				continue
			}
			terminal = failure(ErrTimeout, "realtime initialization timed out")
			return
		case <-tick.C:
		case <-s.wake:
		case c := <-s.commands:
			if len(w.pending) >= 128 {
				w.finish(c, rtResult{err: failure(ErrResourceLimit, "too many pending realtime operations")})
			} else {
				w.pending = append(w.pending, c)
			}
		case result := <-written:
			if ctx.Err() != nil {
				if !s.normalClose.Load() {
					terminal = operationError(context.Cause(s.op.ctx), "")
				}
				return
			}
			if err := w.written(result); err != nil {
				terminal = err
				return
			}
		case read := <-reads:
			if ctx.Err() != nil {
				if !s.normalClose.Load() {
					terminal = operationError(context.Cause(s.op.ctx), "")
				}
				return
			}
			if read.err != nil {
				terminal = read.err
				return
			}
			event, e := decodeRealtime(read.data, w.sessionID)
			if e != nil {
				terminal = e
				return
			}
			if event == nil {
				continue
			}
			if event.kind == "session.created" {
				if w.sessionID == "" {
					w.sessionID = event.info.SessionID
					initDeadline.Stop()
					c := &rtCommand{ctx: ctx, kind: "update", body: rtObject{"session": settings}, deadline: time.Now().Add(s.timeouts.Request), result: make(chan rtResult, 1)}
					w.pending = append(w.pending, c)
				}
				continue
			}
			if e = w.dispatch(event); e != nil {
				terminal = e
				return
			}

		}
	}
}
func (s *RealtimeSession) failureOr(e error) error {
	if e != nil {
		return e
	}
	return realtimeClosedError()
}
func (w *rtWorker) finish(c *rtCommand, r rtResult) {
	if c.kind == "update" && !w.initialized && r.err != nil {
		select {
		case w.s.initialized <- r.err:
		default:
		}
	}
	if c.result != nil {
		select {
		case c.result <- r:
		default:
		}
		c.result = nil
	}
	if c.stop != nil && c.accepted == nil {
		c.stop()
		c.stop = nil
	}
	c.finished = true
}
func (w *rtWorker) response(ref ResponseRef) (*rtResponse, error) {
	if ref.sessionID != w.sessionID {
		return nil, failure(ErrInput, "reference belongs to another session")
	}
	r := w.responses[ref.responseID]
	if r == nil {
		return nil, failure(ErrInput, "unknown or expired response reference")
	}
	return r, nil
}
func (w *rtWorker) publish(payload RealtimeEvent, eventID, requestID string) error {
	select {
	case w.s.events <- SessionEvent{w.sessionID, eventID, requestID, payload}:
		return nil
	default:
		return failure(ErrResourceLimit, "realtime event consumer is too slow")
	}
}
func (w *rtWorker) abandon(ref ResponseRef) error {
	r := w.responses[ref.responseID]
	if r == nil || r.abandoned {
		return nil
	}
	r.abandoned = true
	if e := w.publish(ResponseAbandoned{ref}, "", ""); e != nil {
		return e
	}
	if !r.done {
		if len(w.pending) >= 128 {
			return failure(ErrResourceLimit, "too many cancellation requests")
		}
		w.pending = append(w.pending, &rtCommand{ctx: w.s.op.ctx, kind: "cancel", ref: ref, body: rtObject{"response_id": ref.responseID}, deadline: time.Now().Add(w.s.timeouts.Ready)})
	}
	return nil
}
func (w *rtWorker) advance(writes chan<- rtWrite) error {
	kept := w.pending[:0]
	for _, c := range w.pending {
		if !c.finished || (c.accepted != nil && !c.delivered.Load()) {
			kept = append(kept, c)
		} else if c.stop != nil {
			c.stop()
		}
	}
	w.pending = kept
	for _, c := range w.pending {
		if c.ctx.Err() != nil {
			if !c.submitted {
				w.finish(c, rtResult{err: operationError(context.Cause(c.ctx), "")})
				continue
			}
			c.abandoned = true
			if c.accepted != nil {
				if e := w.abandon(*c.accepted); e != nil {
					return e
				}
				c.accepted = nil
				if c.stop != nil {
					c.stop()
				}
			}
		}
		if c.finished {
			continue
		}
		if time.Now().After(c.deadline) {
			if c.submitted {
				return failure(ErrTimeout, "realtime request timed out; outcome unknown")
			}
			w.finish(c, rtResult{err: failure(ErrTimeout, "realtime admission timed out")})
			continue
		}
		if c.submitted || w.writeBusy {
			continue
		}
		wait, e := w.prepare(c)
		if e != nil {
			w.finish(c, rtResult{err: e})
			continue
		}
		if wait || c.finished {
			continue
		}
		select {
		case writes <- rtWrite{id: c.id, frames: c.frames, caller: c.ctx, audio: c.kind == "audio"}:
			// Retain tracking conservatively from writer handoff. The worker cannot
			// dispatch an acknowledgment or cancellation until this transition ends.
			// Only a writer-confirmed skip can release tracking without a write.
			c.submitted = true
			c.epoch = w.epoch
			c.deadline = time.Now().Add(w.s.timeouts.Request)
			w.writeBusy = true
			if c.kind == "text" || c.kind == "reply" {
				w.ready = false
			}
			if c.kind == "continue" {
				w.responses[c.ref.responseID].continued = true
			}
		default:
		}
	}
	return nil
}
func (w *rtWorker) prepare(c *rtCommand) (bool, error) {
	// Serialize uncorrelated controls and tool results without blocking the worker.
	for _, p := range w.pending {
		if p == c {
			break
		}
		if !p.finished && (p.kind == c.kind || ((p.kind == "audio" || p.kind == "clear") && (c.kind == "audio" || c.kind == "clear"))) && (c.kind == "tool" || c.kind == "clear" || c.kind == "truncate" || c.kind == "audio" || c.kind == "cancel") {
			return true, nil
		}
	}
	wire := ""
	switch c.kind {
	case "text", "reply":
		if c.call.callID != "" {
			r, e := w.response(c.call.ResponseRef)
			if e != nil {
				return false, e
			}
			d, ok := r.calls[c.call.callID]
			if !ok || d == nil {
				return false, failure(ErrInput, "record the tool result before requesting its report")
			}
		}
		if !w.ready {
			return true, nil
		}
		wire = "response.create"
		c.expected = "response.started"
	case "continue", "cancel", "tool", "truncate", "drain":
		r, e := w.response(c.ref)
		if e != nil {
			return false, e
		}
		switch c.kind {
		case "continue":
			if w.latest != c.ref.responseID || r.continued {
				return false, failure(ErrInput, "tool round is superseded or already continued")
			}
			if !r.done {
				return true, nil
			}
			if len(r.calls) == 0 {
				return false, failure(ErrInput, "response has no tool calls")
			}
			for _, d := range r.calls {
				if d == nil {
					return true, nil
				}
			}
			wire = "response.create"
			c.expected = "response.started"
		case "cancel":
			if r.done {
				w.finish(c, rtResult{})
				return false, nil
			}
			wire = "response.cancel"
			c.expected = "response.ended"
			c.target = c.ref.responseID
		case "tool":
			d, ok := r.calls[c.call.callID]
			if !ok {
				return false, failure(ErrInput, "unknown tool call")
			}
			if d != nil {
				if *d != c.digest {
					return false, failure(ErrInput, "a recorded tool result cannot change")
				}
				w.finish(c, rtResult{})
				return false, nil
			}
			wire = "conversation.item.create"
			c.expected = "tool.result.created"
			c.target = c.call.callID
		case "truncate":
			if !r.outputs[c.output] {
				return false, failure(ErrInput, "unknown output reference")
			}
			wire = "conversation.item.truncate"
			c.expected = "item.truncated"
			c.target = c.output.itemID
		case "drain":
			if !r.done {
				return true, nil
			}
			if r.token == "" {
				w.finish(c, rtResult{})
				return false, nil
			}
			wire = "prism.playback.drained"
			c.body = rtObject{"response_id": r.ref.responseID, "drain_token": r.token, "played_ms": c.report.PlayedMS, "audible_tail_ms": c.report.AudibleTailMS}
		}
	case "history_get":
		wire = "prism.conversation.get"
		c.expected = "history.listed"
	case "history_delete":
		wire = "conversation.item.delete"
		c.expected = "item.deleted"
	case "history_result":
		wire = "conversation.item.create"
		c.expected = "tool.result.created"
	case "history":
		wire = "conversation.item.create"
		c.expected = "item.created"
	case "update", "labels":
		wire = "session.update"
		c.expected = "session.updated"
	case "clear":
		wire = "input_audio_buffer.clear"
		c.expected = "audio.cleared"
	case "audio":
		wire = "input_audio_buffer.append"
	}
	if c.id != "" {
		return false, nil
	}
	w.sequence++
	c.id = fmt.Sprintf("evt_%d", w.sequence)
	if c.kind == "audio" {
		previous := w.converter
		if previous == nil || previous.format != c.audio.Format {
			previous = newPCMInput(c.audio.Format)
		}
		next := *previous
		next.pending = append([]byte(nil), previous.pending...)
		var converted []byte
		e := next.feed(c.ctx, c.audio.Data, func(b []byte) error { converted = append(converted, b...); return nil })
		if e != nil {
			return false, e
		}
		for len(converted) > 0 {
			n := min(1280, len(converted))
			b, e := rtEncode(c.id, wire, rtObject{"audio": base64.StdEncoding.EncodeToString(converted[:n])})
			if e != nil {
				return false, e
			}
			c.frames = append(c.frames, b)
			converted = converted[n:]
		}
		c.converter = &next
	} else {
		b, e := rtEncode(c.id, wire, c.body)
		if e != nil {
			return false, e
		}
		c.frames = [][]byte{b}
	}
	return false, nil
}
func (w *rtWorker) complete(c *rtCommand, e *rtWire) error {
	if e.kind != c.expected {
		return failure(ErrStream, "unexpected request acknowledgment")
	}
	if (c.kind == "tool" || c.kind == "history_result" || c.kind == "history_delete") && e.target != c.target {
		return failure(ErrStream, "tool acknowledgment names another call")
	}
	result := rtResult{}
	switch p := e.payload.(type) {
	case ResponseStarted:
		w.epoch++
		result.ref = p.Response
		if c.ctx.Err() != nil || c.abandoned {
			if err := w.abandon(p.Response); err != nil {
				return err
			}
		}
	}
	if c.kind == "update" || c.kind == "labels" {
		result.info = cloneSessionInfo(e.info)
		if !w.initialized {
			w.initialized = true
			w.s.initialized <- nil
		}
	}
	if c.kind == "history_get" {
		result.history = e.history
	}
	if c.kind == "history" || c.kind == "history_result" {
		id := e.createdID
		if id == "" || (c.kind == "history" && c.target != "" && c.target != id) {
			return failure(ErrStream, "history acknowledgment names an invalid item")
		}
		if item, ok := c.body["item"].(rtObject); ok {
			if want, ok := item["id"].(string); ok && want != id {
				return failure(ErrStream, "history acknowledgment names another item")
			}
		}
		result.item = ItemRef{w.sessionID, id}
	}
	if c.kind == "history_result" {
		for _, response := range w.responses {
			if previous, ok := response.calls[c.target]; ok && previous == nil {
				digest := c.digest
				response.calls[c.target] = &digest
			}
		}
	}
	if c.kind == "tool" {
		d := c.digest
		w.responses[c.ref.responseID].calls[c.call.callID] = &d
	}
	if c.kind == "clear" {
		w.converter = nil
	}
	w.finish(c, result)
	return nil
}
func (w *rtWorker) applySessionInfo(c *rtCommand, e *rtWire) {
	info := e.info
	info.SessionID = w.sessionID
	if !e.interruptKnown {
		info.InterruptOnSpeech = w.interrupt
		if settings, ok := c.body["session"].(rtObject); ok {
			if detection, ok := settings["turn_detection"].(map[string]any); ok {
				if value, ok := detection["interrupt_response"].(bool); ok {
					info.InterruptOnSpeech = value
				}
			}
		}
	}
	w.interrupt = info.InterruptOnSpeech
	w.s.setInfo(info)
	e.info = info
}
func (w *rtWorker) dispatch(e *rtWire) error {
	if e.kind == "session.idle_timeout" {
		timeout := &RealtimeIdleTimeout{IdleTimeoutS: e.idleTimeoutS}
		return &Error{Kind: ErrTimeout, Message: timeout.Error(), Cause: timeout}
	}
	var matched []*rtCommand
	for _, c := range w.pending {
		if c.finished || !c.submitted {
			continue
		}
		match := e.requestID != "" && e.requestID == c.id
		if e.kind == "audio.cleared" && c.kind == "clear" {
			match = true
		}
		if e.kind == "item.truncated" && c.kind == "truncate" && c.target == e.target && e.index == 0 {
			match = true
		}
		if p, ok := e.payload.(ResponseEnded); ok && c.kind == "cancel" && c.target == p.Response.responseID {
			match = true
		}
		if match && e.kind != "error" {
			if c.kind == "text" && e.kind == "item.created" {
				c.itemID = e.target
				continue
			}
			if e.kind != c.expected {
				return failure(ErrStream, "unexpected request acknowledgment")
			}
			if c.kind == "tool" && c.target != e.target {
				return failure(ErrStream, "tool acknowledgment names another call")
			}
			matched = append(matched, c)
		}
	}
	switch p := e.payload.(type) {
	case InputReady:
		w.epoch++
		w.ready = true
	case SpeechStarted:
		w.epoch++
		w.ready = false
		w.latest = ""
	case ResponseStarted:
		policy, err := w.s.interruptionPolicy(p.SettingsRevision)
		if err != nil {
			return err
		}
		p.interruptOnSpeech = &policy
		e.payload = p
		w.epoch++
		id := p.Response.responseID
		if w.responses[id] != nil {
			return failure(ErrStream, "duplicate response.created")
		}
		if len(w.order) >= 128 {
			old := w.responses[w.order[0]]
			if !old.done || w.referencesPending(old.ref) {
				return failure(ErrResourceLimit, "too many retained active responses")
			}
			delete(w.responses, w.order[0])
			w.order = w.order[1:]
		}
		w.order = append(w.order, id)
		w.responses[id] = &rtResponse{ref: p.Response, calls: map[string]*[32]byte{}, outputs: map[OutputRef]bool{}}
		w.latest = id
		w.ready = false
	case ResponseEnded:
		r := w.responses[p.Response.responseID]
		if r == nil {
			return failure(ErrStream, "event refers to unknown response")
		}
		if r.done {
			return nil
		}
		r.done = true
		r.token = e.token
	case ToolCall:
		r := w.responses[p.Call.responseID]
		if r == nil {
			return failure(ErrStream, "unknown tool response")
		}
		if _, ok := r.calls[p.Call.callID]; ok {
			return nil
		}
		for _, other := range w.responses {
			if _, ok := other.calls[p.Call.callID]; ok {
				return failure(ErrStream, "tool call ID reused")
			}
		}
		if len(r.calls) >= 128 {
			return failure(ErrResourceLimit, "too many tool calls")
		}
		r.calls[p.Call.callID] = nil
	case MessageStarted:
		r := w.responses[p.Output.response.responseID]
		if r == nil {
			return failure(ErrStream, "unknown output response")
		}
		if len(r.outputs) >= 128 {
			return failure(ErrResourceLimit, "too many outputs")
		}
		r.outputs[p.Output] = true
	case TextDelta:
		if w.responses[p.Output.response.responseID] == nil {
			return failure(ErrStream, "unknown response")
		}
	case TextDone:
		if w.responses[p.Output.response.responseID] == nil {
			return failure(ErrStream, "unknown response")
		}
	case AudioDelta:
		if w.responses[p.Output.response.responseID] == nil {
			return failure(ErrStream, "unknown response")
		}
	case AudioDone:
		if w.responses[p.Output.response.responseID] == nil {
			return failure(ErrStream, "unknown response")
		}
	case FaultEvent:
		f := p.Fault
		if f.Scope == "session" {
			return rtFaultError(f)
		}
		busy := f.Scope == "event" && (f.Code == "typed_turn_busy" || f.Code == "instructions_busy" || f.Code == "proactive_unavailable")
		if busy {
			w.ready = false
		}
		for _, c := range w.pending {
			if c.finished {
				continue
			}
			if (c.submitted && f.RequestID != "" && c.id == f.RequestID) || (f.Code == "typed_turn_superseded" && f.ItemID != "" && c.itemID == f.ItemID) {
				if busy && (c.kind == "text" || c.kind == "reply") && c.ctx.Err() == nil {
					c.id = ""
					c.frames = nil
					c.submitted = false
					c.written = false
					c.deadline = c.admissionDeadline
				} else {
					if c.kind == "continue" && f.Code == "tool_continuation_not_ready" {
						w.responses[c.ref.responseID].continued = false
					}
					w.finish(c, rtResult{err: rtFaultError(f)})
				}
			}
		}
	}
	// Publish acceptance before a possible local abandonment event.
	if e.payload != nil {
		if err := w.publish(e.payload, e.eventID, e.requestID); err != nil {
			return err
		}
	}
	for _, c := range matched {
		if c.kind == "update" || c.kind == "labels" {
			w.applySessionInfo(c, e)
		}
		c.ack = e
		if p, ok := e.payload.(ResponseStarted); ok {
			r := p.Response
			c.accepted = &r
		}
		if c.written {
			if err := w.complete(c, e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *rtWorker) referencesPending(ref ResponseRef) bool {
	for _, c := range w.pending {
		if !c.finished && (c.ref == ref || c.call.ResponseRef == ref) {
			return true
		}
	}
	return false
}

func (w *rtWorker) written(result rtWritten) error {
	w.writeBusy = false
	for _, c := range w.pending {
		if c.id == result.id {
			if result.skipped {
				if (c.kind == "text" || c.kind == "reply") && c.epoch == w.epoch {
					w.ready = true
				}
				if c.kind == "continue" {
					w.responses[c.ref.responseID].continued = false
				}
				w.finish(c, rtResult{err: result.err})
				break
			}
			if result.err != nil {
				return result.err
			}
			c.written = true
			if c.kind == "audio" {
				w.converter = c.converter
			}
			if c.expected == "" {
				w.finish(c, rtResult{})
			} else if c.ack != nil {
				if err := w.complete(c, c.ack); err != nil {
					return err
				}
			}
			break
		}
	}
	return nil
}
