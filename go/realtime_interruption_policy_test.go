package rime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func rtUpdateInterruption(t *testing.T, p *realtimePeer, s *RealtimeSession, revision uint64, enabled bool) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := s.Update(context.Background(), RealtimeUpdateOptions{InterruptOnSpeech: &enabled})
		done <- err
	}()
	q := p.next("session.update")
	p.send(rtObject{"type": "session.updated", "prism_request_event_id": q["event_id"],
		"session": rtObject{"id": "session-1", "voice": "test", "prism_settings_revision": revision, "prism_effective_interrupt_response": enabled}})
	rtWait(t, done)
}

func rtAwaitRunnerEvent(t *testing.T, events <-chan SessionEvent, matches func(RealtimeEvent) bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case e := <-events:
			if matches(e.Payload) {
				return
			}
		case <-deadline.C:
			t.Fatal("runner did not process expected event")
		}
	}
}

func TestRealtimeRunnerRetainsReplyInterruptionPolicy(t *testing.T) {
	for _, scenario := range []struct {
		name                string
		initial, updated    bool
		change, late, ended bool
	}{
		{name: "initial false with revision and delayed device", initial: false, ended: true},
		{name: "true reply remains interruptible after disabling", initial: true, updated: false, change: true},
		{name: "false reply remains protected after enabling", initial: false, updated: true, change: true, ended: true},
		{name: "late true early guess after disabling", initial: true, updated: false, change: true, late: true},
		{name: "late false early guess after enabling", initial: false, updated: true, change: true, late: true, ended: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			p := newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test", "prism_settings_revision": 1, "prism_effective_interrupt_response": scenario.initial})
			_, s := p.setup(t)
			if scenario.change && scenario.late {
				rtUpdateInterruption(t, p, s, 2, scenario.updated)
			}
			source := &rtControlledSource{stopped: make(chan struct{})}
			playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seen := make(chan SessionEvent, 32)
			done := make(chan error, 1)
			go func() {
				done <- (&ConversationRunner{Session: s, Source: source, Playback: playback, OnEvent: func(e SessionEvent) error { seen <- e; return nil }}).Run(ctx)
			}()
			p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1", "prism_settings_revision": 1}})
			rtEmitOutput(p, "r1", "i1")
			o := rtOutput(t, playback)
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(AudioDelta); return ok })
			if scenario.ended {
				// Generation and the server's playback estimate may finish before
				// the physical output reports its measured receipt.
				p.ended("r1")
				rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(ResponseEnded); return ok })
			}
			if scenario.change && !scenario.late {
				rtUpdateInterruption(t, p, s, 2, scenario.updated)
			}
			p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(SpeechStarted); return ok })
			if got := playback.interrupts.Load(); (got != 0) != scenario.initial {
				t.Fatalf("interrupt calls=%d; reply policy=%v", got, scenario.initial)
			}
			if scenario.initial {
				q := p.next("conversation.item.truncate")
				p.send(rtObject{"type": "conversation.item.truncated", "item_id": q["item_id"], "audio_end_ms": 12, "content_index": 0})
			} else if o.writes.Load() != 1 {
				t.Fatal("protected playback lost audio")
			}
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, ErrCancelled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("runner did not stop")
			}
		})
	}
}

func TestRealtimeInterruptionPolicyHistoryAndDispatchCapture(t *testing.T) {
	s := &RealtimeSession{events: make(chan SessionEvent, 4)}
	revision := uint64(1)
	s.setInfo(SessionInfo{InterruptOnSpeech: false, SettingsRevision: &revision})
	for revision = 2; revision < 300; revision++ {
		copy := revision
		s.setInfo(SessionInfo{InterruptOnSpeech: false, SettingsRevision: &copy})
	}
	original := uint64(1)
	if enabled, err := s.interruptionPolicy(&original); err != nil || enabled {
		t.Fatal(enabled, err)
	}
	if len(s.interruptionPolicies) != 1 {
		t.Fatal("unchanged policies consumed history")
	}
	w := rtWorker{s: s, sessionID: "session-1", responses: map[string]*rtResponse{}}
	old := &rtWire{kind: "response.started", payload: ResponseStarted{Response: ResponseRef{"session-1", "captured"}, SettingsRevision: &original}}
	if err := w.dispatch(old); err != nil {
		t.Fatal(err)
	}
	captured := (<-s.events).Payload.(ResponseStarted)
	if captured.interruptOnSpeech == nil || *captured.interruptOnSpeech {
		t.Fatal("did not capture original false policy")
	}
	for i := uint64(0); i < 128; i++ {
		next := uint64(300) + i
		s.setInfo(SessionInfo{InterruptOnSpeech: i%2 == 0, SettingsRevision: &next})
	}
	if len(s.interruptionPolicies) != 128 {
		t.Fatal(len(s.interruptionPolicies))
	}
	if *captured.interruptOnSpeech {
		t.Fatal("queued event changed after policy eviction")
	}
	if _, err := s.interruptionPolicy(&original); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("guessed evicted policy", err)
	}
	stale := &rtWire{kind: "response.started", payload: ResponseStarted{Response: ResponseRef{"session-1", "stale"}, SettingsRevision: &original}}
	if err := w.dispatch(stale); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("stale response did not fail explicitly", err)
	}
	if enabled, err := s.interruptionPolicy(nil); err != nil || enabled != s.Info().InterruptOnSpeech {
		t.Fatal("missing revision did not use acknowledged setting", enabled, err)
	}
}

func TestRealtimeResponseCapturesAcknowledgedPolicyBeforeWriterCompletion(t *testing.T) {
	s := &RealtimeSession{events: make(chan SessionEvent, 4)}
	first := uint64(1)
	s.setInfo(SessionInfo{InterruptOnSpeech: true, SettingsRevision: &first})
	command := &rtCommand{kind: "update", expected: "session.updated", id: "evt_1", submitted: true, body: rtObject{"session": rtObject{}}}
	w := rtWorker{s: s, initialized: true, interrupt: true, sessionID: "session-1", pending: []*rtCommand{command}, responses: map[string]*rtResponse{}}
	second := uint64(2)
	acknowledgment := &rtWire{kind: "session.updated", requestID: "evt_1", interruptKnown: true, info: SessionInfo{InterruptOnSpeech: false, SettingsRevision: &second}}
	if err := w.dispatch(acknowledgment); err != nil {
		t.Fatal(err)
	}
	response := &rtWire{kind: "response.started", payload: ResponseStarted{Response: ResponseRef{"session-1", "r1"}, SettingsRevision: &second}}
	if err := w.dispatch(response); err != nil {
		t.Fatal(err)
	}
	captured := (<-s.events).Payload.(ResponseStarted)
	if captured.interruptOnSpeech == nil || *captured.interruptOnSpeech {
		t.Fatalf("new response captured stale policy: %+v", captured)
	}
}

func TestRealtimeSpeechPolicySelectsLatestUnfinishedPlayback(t *testing.T) {
	old := &rtPlayReply{interruptOnSpeech: false, ended: true, outputs: []*rtPlayOutput{{measured: false}}}
	newest := &rtPlayReply{interruptOnSpeech: true, ended: true, outputs: []*rtPlayOutput{{measured: false}}}
	replies := []*rtPlayReply{old, newest}
	if !rtSpeechInterrupts(replies) {
		t.Fatal("latest unmeasured playback did not control interruption")
	}
	newest.outputs[0].measured = true
	if rtSpeechInterrupts(replies) {
		t.Fatal("measured playback hid an older protected reply")
	}
	newest.ended = false
	if !rtSpeechInterrupts(replies) {
		t.Fatal("generating reply did not control interruption")
	}
	newest.interrupted = true
	if rtSpeechInterrupts(replies) {
		t.Fatal("interrupted reply hid an older protected reply")
	}
	old.outputs[0].measured = true
	if rtSpeechInterrupts(replies) {
		t.Fatal("fully measured playback was eligible for interruption")
	}
}

func TestRealtimeLatestReplyPolicyControlsPlaybackBatch(t *testing.T) {
	for _, latest := range []bool{false, true} {
		t.Run(map[bool]string{false: "protected batch", true: "interruptible batch"}[latest], func(t *testing.T) {
			p := newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test", "prism_settings_revision": 1, "prism_effective_interrupt_response": !latest})
			_, s := p.setup(t)
			source := &rtControlledSource{stopped: make(chan struct{})}
			playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seen := make(chan SessionEvent, 32)
			done := make(chan error, 1)
			go func() {
				done <- (&ConversationRunner{Session: s, Source: source, Playback: playback, OnEvent: func(e SessionEvent) error { seen <- e; return nil }}).Run(ctx)
			}()
			p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1", "prism_settings_revision": 1}})
			rtEmitOutput(p, "r1", "i1")
			rtOutput(t, playback)
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(AudioDelta); return ok })
			rtUpdateInterruption(t, p, s, 2, latest)
			p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r2", "prism_settings_revision": 2}})
			rtEmitOutput(p, "r2", "i2")
			rtOutput(t, playback)
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(AudioDelta); return ok })
			p.ended("r2")
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(ResponseEnded); return ok })
			p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
			rtAwaitRunnerEvent(t, seen, func(e RealtimeEvent) bool { _, ok := e.(SpeechStarted); return ok })
			if got := playback.interrupts.Load(); (got != 0) != latest {
				t.Fatalf("interrupt calls=%d; latest policy=%v", got, latest)
			}
			if latest {
				for i := 0; i < 2; i++ {
					q := p.next("conversation.item.truncate")
					p.send(rtObject{"type": "conversation.item.truncated", "item_id": q["item_id"], "audio_end_ms": 12, "content_index": 0})
				}
			}
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, ErrCancelled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("runner did not stop")
			}
		})
	}
}
