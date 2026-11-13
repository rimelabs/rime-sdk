package rime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// clientTurnsPeer echoes turn_detection null on the first update, as Prism does.
func clientTurnsPeer(t *testing.T) *realtimePeer {
	return newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test", "turn_detection": nil, "prism_effective_interrupt_response": false})
}

func clientTurnsSession(t *testing.T) (*realtimePeer, *RealtimeSession) {
	p := clientTurnsPeer(t)
	_, s := p.setupWith(t, RealtimeConnectOptions{ClientEndsTurns: true})
	p.send(rtObject{"type": "prism.typed_input.ready"})
	return p, s
}

func eventOf[T any](t *testing.T, s *RealtimeSession) (SessionEvent, T) {
	t.Helper()
	for {
		e := rtRecv(t, s)
		if payload, ok := e.Payload.(T); ok {
			return e, payload
		}
	}
}

func TestClientEndedTurnsSendNullTurnDetection(t *testing.T) {
	p, s := clientTurnsSession(t)
	settings := p.update()
	if value, ok := settings["turn_detection"]; !ok || value != nil {
		t.Fatalf("turn_detection = %v, want null", settings["turn_detection"])
	}
	info := s.Info()
	if !info.ClientEndsTurns || info.InterruptOnSpeech {
		t.Fatalf("info = %+v", info)
	}
}

func TestPrismEndsTurnsByDefault(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	if detection := p.update()["turn_detection"]; !reflect.DeepEqual(detection, map[string]any{"interrupt_response": true}) {
		t.Fatal(detection)
	}
	if s.Info().ClientEndsTurns {
		t.Fatal("default session reports client-ended turns")
	}
}

func TestClientEndedTurnsPushToTalk(t *testing.T) {
	p, s := clientTurnsSession(t)
	ctx := context.Background()
	if err := s.StartTurn(ctx); err != nil {
		t.Fatal(err)
	}
	if start := p.next("prism.input_audio_buffer.start"); len(start) != 2 {
		t.Fatal(start)
	}
	if err := s.SendAudio(ctx, AudioChunk{Data: make([]byte, 640), Format: PCMFormat{SampleRate: 16000, Channels: 1}}); err != nil {
		t.Fatal(err)
	}
	p.next("input_audio_buffer.append")
	if err := s.EndTurn(ctx); err != nil {
		t.Fatal(err)
	}
	if commit := p.next("input_audio_buffer.commit"); len(commit) != 2 {
		t.Fatal(commit)
	}
	done := make(chan ResponseRef, 1)
	go func() {
		ref, err := s.ReplyToTurns(ctx)
		if err != nil {
			t.Error(err)
		}
		done <- ref
	}()
	request := p.next("response.create")
	if !reflect.DeepEqual(request["response"], map[string]any{}) {
		t.Fatal(request)
	}
	p.accept(request, "reply-1")
	if ref := <-done; ref.ResponseID() != "reply-1" {
		t.Fatal(ref)
	}
}

func TestReplyToTurnsCarriesLabelsWithoutACause(t *testing.T) {
	p, s := clientTurnsSession(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.ReplyToTurnsWithOptions(context.Background(), ReplyRequestOptions{Metadata: map[string]string{"order": "42"}})
		done <- err
	}()
	request := p.next("response.create")
	if !reflect.DeepEqual(request["response"], map[string]any{"metadata": map[string]any{"order": "42"}}) {
		t.Fatal(request)
	}
	p.accept(request, "reply-1")
	rtWait(t, done)
}

func TestReplyToTurnsWaitsForTheActiveReply(t *testing.T) {
	p, s := clientTurnsSession(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.ReplyToTurns(context.Background())
		done <- err
	}()
	first := p.next("response.create")
	p.send(rtObject{"type": "error", "error": rtObject{"code": "conversation_already_has_active_response", "scope": "event", "owner": rtObject{"event_id": first["event_id"]}}})
	select {
	case err := <-done:
		t.Fatalf("finished before Prism was idle: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	second := p.next("response.create")
	if second["event_id"] == first["event_id"] {
		t.Fatal("reused the refused request ID")
	}
	p.accept(second, "reply-1")
	rtWait(t, done)
}

func TestEndTurnRefusalArrivesAsAnErrorEvent(t *testing.T) {
	p, s := clientTurnsSession(t)
	ctx := context.Background()
	if err := s.EndTurn(ctx); err != nil {
		t.Fatal(err)
	}
	commit := p.next("input_audio_buffer.commit")
	p.send(rtObject{"type": "error", "error": rtObject{"code": "input_audio_buffer_commit_empty", "scope": "event", "owner": rtObject{"event_id": commit["event_id"]}}})
	_, fault := eventOf[FaultEvent](t, s)
	if fault.Fault.Code != "input_audio_buffer_commit_empty" || fault.Fault.RequestID != commit["event_id"] {
		t.Fatal(fault.Fault)
	}
	if err := s.EndTurn(ctx); err != nil {
		t.Fatal(err)
	}
	p.next("input_audio_buffer.commit")
}

func TestEndTurnAlsoEndsATurnPrismDetects(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	if err := s.EndTurn(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.next("input_audio_buffer.commit")
}

func TestClientCommitIsAKnownEndReason(t *testing.T) {
	p, s := clientTurnsSession(t)
	p.send(rtObject{"type": "input_audio_buffer.speech_stopped", "item_id": "speech", "audio_end_ms": 1200, "prism_end_reason": "client_commit"})
	if _, stopped := eventOf[SpeechStopped](t, s); stopped.EndReason != "client_commit" {
		t.Fatal(stopped)
	}
}

func TestTurnStartsAndRepliesNeedClientEndedTurns(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	ctx := context.Background()
	if err := s.StartTurn(ctx); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := s.ReplyToTurns(ctx); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	select {
	case request := <-p.requests:
		t.Fatalf("sent %v", request)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClientEndedTurnsRefuseTurnSettings(t *testing.T) {
	off, words := false, 2
	for _, options := range []RealtimeConnectOptions{
		{ClientEndsTurns: true, TurnDetection: TurnDetection{Eagerness: "low"}},
		{ClientEndsTurns: true, TurnDetection: TurnDetection{InterruptMinWords: &words}},
		{ClientEndsTurns: true, InterruptOnSpeech: &off},
	} {
		if _, _, _, err := rtSettings(options); !errors.Is(err, ErrInput) {
			t.Fatalf("%+v: %v", options, err)
		}
	}
}

func TestClientEndedTurnsRefuseLaterTurnSettings(t *testing.T) {
	p, s := clientTurnsSession(t)
	on, words := true, 2
	for _, options := range []RealtimeUpdateOptions{
		{InterruptOnSpeech: &on},
		{TurnDetection: &TurnDetectionUpdate{InterruptMinWords: &words}},
	} {
		if _, err := s.Update(context.Background(), options); !errors.Is(err, ErrInput) {
			t.Fatalf("%+v: %v", options, err)
		}
	}
	select {
	case request := <-p.requests:
		t.Fatalf("sent %v", request)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRunnerCutsPlaybackAtATurnStartWhenTheClientEndsTurns(t *testing.T) {
	p, s := clientTurnsSession(t)
	if info := s.Info(); !info.ClientEndsTurns || info.InterruptOnSpeech {
		t.Fatalf("info = %+v", info)
	}
	source := &rtControlledSource{stopped: make(chan struct{})}
	playback := &rtControlledPlayback{opened: make(chan *rtControlledOutput, 4), outputs: map[OutputRef]*rtControlledOutput{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&ConversationRunner{Session: s, Source: source, Playback: playback}).Run(ctx)
	}()
	p.send(rtObject{"type": "response.created", "response": rtObject{"id": "r1"}})
	rtEmitOutput(p, "r1", "i1")
	rtOutput(t, playback)
	// Prism announces the turn that StartTurn began, and cuts the reply off.
	p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "speech", "audio_start_ms": 0})
	if q := p.next("conversation.item.truncate"); q["item_id"] != "i1" {
		t.Fatal(q)
	}
	if playback.interrupts.Load() != 1 {
		t.Fatal("playback was not interrupted")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestReplyToTurnsWaitsForReadinessAfterTheTurn(t *testing.T) {
	// Prism's own order: the turn's start clears readiness, and its end restores it.
	p, s := clientTurnsSession(t)
	ctx := context.Background()
	if err := s.StartTurn(ctx); err != nil {
		t.Fatal(err)
	}
	p.next("prism.input_audio_buffer.start")
	p.send(rtObject{"type": "input_audio_buffer.speech_started", "item_id": "turn-1", "audio_start_ms": 0})
	if err := s.SendAudio(ctx, AudioChunk{Data: make([]byte, 640), Format: PCMFormat{SampleRate: 16000, Channels: 1}}); err != nil {
		t.Fatal(err)
	}
	p.next("input_audio_buffer.append")
	if err := s.EndTurn(ctx); err != nil {
		t.Fatal(err)
	}
	p.next("input_audio_buffer.commit")
	p.send(rtObject{"type": "input_audio_buffer.speech_stopped", "item_id": "turn-1", "audio_end_ms": 40, "prism_end_reason": "client_commit"})
	p.send(rtObject{"type": "input_audio_buffer.committed", "item_id": "turn-1"})
	eventOf[InputCommitted](t, s)
	done := make(chan ResponseRef, 1)
	go func() {
		ref, err := s.ReplyToTurns(ctx)
		if err != nil {
			t.Error(err)
		}
		done <- ref
	}()
	select {
	case request := <-p.requests:
		t.Fatalf("the reply request went out before Prism was ready: %v", request)
	case <-time.After(50 * time.Millisecond):
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	request := p.next("response.create")
	if !reflect.DeepEqual(request["response"], map[string]any{}) {
		t.Fatal(request)
	}
	p.accept(request, "reply-1")
	if ref := <-done; ref.ResponseID() != "reply-1" {
		t.Fatal(ref)
	}
}

func TestTurnControlsFollowAudioSentBeforeThem(t *testing.T) {
	for _, c := range []struct {
		kind string
		call func(*RealtimeSession, context.Context) error
	}{
		{"prism.input_audio_buffer.start", (*RealtimeSession).StartTurn},
		{"input_audio_buffer.commit", (*RealtimeSession).EndTurn},
	} {
		t.Run(c.kind, func(t *testing.T) {
			p, s := clientTurnsSession(t)
			ctx := context.Background()
			// A clear waiting for its acknowledgment holds back the audio sent after it.
			cleared := make(chan error, 1)
			go func() { cleared <- s.ClearAudio(ctx) }()
			p.next("input_audio_buffer.clear")
			sent := make(chan error, 1)
			go func() {
				sent <- s.SendAudio(ctx, AudioChunk{Data: make([]byte, 3840), Format: PCMFormat{SampleRate: 16000, Channels: 1}})
			}()
			time.Sleep(10 * time.Millisecond)
			controlled := make(chan error, 1)
			go func() { controlled <- c.call(s, ctx) }()
			select {
			case request := <-p.requests:
				t.Fatalf("the turn control overtook audio sent before it: %v", request)
			case <-time.After(50 * time.Millisecond):
			}
			p.send(rtObject{"type": "input_audio_buffer.cleared"})
			rtWait(t, cleared)
			rtWait(t, sent)
			rtWait(t, controlled)
			for range 3 { // 120 ms of audio is three 40 ms appends.
				p.next("input_audio_buffer.append")
			}
			p.next(c.kind)
		})
	}
}
