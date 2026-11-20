package rime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

var rtKubectl = PronunciationEntry{Spelling: "kubectl", Pronunciation: `" k u b . k @ n . % t r\ oU l`}

func TestRealtimeContextTermsAndLexiconAreSentAndReadBack(t *testing.T) {
	kubectlWire := rtObject{"spelling": rtKubectl.Spelling, "pronunciation": rtKubectl.Pronunciation}
	p := newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test",
		"context_terms": []any{"Skyvale", "Doctor Chen"}, "custom_lexicon": []any{kubectlWire}})
	_, s := p.setupWith(t, RealtimeConnectOptions{
		ContextTerms: []string{"Skyvale", "Doctor Chen"}, CustomLexicon: []PronunciationEntry{rtKubectl}})
	sent := p.update()
	if !reflect.DeepEqual(sent["context_terms"], []any{"Skyvale", "Doctor Chen"}) || !reflect.DeepEqual(sent["custom_lexicon"], []any{kubectlWire}) {
		t.Fatalf("unexpected initial settings: %v", sent)
	}
	info := s.Info()
	if !reflect.DeepEqual(info.ContextTerms, []string{"Skyvale", "Doctor Chen"}) || !reflect.DeepEqual(info.CustomLexicon, []PronunciationEntry{rtKubectl}) {
		t.Fatalf("unexpected confirmed settings: %#v", info)
	}
	info.CustomLexicon[0].Spelling = "changed"
	if s.Info().CustomLexicon[0].Spelling != rtKubectl.Spelling {
		t.Fatal("returned lexicon aliases stored settings")
	}

	results := make(chan SessionInfo, 1)
	done := make(chan error, 1)
	go func() {
		info, err := s.Update(context.Background(), RealtimeUpdateOptions{ContextTerms: []string{"Velkora"}, CustomLexicon: []PronunciationEntry{}})
		results <- info
		done <- err
	}()
	q := p.next("session.update")
	if want := (rtObject{"context_terms": []any{"Velkora"}, "custom_lexicon": []any{}}); !reflect.DeepEqual(q["session"], want) {
		t.Fatalf("got %v, want %v", q["session"], want)
	}
	p.send(rtObject{"type": "session.updated", "prism_request_event_id": q["event_id"],
		"session": rtObject{"id": "session-1", "voice": "test", "context_terms": []any{"Velkora"}, "custom_lexicon": []any{}}})
	rtWait(t, done)
	info = <-results
	// An empty list Prism confirmed is not nil, which means not published.
	if !reflect.DeepEqual(info.ContextTerms, []string{"Velkora"}) || info.CustomLexicon == nil || len(info.CustomLexicon) != 0 {
		t.Fatalf("unexpected confirmed update: %#v", info)
	}
	info.ContextTerms[0] = "changed"
	if s.Info().ContextTerms[0] != "Velkora" {
		t.Fatal("returned context terms alias stored settings")
	}
}

func TestRealtimeOmittedContextTermsAndLexiconStayOutOfTheRequest(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	sent := p.update()
	if _, present := sent["context_terms"]; present {
		t.Fatalf("sent context terms: %v", sent)
	}
	if _, present := sent["custom_lexicon"]; present {
		t.Fatalf("sent a custom lexicon: %v", sent)
	}
	// A Prism release without these fields does not publish them.
	if info := s.Info(); info.ContextTerms != nil || info.CustomLexicon != nil {
		t.Fatalf("unpublished settings are not nil: %#v", info)
	}
}

func TestRealtimeContextTermsAndLexiconRefusalReachesTheCaller(t *testing.T) {
	for _, code := range []string{"invalid_client_event", "invalid_session", "turn_failed"} {
		t.Run(code, func(t *testing.T) {
			p := newRealtimePeerWithSession(t, false, rtObject{"id": "session-1", "voice": "test", "context_terms": []any{"Skyvale"}, "custom_lexicon": []any{}})
			_, s := p.setupWith(t, RealtimeConnectOptions{ContextTerms: []string{"Skyvale"}})
			done := make(chan error, 1)
			go func() {
				_, err := s.Update(context.Background(), RealtimeUpdateOptions{ContextTerms: []string{"Velkora"}, CustomLexicon: []PronunciationEntry{rtKubectl}})
				done <- err
			}()
			q := p.next("session.update")
			p.send(rtObject{"type": "error", "error": rtObject{"code": code, "message": code, "scope": "event", "owner": rtObject{"event_id": q["event_id"]}}})
			var fault *RealtimeFault
			if err := <-done; !errors.As(err, &fault) || fault.Code != code || fault.Scope != "event" {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if info := s.Info(); !reflect.DeepEqual(info.ContextTerms, []string{"Skyvale"}) || len(info.CustomLexicon) != 0 {
				t.Fatalf("refused update changed settings: %#v", info)
			}
		})
	}
}

func TestRealtimeMalformedContextTermsAndLexiconFailBeforeSending(t *testing.T) {
	p := newRealtimePeer(t)
	_, s := p.setup(t)
	for _, o := range []RealtimeUpdateOptions{
		{ContextTerms: []string{"Skyvale", "\xff"}},
		{CustomLexicon: []PronunciationEntry{{Spelling: "\xff", Pronunciation: "k"}}},
	} {
		if _, err := s.Update(context.Background(), o); !errors.Is(err, ErrInput) {
			t.Fatalf("accepted invalid UTF-8: %v", err)
		}
	}
	select {
	case q := <-p.requests:
		t.Fatalf("sent %v", q)
	default:
	}
}
