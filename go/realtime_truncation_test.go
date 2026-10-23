package rime

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestRealtimeTruncation(t *testing.T) {
	for _, setting := range []*Truncation{nil, {Mode: TruncationAuto}, {Mode: TruncationDisabled}, {Mode: TruncationRetentionRatio, Ratio: 0.9}, {Mode: TruncationRetentionRatio, Ratio: 1}} {
		_, settings, _, err := rtSettings(RealtimeConnectOptions{Truncation: setting})
		if err != nil {
			t.Fatal(err)
		}
		if setting == nil {
			if _, ok := settings["truncation"]; ok {
				t.Fatal("sent an omitted policy")
			}
			continue
		}
		p := newRealtimePeer(t)
		_, s := p.setup(t)
		done := make(chan error, 1)
		result := make(chan SessionInfo, 1)
		go func() {
			info, err := s.Update(context.Background(), RealtimeUpdateOptions{Truncation: setting})
			result <- info
			done <- err
		}()
		q := p.next("session.update")
		want, _ := rtTruncation(*setting)
		if !reflect.DeepEqual(q["session"], rtObject{"truncation": want}) {
			t.Fatal(q)
		}
		p.send(rtObject{"type": "session.updated", "prism_request_event_id": q["event_id"], "session": rtObject{"id": "session-1", "truncation": want}})
		rtWait(t, done)
		info := <-result
		if !reflect.DeepEqual(info.Truncation, setting) {
			t.Fatal(info)
		}
		info.Truncation.Mode = "changed"
		if !reflect.DeepEqual(s.Info().Truncation, setting) {
			t.Fatal("returned policy aliases session state")
		}
	}
	for _, setting := range []Truncation{{}, {Mode: TruncationAuto, Ratio: 0.9}, {Mode: TruncationRetentionRatio}, {Mode: TruncationRetentionRatio, Ratio: -1}, {Mode: TruncationRetentionRatio, Ratio: 1.1}, {Mode: TruncationRetentionRatio, Ratio: math.NaN()}, {Mode: TruncationRetentionRatio, Ratio: math.Inf(1)}} {
		if _, err := rtPatch(RealtimeUpdateOptions{Truncation: &setting}); !errors.Is(err, ErrInput) {
			t.Fatal(setting, err)
		}
	}
}
