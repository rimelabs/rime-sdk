package rime

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestRealtimeTruncation(t *testing.T) {
	for _, scenario := range []struct {
		setting *Truncation
		wire    any
	}{
		{nil, nil},
		{&Truncation{Mode: TruncationAuto}, "auto"},
		{&Truncation{Mode: TruncationDisabled}, "disabled"},
		{&Truncation{Mode: TruncationRetentionRatio, Ratio: 0.9}, rtObject{"type": "retention_ratio", "retention_ratio": 0.9}},
		{&Truncation{Mode: TruncationRetentionRatio, Ratio: 1}, rtObject{"type": "retention_ratio", "retention_ratio": float64(1)}},
	} {
		setting, want := scenario.setting, scenario.wire
		p := newRealtimePeer(t)
		_, s := p.setupWith(t, RealtimeConnectOptions{Truncation: setting})
		settings := p.update()
		if setting == nil {
			if _, ok := settings["truncation"]; ok {
				t.Fatal("sent an omitted policy")
			}
			continue
		}
		if !reflect.DeepEqual(settings["truncation"], want) {
			t.Fatal(settings)
		}
		done := make(chan error, 1)
		result := make(chan SessionInfo, 1)
		go func() {
			info, err := s.Update(context.Background(), RealtimeUpdateOptions{Truncation: setting})
			result <- info
			done <- err
		}()
		q := p.next("session.update")
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
