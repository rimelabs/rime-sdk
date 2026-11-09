package rime

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestRealtimePrismSettingsOmitDefaultsAndMergeUpdates(t *testing.T) {
	_, settings, _, err := rtSettings(RealtimeConnectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := settings["speed"]; ok {
		t.Fatal("default speed was sent")
	}
	if _, ok := settings["prism_review"]; ok {
		t.Fatal("default review was sent")
	}
	if len(settings["turn_detection"].(map[string]any)) != 1 {
		t.Fatal(settings)
	}
	speed, words, silence, interrupt := 1.2, 2, 0, false
	phrases := []string{"stop"}
	_, settings, _, err = rtSettings(RealtimeConnectOptions{Speed: &speed, TurnDetection: TurnDetection{InterruptMinWords: &words, EndOfTurnMinSilenceMS: &silence, InterruptPhrases: phrases, AcknowledgmentPhrases: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	phrases[0] = "changed"
	detection := settings["turn_detection"].(map[string]any)
	if detection["interrupt_phrases"].([]any)[0] != "stop" || detection["end_of_turn_min_silence_ms"].(json.Number).String() != "0" {
		t.Fatal(settings)
	}
	patch, err := rtPatch(RealtimeUpdateOptions{Speed: &speed, InterruptOnSpeech: &interrupt, TurnDetection: &TurnDetectionUpdate{InterruptMinWords: &words, InterruptPhrases: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	detection = patch["turn_detection"].(map[string]any)
	if detection["interrupt_response"] != false || detection["interrupt_min_words"].(json.Number).String() != "2" || len(detection["interrupt_phrases"].([]any)) != 0 {
		t.Fatal(patch)
	}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), 0.84, 1.31} {
		if _, err := rtPatch(RealtimeUpdateOptions{Speed: &invalid}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	}
}

func TestRealtimePrismConfirmedSettingsAndIndependentCopies(t *testing.T) {
	initial := rtObject{"id": "session-1", "voice": "test", "version": "20261104", "speed": 1.0,
		"turn_detection": rtObject{"interrupt_min_words": 1, "interrupt_phrases": []string{"stop"}, "acknowledgment_phrases": []string{}, "end_of_turn_min_silence_ms": 0}}
	p := newRealtimePeerWithSession(t, false, initial)
	_, session := p.setup(t)
	before := session.Info()
	speed, words := 1.25, 3
	done := make(chan error, 1)
	go func() {
		_, err := session.Update(context.Background(), RealtimeUpdateOptions{Speed: &speed, TurnDetection: &TurnDetectionUpdate{InterruptMinWords: &words}})
		done <- err
	}()
	request := p.next("session.update")
	if !reflect.DeepEqual(session.Info(), before) {
		t.Fatal("updated before acknowledgment")
	}
	p.send(rtObject{"type": "error", "error": rtObject{"code": "invalid_session", "scope": "event", "owner": rtObject{"event_id": request["event_id"]}}})
	if err := <-done; !errors.Is(err, ErrRealtime) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(session.Info(), before) {
		t.Fatal("rejected update changed settings")
	}
	go func() {
		_, err := session.Update(context.Background(), RealtimeUpdateOptions{Speed: &speed})
		done <- err
	}()
	request = p.next("session.update")
	initial["speed"], initial["version"] = speed, "development-commit"
	p.send(rtObject{"type": "session.updated", "prism_request_event_id": request["event_id"], "session": initial})
	rtWait(t, done)
	info := session.Info()
	if info.Speed == nil || *info.Speed != speed || info.Version == nil || *info.Version != "development-commit" {
		t.Fatal(info)
	}
	*info.Speed, *info.Version, *info.InterruptMinWords, info.InterruptPhrases[0] = 0, "changed", 99, "changed"
	stored := session.Info()
	if *stored.Speed != speed || *stored.Version != "development-commit" || *stored.InterruptMinWords != 1 || stored.InterruptPhrases[0] != "stop" {
		t.Fatal(stored)
	}
}

func TestRealtimePrismTranscriptAndTimestampReachPublicEvents(t *testing.T) {
	p := newRealtimePeer(t)
	_, session := p.setup(t)
	for _, transcript := range []*string{nil, rtStringPointer("corrected words"), rtStringPointer("")} {
		wire := rtObject{"type": "conversation.item.input_audio_transcription.delta", "event_id": "evt_caption", "item_id": "speech", "content_index": 0, "delta": "", "server_time_ms": uint64(1792857632418)}
		if transcript != nil {
			wire["transcript"] = *transcript
		}
		p.send(wire)
		event := rtRecv(t, session)
		if event.ServerTimeMS == nil || *event.ServerTimeMS != 1792857632418 || !reflect.DeepEqual(event.Payload.(TranscriptDelta).Transcript, transcript) {
			t.Fatal(event)
		}
	}
	p.send(rtObject{"type": "error", "server_time_ms": uint64(1792857632419), "error": rtObject{"code": "review_not_found", "scope": "event"}})
	if event := rtRecv(t, session); event.ServerTimeMS == nil || *event.ServerTimeMS != 1792857632419 {
		t.Fatal(event)
	}
	p.send(rtObject{"type": "prism.typed_input.ready"})
	if event := rtRecv(t, session); event.ServerTimeMS != nil {
		t.Fatal("invented timestamp", event)
	}
}

func rtStringPointer(value string) *string { return &value }

func TestRealtimePrismMalformedOptionalFields(t *testing.T) {
	for _, stamp := range []any{nil, "1792857632418", -1, 1.5, float64(9007199254740992)} {
		wire, _ := json.Marshal(rtObject{"type": "prism.typed_input.ready", "server_time_ms": stamp})
		if _, err := decodeRealtime(wire, "session-1"); err == nil {
			t.Fatalf("accepted timestamp %#v", stamp)
		}
	}
	for _, field := range []string{"version", "speed"} {
		wire, _ := json.Marshal(rtObject{"type": "session.created", "session": rtObject{"id": "session-1", field: nil}})
		if _, err := decodeRealtime(wire, ""); err == nil {
			t.Fatalf("accepted null %s", field)
		}
	}
	wire := []byte(`{"type":"conversation.item.input_audio_transcription.delta","item_id":"i","delta":"","transcript":null}`)
	if _, err := decodeRealtime(wire, "session-1"); err == nil {
		t.Fatal("accepted null transcript")
	}
}

func TestRealtimeVersionSurvivesPartialSessionUpdates(t *testing.T) {
	p := newRealtimePeerWithViews(t, false, rtObject{"id": "session-1", "version": "created-build"}, nil)
	_, session := p.setup(t)
	if version := session.Info().Version; version == nil || *version != "created-build" {
		t.Fatal("initial acknowledgment lost the created version", version)
	}
	for _, version := range []string{"", "updated-build", ""} {
		done := make(chan error, 1)
		go func() { _, err := session.Update(context.Background(), RealtimeUpdateOptions{}); done <- err }()
		request := p.next("session.update")
		view := rtObject{"id": "session-1"}
		if version != "" {
			view["version"] = version
		}
		p.send(rtObject{"type": "session.updated", "prism_request_event_id": request["event_id"], "session": view})
		rtWait(t, done)
		if session.Info().Version == nil {
			t.Fatal("update lost version")
		}
		if version != "" && *session.Info().Version != version {
			t.Fatal(session.Info())
		}
	}
	if *session.Info().Version != "updated-build" {
		t.Fatal(session.Info())
	}
}

func TestRealtimePrismLongTextReachesServerInAllPaths(t *testing.T) {
	for _, path := range []string{"typed", "instruction", "history", "create"} {
		t.Run(path, func(t *testing.T) {
			p := newRealtimePeer(t)
			_, session := p.setup(t)
			p.send(rtObject{"type": "prism.typed_input.ready"})
			text := strings.Repeat("界", 4100)
			done := make(chan error, 1)
			go func() {
				var err error
				switch path {
				case "typed":
					_, err = session.SendText(context.Background(), text)
				case "instruction":
					_, err = session.RequestReply(context.Background(), ReplyOptions{Instruction: text})
				case "history":
					_, err = session.AddMessage(context.Background(), "user", text)
				case "create":
					_, err = session.CreateItem(context.Background(), HistoryItem{Type: HistoryMessage, Role: "user", Text: text}, "")
				}
				done <- err
			}()
			kind := "response.create"
			if path == "history" || path == "create" {
				kind = "conversation.item.create"
			}
			request := p.next(kind)
			code := "invalid_typed_input"
			if path == "instruction" {
				code = "invalid_instruction"
			}
			if kind == "conversation.item.create" {
				code = "item_too_long"
				if request["item"].(map[string]any)["text"] != text {
					t.Fatal("history text changed")
				}
			} else {
				field := "prism_input_text"
				if path == "instruction" {
					field = "prism_instruction"
				}
				if request["response"].(map[string]any)[field] != text {
					t.Fatal("request text changed")
				}
			}
			p.send(rtObject{"type": "error", "error": rtObject{"code": code, "scope": "event", "owner": rtObject{"event_id": request["event_id"]}}})
			var fault *RealtimeFault
			if err := <-done; !errors.As(err, &fault) || fault.Code != code {
				t.Fatal(err)
			}
			p.turn(session, "after-refusal")
		})
	}
	if _, err := rtTools([]ToolDefinition{{Name: strings.Repeat("x", 4001), Parameters: rtObject{}}}); !errors.Is(err, ErrInput) {
		t.Fatal("tool-name limit removed", err)
	}
	if _, err := rtTools([]ToolDefinition{{Name: "tool", Parameters: rtObject{}, PrismFiller: &PrismFiller{Type: "text", Text: strings.Repeat("x", 501)}}}); !errors.Is(err, ErrInput) {
		t.Fatal("filler limit removed", err)
	}
}
