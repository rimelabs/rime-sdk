package rime

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestRealtimeToolSchemasRejectInvalidUTF8WithoutChangingValues(t *testing.T) {
	bad := string([]byte{0xff})
	type namedString string
	type schemaPart struct {
		Description string `json:"description"`
		Ignored     string `json:"-"`
	}
	for name, value := range map[string]any{
		"nested string": map[string]any{"items": []any{map[string]any{"description": bad}}},
		"nested key":    map[string]any{"properties": map[string]any{bad: map[string]any{}}},
		"typed map":     map[namedString]namedString{namedString(bad): "value"},
		"typed slice":   []namedString{"valid", namedString(bad)},
		"pointer":       &schemaPart{Description: bad},
		"raw JSON":      json.RawMessage(append(append([]byte(`{"description":"`), 0xff), []byte(`"}`)...)),
	} {
		t.Run(name, func(t *testing.T) {
			options := []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"nested": value}}}
			if _, _, _, err := rtSettings(RealtimeConnectOptions{Tools: options}); !errors.Is(err, ErrInput) {
				t.Fatalf("connect accepted invalid schema: %v", err)
			}
			if _, err := rtPatch(RealtimeUpdateOptions{Tools: options}); !errors.Is(err, ErrInput) {
				t.Fatalf("update accepted invalid schema: %v", err)
			}
		})
	}

	// Distinct slices can share the same first element. Checking a short view
	// must not hide invalid strings in a longer view of that backing array.
	values := []string{"valid", bad}
	if _, err := rtPatch(RealtimeUpdateOptions{Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"views": []any{values[:1], values}}}}}); !errors.Is(err, ErrInput) {
		t.Fatalf("accepted invalid string after shared slice prefix: %v", err)
	}

	valid := map[string]any{
		"unicode": "日本語 🎙", "number": json.Number("9007199254740993"),
		"typed":   map[namedString][]namedString{"name": {"value"}},
		"pointer": &schemaPart{Description: "valid", Ignored: bad},
		"bytes":   []byte{0xff}, "raw": json.RawMessage(`{"description":"valid"}`),
	}
	tools := []ToolDefinition{{Name: "lookup", Parameters: valid}}
	_, connected, _, err := rtSettings(RealtimeConnectOptions{Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := rtPatch(RealtimeUpdateOptions{Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(valid)
	for _, settings := range []rtObject{connected, updated} {
		parameters := settings["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"]
		got, err := json.Marshal(parameters)
		if err != nil || string(got) != string(want) {
			t.Fatalf("valid schema changed: got %s, want %s; error %v", got, want, err)
		}
	}
}

func TestRealtimeCyclicToolSchemasFailWithoutRecursingForever(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	for _, update := range []bool{false, true} {
		tools := []ToolDefinition{{Name: "lookup", Parameters: cyclic}}
		var err error
		if update {
			_, err = rtPatch(RealtimeUpdateOptions{Tools: tools})
		} else {
			_, _, _, err = rtSettings(RealtimeConnectOptions{Tools: tools})
		}
		if !errors.Is(err, ErrInput) {
			t.Fatalf("cyclic schema accepted, update=%v: %v", update, err)
		}
	}
}

func TestRealtimePolicyHistoryMarksMissingRevisionChanges(t *testing.T) {
	one, three, ten := uint64(1), uint64(3), uint64(10)
	for _, first := range []*uint64{nil, &one} {
		s := &RealtimeSession{}
		s.setInfo(SessionInfo{SettingsRevision: first, InterruptOnSpeech: true})
		s.setInfo(SessionInfo{InterruptOnSpeech: true})
		s.setInfo(SessionInfo{InterruptOnSpeech: false})
		s.setInfo(SessionInfo{InterruptOnSpeech: true})
		s.setInfo(SessionInfo{InterruptOnSpeech: false})
		if policy, err := s.interruptionPolicy(nil); err != nil || policy {
			t.Fatalf("revisionless reply lost current setting: %v, %v", policy, err)
		}
		s.setInfo(SessionInfo{SettingsRevision: &three, InterruptOnSpeech: false})
		if policy, err := s.interruptionPolicy(&three); err != nil || policy {
			t.Fatalf("known boundary did not restore policy: %v, %v", policy, err)
		}
		if first != nil {
			if policy, err := s.interruptionPolicy(&one); err != nil || !policy {
				t.Fatalf("old confirmed policy changed: %v, %v", policy, err)
			}
		}
		two := uint64(2)
		if _, err := s.interruptionPolicy(&two); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("ambiguous revision did not fail explicitly: %v", err)
		}
		// Latest same-policy acknowledgments still establish where the next
		// unknown change starts, without consuming retained policy history.
		s.setInfo(SessionInfo{SettingsRevision: &ten, InterruptOnSpeech: false})
		before := len(s.interruptionPolicies)
		s.setInfo(SessionInfo{InterruptOnSpeech: false})
		if len(s.interruptionPolicies) != before {
			t.Fatal("unchanged revisionless setting consumed policy history")
		}
		s.setInfo(SessionInfo{InterruptOnSpeech: true})
		if policy, err := s.interruptionPolicy(&ten); err != nil || policy {
			t.Fatalf("last confirmed revision became ambiguous: %v, %v", policy, err)
		}
		eleven := uint64(11)
		if _, err := s.interruptionPolicy(&eleven); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("unknown boundary ignored latest acknowledgment: %v", err)
		}
		if s.Info().SettingsRevision != nil {
			t.Fatal("missing revision was fabricated in SessionInfo")
		}
	}
}
