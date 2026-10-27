package rime

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

type rtObject = map[string]any

// Application JSON must retain exact numbers and contain one complete object.
func rtJSONObject(raw []byte) (rtObject, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object rtObject
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	if !rtFiniteJSON(object) {
		return nil, fmt.Errorf("JSON numbers must be finite")
	}
	return object, nil
}

func rtFiniteJSON(value any) bool {
	switch value := value.(type) {
	case json.Number:
		number, err := value.Float64()
		return err == nil && !math.IsInf(number, 0) && !math.IsNaN(number)
	case map[string]any:
		for _, child := range value {
			if !rtFiniteJSON(child) {
				return false
			}
		}
	case []any:
		for _, child := range value {
			if !rtFiniteJSON(child) {
				return false
			}
		}
	}
	return true
}

type rtWire struct {
	history                                 []HistoryItem
	createdID                               string
	interruptKnown                          bool
	kind, eventID, requestID, target, token string
	index                                   uint64
	info                                    SessionInfo
	payload                                 RealtimeEvent
}

// Keep the first validation error while reading a known event. The decoder
// returns no partially decoded event when any required field is invalid.
type rtDecoder struct{ err error }

func (decoder *rtDecoder) bad(field string) {
	if decoder.err == nil {
		decoder.err = failure(ErrStream, "invalid realtime field: "+field)
	}
}
func (decoder *rtDecoder) object(v any) rtObject {
	x, ok := v.(map[string]any)
	if !ok {
		decoder.bad("object")
	}
	return x
}
func (decoder *rtDecoder) string(v any) string {
	x, ok := v.(string)
	if !ok {
		decoder.bad("string")
	}
	return x
}
func (decoder *rtDecoder) id(v any) string {
	x := decoder.string(v)
	if x == "" {
		decoder.bad("id")
	}
	return x
}
func (decoder *rtDecoder) optional(v any) string {
	if v == nil {
		return ""
	}
	return decoder.string(v)
}
func (decoder *rtDecoder) integer(v any) uint64 {
	x, ok := v.(float64)
	if !ok || x < 0 || x > 9007199254740991 || math.Trunc(x) != x {
		decoder.bad("integer")
		return 0
	}
	return uint64(x)
}

// rtLabels keeps the client's own labels in metadata: string values under keys
// without Prism's prism_ prefix.
func rtLabels(metadata rtObject) map[string]string {
	labels := map[string]string{}
	for k, v := range metadata {
		if s, ok := v.(string); ok && !strings.HasPrefix(k, "prism_") {
			labels[k] = s
		}
	}
	return labels
}
func rtEnum(s string, values ...string) string {
	for _, v := range values {
		if s == v {
			return s
		}
	}
	return "unknown"
}
func (decoder *rtDecoder) view(v any) SessionInfo {
	o := decoder.object(v)
	s := SessionInfo{SessionID: decoder.id(o["id"]), Model: "prism", InterruptOnSpeech: true}
	if v, ok := o["truncation"]; ok {
		t := Truncation{}
		if mode, ok := v.(string); ok {
			t.Mode = TruncationMode(mode)
		} else {
			ratio := decoder.object(v)
			t.Mode = TruncationMode(decoder.string(ratio["type"]))
			t.Ratio, ok = ratio["retention_ratio"].(float64)
			if !ok || t.Mode != TruncationRetentionRatio {
				decoder.bad("session.truncation")
			}
		}
		if _, err := rtTruncation(t); err != nil {
			decoder.bad("session.truncation")
		}
		s.Truncation = &t
	}
	if v, ok := o["voice"]; ok {
		s.Voice = decoder.string(v)
	}
	if v := o["request_id"]; v != nil {
		x := decoder.string(v)
		s.RequestID = &x
	}
	if v, ok := o["prism_response_metadata"]; ok {
		s.ResponseMetadata = rtLabels(decoder.object(v))
	}
	if v, ok := o["prism_effective_interrupt_response"]; ok {
		b, ok := v.(bool)
		if !ok {
			decoder.bad("interrupt")
		}
		s.InterruptOnSpeech = b
	}
	if v, ok := o["turn_detection"]; ok {
		d := decoder.object(v)
		if v, ok := d["eagerness"]; ok {
			x := rtEnum(decoder.string(v), "low", "medium", "high", "auto")
			s.Eagerness = &x
		}
		if v, ok := d["prism_end_of_turn_timeout_ms"]; ok {
			x := decoder.integer(v)
			if x < 1000 || x > 10000 {
				decoder.bad("turn timeout")
			}
			s.EndOfTurnTimeoutMS = &x
		}
	}
	if v, ok := o["prism_tool_waits"]; ok {
		d := decoder.object(v)
		ptrs := []**float64{&s.ToolResultTimeoutS, &s.ToolContinuationTimeoutS}
		for i, k := range []string{"result_timeout_s", "continuation_timeout_s"} {
			v, ok := d[k]
			if !ok {
				decoder.bad(k)
			}
			if v != nil {
				x, ok := v.(float64)
				if !ok || x <= 0 || math.IsInf(x, 0) {
					decoder.bad(k)
				}
				*ptrs[i] = &x
			}
		}
	}
	s.SettingsRevision = decoder.revision(o)
	return s
}
func (decoder *rtDecoder) revision(o rtObject) *uint64 {
	if v, ok := o["prism_settings_revision"]; ok {
		n := decoder.integer(v)
		return &n
	}
	return nil
}
func (decoder *rtDecoder) fault(v any) *RealtimeFault {
	o := decoder.object(v)
	f := &RealtimeFault{Code: "unknown", Scope: "unknown"}
	if v, ok := o["code"]; ok {
		f.Code = decoder.string(v)
	}
	if v, ok := o["message"]; ok {
		f.Message = decoder.string(v)
	}
	if v, ok := o["scope"]; ok {
		f.Scope = rtEnum(decoder.string(v), "event", "utterance", "response", "tool_roundtrip", "session")
	}
	f.CorrelationID = decoder.optional(o["correlation_id"])
	f.Parameter = decoder.optional(o["param"])
	if o["owner"] != nil {
		d := decoder.object(o["owner"])
		f.RequestID = decoder.optional(d["event_id"])
		f.ResponseID = decoder.optional(d["response_id"])
		f.ItemID = decoder.optional(d["item_id"])
		if d["call_ids"] != nil {
			a, ok := d["call_ids"].([]any)
			if !ok {
				decoder.bad("call_ids")
			}
			for _, v := range a {
				f.CallIDs = append(f.CallIDs, decoder.id(v))
			}
		}
	}
	return f
}
func decodeRealtime(raw []byte, sessionID string) (w *rtWire, err error) {
	decoder := &rtDecoder{}
	defer func() {
		if decoder.err != nil {
			w = nil
			err = decoder.err
		}
	}()
	if !utf8.Valid(raw) {
		return nil, failure(ErrStream, "invalid JSON UTF-8")
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil, failure(ErrStream, "invalid realtime JSON")
	}
	o := decoder.object(value)
	k := decoder.string(o["type"])
	w = &rtWire{}
	ref := func(v any) ResponseRef {
		if sessionID == "" {
			decoder.bad("event before session.created")
		}
		return ResponseRef{sessionID, decoder.id(v)}
	}
	switch k {
	case "session.created", "session.updated":
		w.kind = k
		w.info = decoder.view(o["session"])
		_, w.interruptKnown = decoder.object(o["session"])["prism_effective_interrupt_response"]
	case "conversation.item.created":
		if item := decoder.object(o["item"]); item["id"] != nil {
			w.createdID = decoder.id(item["id"])
		}
		d := decoder.object(o["item"])
		if d["type"] == "function_call_output" {
			w.kind = "tool.result.created"
			w.target = decoder.id(d["call_id"])
		} else {
			if v, ok := d["type"]; ok && v != "message" && v != "function_call" {
				decoder.bad("item.type")
			}
			w.kind = "item.created"
			w.target = decoder.id(d["id"])
		}
	case "prism.conversation":
		w.kind = "history.listed"
		items, ok := o["items"].([]any)
		if !ok {
			decoder.bad("items")
		}
		w.history = make([]HistoryItem, 0, len(items))
		for _, item := range items {
			w.history = append(w.history, decoder.historyItem(item))
		}
	case "conversation.item.deleted":
		w.kind = "item.deleted"
		w.target = decoder.id(o["item_id"])
	case "input_audio_buffer.cleared":
		w.kind = "audio.cleared"
	case "conversation.item.truncated":
		w.kind = "item.truncated"
		w.target = decoder.id(o["item_id"])
		w.index = decoder.integer(o["content_index"])
		var keptText *string
		if value, present := o["prism_kept_text"]; present {
			text := decoder.string(value)
			keptText = &text
		}
		w.payload = ItemTruncated{ItemID: w.target, ContentIndex: w.index, AudioEndMS: decoder.integer(o["audio_end_ms"]), KeptText: keptText}
	case "prism.typed_input.ready":
		w.kind = "input.ready"
		w.payload = InputReady{}
	case "prism.input_audio_buffer.speech_hint":
		w.kind = "speech.hint"
		w.payload = SpeechHint{decoder.integer(o["audio_start_ms"])}
	case "input_audio_buffer.speech_started":
		w.kind = "speech.started"
		w.payload = SpeechStarted{decoder.id(o["item_id"]), decoder.integer(o["audio_start_ms"])}
	case "input_audio_buffer.speech_stopped":
		w.kind = "speech.stopped"
		reason := ""
		if v, ok := o["prism_end_reason"]; ok {
			reason = rtEnum(decoder.string(v), "semantic", "timeout", "silence", "max_duration", "detector_changed")
		}
		w.payload = SpeechStopped{decoder.id(o["item_id"]), decoder.integer(o["audio_end_ms"]), reason}
	case "input_audio_buffer.committed":
		w.kind = "input.committed"
		w.payload = InputCommitted{decoder.id(o["item_id"])}
	case "conversation.item.input_audio_transcription.delta":
		w.kind = "transcript.delta"
		w.payload = TranscriptDelta{decoder.id(o["item_id"]), decoder.string(o["delta"])}
	case "conversation.item.input_audio_transcription.completed":
		w.kind = "transcript.final"
		w.payload = TranscriptFinal{decoder.id(o["item_id"]), decoder.string(o["transcript"])}
	case "conversation.item.input_audio_transcription.failed":
		w.kind = "transcript.failed"
		d := decoder.object(o["error"])
		id := decoder.id(o["item_id"])
		w.payload = TranscriptFailed{id, &RealtimeFault{Code: decoder.string(d["code"]), Message: decoder.string(d["message"]), Scope: "utterance", ItemID: id}}
	case "response.created", "response.done":
		d := decoder.object(o["response"])
		r := ref(d["id"])
		m := rtObject{}
		if d["metadata"] != nil {
			m = decoder.object(d["metadata"])
		}
		if k == "response.created" {
			w.kind = "response.started"
			cause := "speech"
			if v, ok := m["prism_cause"]; ok {
				switch decoder.string(v) {
				case "user_text":
					cause = "text"
				case "tool_continuation":
					cause = "tools"
				case "proactive":
					cause = "proactive"
				default:
					cause = "unknown"
				}
			}
			var parent *ResponseRef
			if id := decoder.optional(m["prism_parent_response_id"]); id != "" {
				r := ref(id)
				parent = &r
			}
			input := decoder.optional(m["prism_input_item_id"])
			inputs := []string{}
			if v := d["prism_input_item_ids"]; v != nil {
				a, ok := v.([]any)
				if !ok {
					decoder.bad("prism_input_item_ids")
				}
				for _, v := range a {
					inputs = append(inputs, decoder.id(v))
				}
			} else if input != "" {
				inputs = append(inputs, input)
			}
			w.payload = ResponseStarted{Response: r, Cause: cause, Parent: parent, InputItemID: input, Metadata: rtLabels(m), InputItemIDs: inputs, SettingsRevision: decoder.revision(d)}
		} else {
			w.kind = "response.ended"
			details := rtObject{}
			if d["status_details"] != nil {
				details = decoder.object(d["status_details"])
			}
			w.token = decoder.optional(m["prism_drain_token"])
			w.payload = ResponseEnded{Response: r, Status: rtEnum(decoder.string(d["status"]), "completed", "cancelled", "failed"), Reason: decoder.optional(details["reason"]), SettingsRevision: decoder.revision(d)}
		}
	case "response.function_call_arguments.done":
		w.kind = "tool.call"
		args, err := rtJSONObject([]byte(decoder.string(o["arguments"])))
		if err != nil {
			decoder.bad("arguments")
		}
		w.payload = ToolCall{ToolCallRef{ref(o["response_id"]), decoder.id(o["call_id"])}, decoder.id(o["item_id"]), decoder.string(o["name"]), decoder.object(args)}
	case "response.output_item.added", "response.text.delta", "response.text.done", "response.audio.delta", "response.audio.done":
		var item string
		var index uint64
		if k == "response.output_item.added" {
			d := decoder.object(o["item"])
			if decoder.string(d["type"]) != "message" {
				return nil, nil
			}
			item = decoder.id(d["id"])
			if v, ok := o["content_index"]; ok {
				index = decoder.integer(v)
			}
		} else {
			item = decoder.id(o["item_id"])
			index = decoder.integer(o["content_index"])
		}
		if index != 0 {
			decoder.bad("content_index must be zero")
		}
		out := OutputRef{ref(o["response_id"]), item, decoder.integer(o["output_index"]), index}
		switch k {
		case "response.output_item.added":
			w.kind = "message.started"
			w.payload = MessageStarted{out}
		case "response.text.delta":
			w.kind = "text.delta"
			w.payload = TextDelta{out, decoder.string(o["delta"])}
		case "response.text.done":
			w.kind = "text.done"
			w.payload = TextDone{out, decoder.string(o["text"])}
		case "response.audio.done":
			w.kind = "audio.done"
			w.payload = AudioDone{out}
		case "response.audio.delta":
			w.kind = "audio.delta"
			s := decoder.string(o["delta"])
			b, e := base64.StdEncoding.Strict().DecodeString(s)
			if e != nil || len(b)%2 != 0 || base64.StdEncoding.EncodeToString(b) != s {
				decoder.bad("audio.delta")
			}
			w.payload = AudioDelta{out, AudioChunk{b, PCMFormat{24000, 1}}}
		}
	case "error":
		w.kind = "error"
		w.payload = FaultEvent{decoder.fault(o["error"])}
	default:
		return nil, nil
	}
	if v, ok := o["event_id"]; ok {
		w.eventID = decoder.string(v)
	}
	w.requestID = decoder.optional(o["prism_request_event_id"])
	return w, nil
}
func rtFaultError(f *RealtimeFault) error {
	return &Error{Kind: ErrRealtime, Message: f.Error(), RequestID: f.RequestID, Cause: f}
}
func rtEncode(id, kind string, body rtObject) ([]byte, error) {
	v := rtObject{"event_id": id, "type": kind}
	for k, x := range body {
		v[k] = x
	}
	b, e := json.Marshal(v)
	if e != nil {
		return nil, failure(ErrInput, fmt.Sprint("invalid realtime request: ", e))
	}
	return b, nil
}
