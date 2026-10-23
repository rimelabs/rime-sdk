package rime

import "time"

// RealtimeTimeouts are independent of TTS. Zero selects 10s, 30s, and 10s.
type RealtimeTimeouts struct{ Connect, Ready, Request time.Duration }

// TurnDetection controls Prism's speech endpoint detection.
type TurnDetection struct {
	Eagerness          string
	EndOfTurnTimeoutMS int
}

// PrismFiller selects none, text, or auto speech during a tool call.
type PrismFiller struct {
	Type         string `json:"type"`
	Text         string `json:"text,omitempty"`
	Instructions string `json:"instructions,omitempty"`
}

// ToolDefinition describes an application-owned function.
type ToolDefinition struct {
	Name, Description string
	Parameters        map[string]any
	PrismFiller       *PrismFiller
}

// RealtimeConnectOptions configure Prism independently of the parent TTS client.
type RealtimeConnectOptions struct {
	Endpoint, Voice, Instructions string
	Tools                         []ToolDefinition
	InterruptOnSpeech             *bool // Nil means true.
	TurnDetection                 TurnDetection
	Timeouts                      RealtimeTimeouts
	// RequestID is sent as the upgrade's x-request-id. It must be printable
	// ASCII, or Connect returns ErrInput. Prism uses it if it is at most 128
	// characters, else makes its own; Info reports the one it used.
	RequestID string
	// ResponseMetadata labels every reply, such as {"step": "3"}: at most 16,
	// keys up to 64 characters without the prism_ prefix, values up to 512.
	// Change them with SetResponseMetadata.
	ResponseMetadata map[string]string
}

// ReplyOptions request a proactive reply, optionally about a recorded tool result.
type ReplyOptions struct {
	Instruction string
	ToolCall    *ToolCallRef
}

// AudioChunk contains complete signed little-endian PCM16 frames, at most 192000 bytes.
type AudioChunk struct {
	Data   []byte
	Format PCMFormat
}

// ResponseRef identifies a retained response in one session.
type ResponseRef struct{ sessionID, responseID string }

func (r ResponseRef) SessionID() string  { return r.sessionID }
func (r ResponseRef) ResponseID() string { return r.responseID }

// ItemRef identifies a history message.
type ItemRef struct{ sessionID, itemID string }

func (r ItemRef) SessionID() string { return r.sessionID }
func (r ItemRef) ItemID() string    { return r.itemID }

// ToolCallRef identifies a tool call in one response.
type ToolCallRef struct {
	ResponseRef
	callID string
}

func (r ToolCallRef) CallID() string { return r.callID }

// OutputRef identifies an assistant message's audio content.
type OutputRef struct {
	response                  ResponseRef
	itemID                    string
	outputIndex, contentIndex uint64
}

func (r OutputRef) Response() ResponseRef { return r.response }
func (r OutputRef) ItemID() string        { return r.itemID }
func (r OutputRef) OutputIndex() uint64   { return r.outputIndex }
func (r OutputRef) ContentIndex() uint64  { return r.contentIndex }

// SessionInfo contains the settings returned by Prism. Nil fields were not published.
type SessionInfo struct {
	SessionID, Model, Voice                      string
	InterruptOnSpeech                            bool
	ToolResultTimeoutS, ToolContinuationTimeoutS *float64
	Eagerness                                    *string
	EndOfTurnTimeoutMS                           *uint64
	// RequestID is the x-request-id Prism's logs name the session by.
	RequestID *string
	// ResponseMetadata holds the labels replies carry, as Prism last confirmed them.
	ResponseMetadata map[string]string
}

// RealtimeFault preserves structured service fault details. Use errors.As.
type RealtimeFault struct {
	Code, Message, Scope, RequestID, ResponseID, ItemID, CorrelationID, Parameter string
	CallIDs                                                                       []string
}

func (f *RealtimeFault) Error() string { return f.Code + ": " + f.Message }

// Retryable reports whether the same request, unchanged, may succeed on another
// attempt. Only capacity_exceeded, unavailable, and response_timeout return true.
// Unknown codes return false. The application owns retry timing, limits, and
// recovery after partial output; this method does not replay a request.
func (f *RealtimeFault) Retryable() bool {
	switch f.Code {
	case "capacity_exceeded", "unavailable", "response_timeout":
		return true
	default:
		return false
	}
}

// RealtimeEvent is one typed event payload. Unknown server events are ignored.
type RealtimeEvent interface{ isRealtimeEvent() }

// SessionEvent carries protocol correlation and a typed payload.
type SessionEvent struct {
	SessionID, EventID, RequestID string
	Payload                       RealtimeEvent
}
type InputReady struct{}
type SpeechHint struct{ AudioStartMS uint64 }
type SpeechStarted struct {
	ItemID       string
	AudioStartMS uint64
}
type SpeechStopped struct {
	ItemID     string
	AudioEndMS uint64
	EndReason  string
}
type InputCommitted struct{ ItemID string }
type TranscriptDelta struct{ ItemID, Delta string }
type TranscriptFinal struct{ ItemID, Text string }
type TranscriptFailed struct {
	ItemID string
	Fault  *RealtimeFault
}
type ResponseStarted struct {
	Response    ResponseRef
	Cause       string
	Parent      *ResponseRef
	InputItemID string
	// Metadata holds the session's labels when the reply began.
	Metadata map[string]string
	// InputItemIDs names the user items the reply answers; empty when Prism names none.
	InputItemIDs []string
}
type ResponseAbandoned struct{ Response ResponseRef }
type MessageStarted struct{ Output OutputRef }
type TextDelta struct {
	Output OutputRef
	Delta  string
}
type TextDone struct {
	Output OutputRef
	Text   string
}
type AudioDelta struct {
	Output OutputRef
	Audio  AudioChunk
}
type AudioDone struct{ Output OutputRef }
type ToolCall struct {
	Call         ToolCallRef
	ItemID, Name string
	Arguments    map[string]any
}
type ResponseEnded struct {
	Response       ResponseRef
	Status, Reason string
}
type FaultEvent struct{ Fault *RealtimeFault }

func (InputReady) isRealtimeEvent()        {}
func (SpeechHint) isRealtimeEvent()        {}
func (SpeechStarted) isRealtimeEvent()     {}
func (SpeechStopped) isRealtimeEvent()     {}
func (InputCommitted) isRealtimeEvent()    {}
func (TranscriptDelta) isRealtimeEvent()   {}
func (TranscriptFinal) isRealtimeEvent()   {}
func (TranscriptFailed) isRealtimeEvent()  {}
func (ResponseStarted) isRealtimeEvent()   {}
func (ResponseAbandoned) isRealtimeEvent() {}
func (MessageStarted) isRealtimeEvent()    {}
func (TextDelta) isRealtimeEvent()         {}
func (TextDone) isRealtimeEvent()          {}
func (AudioDelta) isRealtimeEvent()        {}
func (AudioDone) isRealtimeEvent()         {}
func (ToolCall) isRealtimeEvent()          {}
func (ResponseEnded) isRealtimeEvent()     {}
func (FaultEvent) isRealtimeEvent()        {}

// PlaybackReport reports device measurements, not received audio duration.
type PlaybackReport interface{ isPlaybackReport() }
type PlaybackInterrupted struct {
	Output     OutputRef
	AudioEndMS uint32
}
type PlaybackFinished struct {
	Response                ResponseRef
	PlayedMS, AudibleTailMS *float64
}

func (PlaybackInterrupted) isPlaybackReport() {}
func (PlaybackFinished) isPlaybackReport()    {}
