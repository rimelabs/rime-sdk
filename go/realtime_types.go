package rime

import (
	"context"
	"fmt"
	"time"
)

// RealtimeTimeouts are independent of TTS. Zero selects 10s, 30s, and 10s.
type RealtimeTimeouts struct{ Connect, Ready, Request time.Duration }

// TurnDetection controls Prism's speech endpoint detection.
type TurnDetection struct {
	Eagerness          string
	EndOfTurnTimeoutMS int
	// Nil leaves the server default. Minimum silence is fixed at connection.
	EndOfTurnMinSilenceMS *int
	InterruptMinWords     *int
	// Nil omits the setting; an empty slice explicitly clears it.
	InterruptPhrases      []string
	AcknowledgmentPhrases []string
}

// TurnDetectionUpdate contains the turn settings that may change during a session.
// Nil values leave the current setting unchanged; empty lists clear it.
type TurnDetectionUpdate struct {
	InterruptMinWords     *int
	InterruptPhrases      []string
	AcknowledgmentPhrases []string
}

// ReviewRequest is one spoken chunk, possibly before its response is announced.
// A review request does not establish that its text was spoken.
type ReviewRequest struct {
	ReviewID, ResponseID, ItemID string
	Index                        uint64
	Text                         string
}

// ReviewDecision permits, replaces, or stops a chunk. Review does not undo tool calls.
type ReviewDecision interface{ reviewDecision() }

// ReviewAllow permits the chunk as written.
type ReviewAllow struct{}

// ReviewReplace replaces this chunk and ends the reply after the replacement.
type ReviewReplace struct{ Text string }

// ReviewStop omits this chunk and ends the reply after any earlier allowed chunks.
type ReviewStop struct{}

func (ReviewAllow) reviewDecision()   {}
func (ReviewReplace) reviewDecision() {}
func (ReviewStop) reviewDecision()    {}

// ReviewTimeout is the server action when no verdict arrives in time.
type ReviewTimeout interface{ reviewTimeout() }

// ReviewTimeoutStop ends the reply if its review expires.
type ReviewTimeoutStop struct{}

// ReviewTimeoutSpeak allows the original chunk if its review expires.
type ReviewTimeoutSpeak struct{}

// ReviewTimeoutSay replaces the chunk with Text if its review expires.
type ReviewTimeoutSay struct{ Text string }

func (ReviewTimeoutStop) reviewTimeout()  {}
func (ReviewTimeoutSpeak) reviewTimeout() {}
func (ReviewTimeoutSay) reviewTimeout()   {}

// Review enables review at connection. Callbacks run concurrently, independently
// of Recv, and must honor context cancellation. Close waits for them to return.
// Do not call Close from a callback. Callback errors attempt Stop and fail the session.
type Review struct {
	Callback func(context.Context, ReviewRequest) (ReviewDecision, error)
	// TimeoutMS is 100 through 5000. Zero selects the server default of 1000.
	TimeoutMS int
	// Nil selects Stop. The server remains the authority for timeout timing.
	OnTimeout ReviewTimeout
}

// ReviewSettings contains the settings confirmed by the server, without a callback.
type ReviewSettings struct {
	TimeoutMS uint64
	OnTimeout ReviewTimeout
}

// TruncationMode selects the history policy when the context fills.
type TruncationMode string

const (
	TruncationAuto           TruncationMode = "auto"
	TruncationDisabled       TruncationMode = "disabled"
	TruncationRetentionRatio TruncationMode = "retention_ratio"
)

// Truncation keeps Ratio of available history capacity in retention-ratio mode.
// Ratio must be above zero and at most one in that mode; otherwise it must be zero.
type Truncation struct {
	Mode  TruncationMode
	Ratio float64
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
	// Speed is omitted when nil. A supplied multiplier must be 0.85 through 1.3.
	Speed  *float64
	Review *Review
	// Truncation is nil when omitted or not published by an older server.
	Truncation                    *Truncation
	Endpoint, Voice, Instructions string
	Tools                         []ToolDefinition
	InterruptOnSpeech             *bool // Nil means true.
	TurnDetection                 TurnDetection
	Timeouts                      RealtimeTimeouts
	// RequestID is sent as the upgrade's x-request-id. It must be nonblank
	// printable ASCII, or Connect returns ErrInput. Prism replaces IDs longer than
	// 128 characters. A gateway can replace shorter IDs too. Info reports the ID
	// Prism actually used.
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

// ReplyRequestOptions adds labels to one typed, proactive, or continued reply.
type ReplyRequestOptions struct {
	// Metadata labels this reply. Keys shared with session labels take precedence.
	Metadata map[string]string
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

// RealtimeUpdateOptions change only supplied settings on the existing connection.
// Nil fields keep their values. An empty string clears instructions; an empty,
// non-nil Tools slice removes all tools. Tools replaces the complete list.
type RealtimeUpdateOptions struct {
	Speed         *float64
	TurnDetection *TurnDetectionUpdate
	// Truncation is nil when omitted or not published by an older server.
	Truncation        *Truncation
	Instructions      *string
	Tools             []ToolDefinition
	Voice             *string
	InterruptOnSpeech *bool
}

// SessionInfo contains the settings returned by Prism. Nil fields were not published.
type SessionInfo struct {
	// Nil means the older server did not publish the value.
	Speed                 *float64
	Version               *string
	Review                *ReviewSettings
	EndOfTurnMinSilenceMS *uint64
	InterruptMinWords     *uint64
	InterruptPhrases      []string
	AcknowledgmentPhrases []string
	// Truncation is nil when omitted or not published by an older server.
	Truncation *Truncation
	// SettingsRevision is nil when the server did not publish it.
	SettingsRevision                             *uint64
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

// RealtimeIdleTimeout reports the server's idle limit when it closes a conversation.
type RealtimeIdleTimeout struct{ IdleTimeoutS float64 }

func (e *RealtimeIdleTimeout) Error() string {
	return fmt.Sprintf("realtime session idle for %g seconds", e.IdleTimeoutS)
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
	// ServerTimeMS is the server's Unix time in milliseconds; nil if absent.
	ServerTimeMS *uint64
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
type TranscriptDelta struct {
	ItemID, Delta string
	// Transcript replaces accumulated captions when present, including empty text.
	Transcript *string
}
type TranscriptFinal struct{ ItemID, Text string }
type TranscriptFailed struct {
	ItemID string
	Fault  *RealtimeFault
}
type ResponseStarted struct {
	interruptOnSpeech *bool
	// SettingsRevision names the settings this reply began with, if published.
	SettingsRevision *uint64
	Response         ResponseRef
	Cause            string
	Parent           *ResponseRef
	InputItemID      string
	// InstructionItemID names a saved nudge instruction that DeleteItem can remove.
	InstructionItemID string
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

// ResponseTruncation describes history items excluded from a reply's prompt.
// These items remain in conversation history. Already hidden items are not counted.
type ResponseTruncation struct {
	// ItemsLeftOut counts excluded items, not turns.
	ItemsLeftOut uint64
	// FirstKeptItemID is nil when no earlier item was retained.
	FirstKeptItemID *string
}

// ResponseUsage contains the model tokens counted for a reply.
type ResponseUsage struct {
	InputTokens   uint64 `json:"input_tokens"`
	OutputTokens  uint64 `json:"output_tokens"`
	TotalTokens   uint64 `json:"total_tokens"`
	ContextWindow uint64 `json:"context_window"`
}
type ResponseEnded struct {
	// SettingsRevision names the settings this reply began with, if published.
	SettingsRevision *uint64
	Response         ResponseRef
	Status, Reason   string
	// Truncation is nil when the server reports no exclusions or the information is unavailable.
	Truncation *ResponseTruncation
	// Usage is nil when Prism did not count tokens.
	Usage *ResponseUsage
}
type FaultEvent struct{ Fault *RealtimeFault }

// ItemTruncated records a playback report and the text retained in history.
// Replies with tool calls retain all words, which can differ from audible text.
type ItemTruncated struct {
	ItemID       string
	ContentIndex uint64
	AudioEndMS   uint64
	// KeptText is nil when unavailable, including during generation, and points
	// to an empty string when no words remain.
	KeptText *string
}

func (ItemTruncated) isRealtimeEvent() {}

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
