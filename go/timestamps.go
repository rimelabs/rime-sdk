package rime

import (
	"context"
	"strings"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// WordTimestamp is a spoken, normalized word. Start and End are seconds from
// the beginning of the entire synthesis, independent of the output sample rate.
type WordTimestamp struct {
	Text  string
	Start float64
	End   float64
}

// TimestampStatus describes alignment independently of audio synthesis.
// Code is codes.OK on success; Message is the service's explanation otherwise.
type TimestampStatus struct {
	Code    codes.Code
	Message string
}

// TimestampResult contains final word timings. Non-OK statuses carry no spans.
type TimestampResult struct {
	Status TimestampStatus
	Spans  []WordTimestamp
}

type timestampTrailer struct {
	seen, invalid bool
	value         *pb.Timestamps
}

func (t *timestampTrailer) accept(trailer *pb.SynthesisResponseTrailer) {
	if t.seen {
		t.invalid = true
	} else {
		t.seen = true
		t.value = trailer.GetTimestamps()
	}
}

func (t timestampTrailer) result(id string) (TimestampResult, error) {
	malformed := &Error{Kind: ErrStream, Message: "invalid timestamp trailer", RequestID: id}
	if t.invalid {
		return TimestampResult{}, malformed
	}
	value := t.value
	if value == nil {
		return TimestampResult{}, &Error{Kind: ErrStream, Message: "service did not return requested timestamps", RequestID: id}
	}
	if value.Status == nil || (value.Status.Code != 0 && len(value.Spans) != 0) {
		return TimestampResult{}, malformed
	}
	seconds := func(d *durationpb.Duration) (float64, bool) {
		if d == nil || d.CheckValid() != nil || d.Seconds < 0 || d.Nanos < 0 {
			return 0, false
		}
		return float64(d.Seconds) + float64(d.Nanos)/1e9, true
	}
	result := TimestampResult{
		Status: TimestampStatus{Code: codes.Code(value.Status.Code), Message: value.Status.Message},
		Spans:  make([]WordTimestamp, 0, len(value.Spans)),
	}
	for _, span := range value.Spans {
		start, startOK := seconds(span.GetStart())
		end, endOK := seconds(span.GetEnd())
		if !startOK || !endOK || strings.TrimSpace(span.GetText()) == "" || end < start {
			return TimestampResult{}, malformed
		}
		result.Spans = append(result.Spans, WordTimestamp{Text: span.Text, Start: start, End: end})
	}
	return result, nil
}

// Timestamps returns requested word timings after Recv has returned io.EOF.
// It never consumes audio or waits: an early call returns ErrInput. Synthesis
// errors are returned as errors; alignment failures are in the result's Status.
func (s *AudioStream) Timestamps() (TimestampResult, error) {
	if !s.timestampsRequested {
		return TimestampResult{}, failure(ErrInput, "enable Timestamps when creating the stream")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return s.timestampTrailer.result(s.id)
	}
	if s.streamError != nil {
		return TimestampResult{}, s.streamError
	}
	if cause := context.Cause(s.op.ctx); cause != nil {
		return TimestampResult{}, operationError(cause, s.id)
	}
	return TimestampResult{}, failure(ErrInput, "consume all audio before reading timestamps")
}
