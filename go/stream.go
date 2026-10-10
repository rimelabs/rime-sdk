package rime

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	pb "github.com/rimelabs/rime-api/go"
	"github.com/rimelabs/rime-sdk/go/internal/sentences"
	"google.golang.org/grpc"
)

// TextSource returns the next UTF-8 fragment or io.EOF. It must honor cancellation.
// The SDK calls it serially. Return a source error to stop synthesis.
type TextSource func(context.Context) (string, error)

type streamLimits struct{ connection, firstAudio, progress time.Duration }

var defaultStreamLimits = streamLimits{10 * time.Second, 30 * time.Second, 60 * time.Second}

// Input and output advance independently. All lifecycle state is protected by mu.
type inputState uint8

const (
	inputProcessing inputState = iota
	inputWaitingForSource
	inputFinished
)

type outputState uint8

const (
	outputAwaitingText outputState = iota
	// Preserve audio progress even if a service sends audio before the first text.
	outputAwaitingTextAfterAudio
	outputAwaitingFirstAudio
	outputReceivingAudio
	outputFinished
)

// AudioStream permits one concurrent Recv call. Close cancels unfinished work.
// Audio is complete only when Recv returns io.EOF. Earlier audio can precede an error.
type AudioStream struct {
	op                  *operation
	queue               *audioQueue
	format              AudioFormat
	reading             atomic.Bool
	mu                  sync.Mutex
	id                  string
	lastProgress        time.Time
	input               inputState
	output              outputState
	done                chan struct{}
	workersDone         chan struct{}
	finishOnce          sync.Once
	timestampsRequested bool
	timestampTrailer    timestampTrailer
	completed           bool
	streamError         error
}

// Stream starts synthesis of complete text. Use StreamSource for incremental input.
func (t *TTSService) Stream(ctx context.Context, text string, options SynthesisOptions) (*AudioStream, error) {
	if strings.TrimSpace(text) == "" || !utf8.ValidString(text) {
		return nil, failure(ErrInput, "text must be nonblank UTF-8")
	}
	if options.CompleteText && len(text) > 65536 {
		return nil, failure(ErrInput, "complete text must be at most 65536 UTF-8 bytes")
	}
	if options.CompleteText {
		return t.stream(ctx, nil, text, options)
	}
	first := true
	return t.StreamSource(ctx, func(context.Context) (string, error) {
		if first {
			first = false
			return text, nil
		}
		return "", io.EOF
	}, options)
}

// StreamSource starts work immediately. The source may supply partial sentences.
func (t *TTSService) StreamSource(ctx context.Context, source TextSource, options SynthesisOptions) (*AudioStream, error) {
	if options.CompleteText {
		return nil, failure(ErrInput, "CompleteText requires Stream with a string, not StreamSource")
	}
	return t.stream(ctx, source, "", options)
}

func (t *TTSService) stream(ctx context.Context, source TextSource, text string, options SynthesisOptions) (*AudioStream, error) {
	lexicon, err := snapshotLexicon(options.CustomLexicon)
	if err != nil {
		return nil, err
	}
	if options.Timestamps && t.client.model != "mistv3" {
		return nil, failure(ErrInput, "word timestamps are supported only with Model=mistv3")
	}
	if source == nil && !options.CompleteText {
		return nil, failure(ErrInput, "text source must not be nil")
	}
	if options.AudioFormat > MULAW8000 {
		return nil, failure(ErrAudioFormat, "select PCM24000 or MULAW8000")
	}
	voice, language := options.Voice, options.Language
	if voice == "" {
		voice = t.client.voice
	}
	if language == "" {
		language = "en"
	}
	if strings.TrimSpace(voice) == "" || strings.TrimSpace(language) == "" || !utf8.ValidString(voice) || !utf8.ValidString(language) {
		return nil, failure(ErrInput, "voice and language must be nonblank UTF-8")
	}
	o, err := t.client.operation(ctx, options.Timeout, 0)
	if err != nil {
		return nil, err
	}
	s := &AudioStream{op: o, queue: newAudioQueue(), format: options.AudioFormat, lastProgress: time.Now(), done: make(chan struct{}), workersDone: make(chan struct{})}
	s.timestampsRequested = options.Timestamps
	go s.watch()
	go s.run(source, text, voice, language, lexicon)
	return s, nil
}

func (s *AudioStream) Format() AudioFormat { return s.format }
func (s *AudioStream) RequestID() string   { s.mu.Lock(); defer s.mu.Unlock(); return s.id }
func (s *AudioStream) setID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == "" {
		s.id = id
	}
}
func (s *AudioStream) complete() { s.finishOnce.Do(func() { close(s.done); s.op.close() }) }
func (s *AudioStream) fail(err error) {
	// Transport cancellation can arrive before watch observes the context.
	// Preserve the original cancellation or deadline cause in either order.
	if cause := context.Cause(s.op.ctx); cause != nil {
		err = cause
	}
	mapped := operationError(err, s.RequestID())
	s.mu.Lock()
	if s.streamError == nil && !s.completed {
		s.streamError = mapped
	}
	s.mu.Unlock()
	s.queue.fail(mapped)
	s.op.cancel(mapped)
}

// Recv returns independently owned bytes containing complete sample frames.
func (s *AudioStream) Recv() ([]byte, error) {
	if !s.reading.CompareAndSwap(false, true) {
		return nil, failure(ErrInput, "AudioStream permits only one concurrent reader")
	}
	defer s.reading.Store(false)
	data, err := s.queue.get(s.op.ctx)
	if err != nil {
		if err != io.EOF {
			s.fail(err)
			err = operationError(err, s.RequestID())
		} else {
			s.mu.Lock()
			s.completed = true
			s.mu.Unlock()
		}
		s.complete()
	}
	return data, err
}

// Close cancels this stream. It does not cancel other streams on the client.
// Cleanup waits at most two seconds for a user source that ignores cancellation.
func (s *AudioStream) Close() error {
	s.fail(failure(ErrCancelled, "synthesis cancelled"))
	s.complete()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-s.workersDone:
	case <-timer.C:
	}
	return nil
}

func (s *AudioStream) watch() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-s.op.ctx.Done():
			s.fail(context.Cause(s.op.ctx))
			s.complete()
			return
		case now := <-ticker.C:
			s.mu.Lock()
			active := (s.output == outputAwaitingFirstAudio || s.output == outputReceivingAudio) && s.input != inputWaitingForSource && !s.queue.pending()
			limit := s.op.client.limits.firstAudio
			if s.output == outputReceivingAudio {
				limit = s.op.client.limits.progress
			}
			stalled := active && now.Sub(s.lastProgress) >= limit
			if !active {
				s.lastProgress = now
			}
			s.mu.Unlock()
			if stalled {
				s.fail(failure(ErrTimeout, "synthesis output stopped making progress"))
			}
		}
	}
}

func (s *AudioStream) run(source TextSource, text, voice, language string, lexicon []PronunciationEntry) {
	defer close(s.workersDone)
	stub, ctx, err := s.op.client.prepare(s.op.ctx)
	if err != nil {
		s.fail(err)
		return
	}
	format, rate := "audio/pcm", int32(24000)
	header := &pb.StreamingSynthesisRequest{Payload: &pb.StreamingSynthesisRequest_Header{Header: &pb.SynthesisRequest{Speaker: &voice, Language: &language, AudioParameters: &pb.AudioParameters{AudioFormat: &format, SamplingRate: &rate}}}}
	if s.timestampsRequested {
		header.GetHeader().Timestamps = &pb.TimestampOptions{Enable: true}
	}
	for _, entry := range lexicon {
		header.GetHeader().CustomLexicon = append(header.GetHeader().CustomLexicon, &pb.PronunciationEntry{Spelling: entry.Spelling, Pronunciation: entry.Pronunciation})
	}
	if source == nil {
		header.GetHeader().Text = text
		s.mu.Lock()
		s.input = inputFinished
		s.output = outputAwaitingFirstAudio
		s.lastProgress = time.Now()
		s.mu.Unlock()
		call, err := stub.Synthesize(ctx, header.GetHeader())
		if err == nil {
			err = s.receive(call)
		}
		if err != nil {
			s.fail(err)
		} else {
			s.queue.finish()
		}
		return
	}
	call, err := stub.SynthesizeStreaming(ctx)
	if err != nil {
		s.fail(err)
		return
	}
	// Receive concurrently with sending the header. Some services send headers only after text.
	result := make(chan error, 2)
	go func() { result <- s.receive(call) }()
	go func() {
		if err := call.Send(header); err != nil {
			result <- s.sendError(call, err)
			return
		}
		result <- s.sendError(call, s.produce(call, source))
	}()
	for count := 0; count < 2; count++ {
		err := <-result
		if err != nil {
			s.fail(err)
		}
	}
	if s.op.ctx.Err() == nil {
		s.queue.finish()
	}
}

// A send can return io.EOF before Recv returns the final server status.
// Let the sole receiver preserve that status instead of replacing it with a write error.
func (s *AudioStream) sendError(_ pb.TextToSpeech_SynthesizeStreamingClient, err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (s *AudioStream) produce(call pb.TextToSpeech_SynthesizeStreamingClient, source TextSource) error {
	detector, err := sentences.New(s.op.ctx)
	if err != nil {
		return &Error{Kind: ErrInput, Message: "sentence detector initialization failed", Cause: err}
	}
	defer detector.Close()
	buffer := sentences.NewBuffer(detector)
	meaningful := false
	emit := func(sentence string) error {
		s.mu.Lock()
		switch s.output {
		case outputAwaitingText:
			s.output = outputAwaitingFirstAudio
			s.lastProgress = time.Now()
		case outputAwaitingTextAfterAudio:
			s.output = outputReceivingAudio
			s.lastProgress = time.Now()
		}
		s.mu.Unlock()
		return call.Send(&pb.StreamingSynthesisRequest{Payload: &pb.StreamingSynthesisRequest_TextChunk{TextChunk: sentence}})
	}
	for {
		s.mu.Lock()
		s.input = inputWaitingForSource
		s.mu.Unlock()
		fragment, sourceErr := source(s.op.ctx)
		s.mu.Lock()
		s.input = inputProcessing
		s.mu.Unlock()
		if s.op.ctx.Err() != nil {
			return context.Cause(s.op.ctx)
		}
		if sourceErr != nil && sourceErr != io.EOF {
			return &Error{Kind: ErrInput, Message: "text source failed", Cause: sourceErr}
		}
		if !utf8.ValidString(fragment) {
			return failure(ErrInput, "text source must return valid UTF-8")
		}
		if strings.TrimSpace(fragment) != "" {
			meaningful = true
		}
		if err := buffer.Feed(s.op.ctx, fragment, false, emit); err != nil {
			return sentenceError(err)
		}
		if sourceErr == io.EOF {
			break
		}
	}
	if !meaningful {
		return failure(ErrInput, "text source contained no meaningful text")
	}
	if err := buffer.Feed(s.op.ctx, "", true, emit); err != nil {
		return sentenceError(err)
	}
	// Mark input complete before half-close can unblock the server's final response.
	s.mu.Lock()
	s.input = inputFinished
	s.mu.Unlock()
	return s.sendError(call, call.CloseSend())
}

func sentenceError(err error) error {
	var limit sentences.LimitError
	if errors.As(err, &limit) {
		return &Error{Kind: ErrResourceLimit, Message: err.Error(), Cause: err}
	}
	return err
}

func (s *AudioStream) receive(call grpc.ServerStreamingClient[pb.SynthesisResponseStream]) error {
	headers, err := call.Header()
	s.setID(requestID(headers, nil))
	if err != nil {
		s.setID(requestID(nil, call.Trailer()))
		return err
	}
	validFormat := len(headers.Get("x-rime-audio-content-type")) > 0 && headers.Get("x-rime-audio-content-type")[0] == "audio/pcm"
	converter := converter{format: s.format}
	for {
		response, err := call.Recv()
		if err != nil {
			s.setID(requestID(nil, call.Trailer()))
			if err != io.EOF {
				return err
			}
			if !validFormat {
				return failure(ErrAudioFormat, "expected raw audio/pcm from the service")
			}
			s.mu.Lock()
			if s.input != inputFinished {
				s.mu.Unlock()
				return failure(ErrStream, "service completed before input finished")
			}
			s.output = outputFinished
			s.mu.Unlock()
			last, err := converter.process(nil, true)
			if err != nil {
				return err
			}
			return s.queue.put(s.op.ctx, last)
		}
		if s.timestampsRequested {
			s.mu.Lock()
			switch payload := response.Payload.(type) {
			case *pb.SynthesisResponseStream_Trailer:
				s.timestampTrailer.accept(payload.Trailer)
			case *pb.SynthesisResponseStream_Audio:
				if s.timestampTrailer.seen {
					s.timestampTrailer.invalid = true
				}
			}
			s.mu.Unlock()
		}
		data := response.GetAudio()
		if len(data) == 0 {
			continue
		}
		if !validFormat {
			return failure(ErrAudioFormat, "expected raw audio/pcm from the service")
		}
		s.mu.Lock()
		if s.output == outputAwaitingText || s.output == outputAwaitingTextAfterAudio {
			s.output = outputAwaitingTextAfterAudio
		} else {
			s.output = outputReceivingAudio
		}
		s.lastProgress = time.Now()
		s.mu.Unlock()
		output, err := converter.process(data, false)
		if err != nil {
			return err
		}
		if err := s.queue.put(s.op.ctx, output); err != nil {
			return err
		}
	}
}
