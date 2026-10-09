package rime

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
)

// TranscriptionStream delivers replacement snapshots through one consumer.
// Close can run concurrently with Recv and cancels only this utterance.
type TranscriptionStream struct {
	op                                             *operation
	queue                                          *transcriptQueue
	config                                         *pb.StreamingConfig
	format                                         PCMFormat
	limits                                         transcriptionLimits
	timeout                                        time.Duration
	reading, inputDone                             atomic.Bool
	accepted, workersDone                          chan struct{}
	finishOnce                                     sync.Once
	mu                                             sync.Mutex
	source                                         AudioSource
	started, stopped                               bool
	id                                             string
	stopCancellation                               func() bool
	overallTimer, acceptanceTimer, completionTimer *time.Timer
}

// RequestID returns the service request identifier once headers or trailers arrive.
func (stream *TranscriptionStream) RequestID() string {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.id
}
func (stream *TranscriptionStream) setRequestID(id string) {
	if id == "" {
		return
	}
	stream.mu.Lock()
	stream.id = id
	stream.mu.Unlock()
}

// Recv starts the operation lazily and returns the next complete snapshot.
// After a final result it returns io.EOF. Concurrent Recv calls return ErrInput.
func (stream *TranscriptionStream) Recv() (TranscriptionUpdate, error) {
	if !stream.reading.CompareAndSwap(false, true) {
		return nil, failure(ErrInput, "transcription stream already has a reader")
	}
	defer stream.reading.Store(false)
	stream.mu.Lock()
	if !stream.started && !stream.stopped && stream.op.ctx.Err() == nil {
		stream.started = true
		source := stream.source
		stream.source = nil
		if stream.timeout > 0 {
			stream.overallTimer = time.AfterFunc(stream.timeout, func() { stream.fail(failure(ErrTimeout, "transcription overall timeout")) })
		}
		go stream.run(source)
	}
	stream.mu.Unlock()
	update, err := stream.queue.get(stream.op.ctx)
	if err != nil && err != io.EOF {
		stream.fail(err)
		return nil, operationError(err, stream.RequestID())
	}
	if _, final := update.(TranscriptionFinal); final {
		stream.complete()
	}
	return update, err
}

// Close cancels an unfinished operation and waits up to two seconds for workers.
// An AudioSource that ignores its context cannot be forcibly stopped by the SDK.
func (stream *TranscriptionStream) Close() error {
	stream.fail(failure(ErrCancelled, "transcription stream closed"))
	timer := time.NewTimer(stream.limits.cleanup)
	defer timer.Stop()
	select {
	case <-stream.workersDone:
	case <-timer.C:
	}
	return nil
}
func (stream *TranscriptionStream) complete() {
	stream.finishOnce.Do(func() {
		stream.mu.Lock()
		stream.stopped = true
		stream.source = nil
		for _, timer := range []*time.Timer{stream.overallTimer, stream.acceptanceTimer, stream.completionTimer} {
			if timer != nil {
				timer.Stop()
			}
		}
		if stream.stopCancellation != nil {
			stream.stopCancellation()
		}
		if !stream.started {
			close(stream.workersDone)
		}
		stream.mu.Unlock()
		stream.op.close()
	})
}
func (stream *TranscriptionStream) fail(err error) {
	stream.queue.fail(operationError(err, stream.RequestID()))
	stream.complete()
}

type transcriptionResult struct {
	final TranscriptionFinal
	err   error
}

func (stream *TranscriptionStream) run(source AudioSource) {
	defer close(stream.workersDone)
	conn, ctx, err := stream.op.client.prepareService(stream.op.ctx, transcriptionService)
	if err != nil {
		stream.fail(err)
		return
	}
	rpc, err := pb.NewSpeechToTextClient(conn).TranscribeStreaming(ctx)
	if err != nil {
		stream.fail(err)
		return
	}
	stream.mu.Lock()
	if !stream.stopped {
		stream.acceptanceTimer = time.AfterFunc(stream.limits.acceptance, func() { stream.fail(failure(ErrTimeout, "transcription acceptance timeout")) })
	}
	stream.mu.Unlock()
	results := make(chan transcriptionResult, 2)
	go func() { final, err := stream.receive(rpc); results <- transcriptionResult{final, err} }()
	go func() { results <- transcriptionResult{err: stream.send(rpc, source)} }()
	remaining := 2
	defer func() {
		if remaining == 0 {
			return
		}
		timer := time.NewTimer(stream.limits.cleanup)
		defer timer.Stop()
		for remaining > 0 {
			select {
			case <-results:
				remaining--
			case <-timer.C:
				return
			}
		}
	}()
	var final TranscriptionFinal
	for remaining > 0 {
		select {
		case <-stream.op.ctx.Done():
			stream.fail(context.Cause(stream.op.ctx))
			return
		case result := <-results:
			remaining--
			if result.err != nil {
				stream.fail(result.err)
				return
			}
			if result.final.Language != "" {
				final = result.final
			}
		}
	}
	stream.mu.Lock()
	if stream.completionTimer != nil {
		stream.completionTimer.Stop()
	}
	stream.mu.Unlock()
	if err := stream.queue.put(ctx, final); err != nil {
		stream.fail(err)
	}
}

func (stream *TranscriptionStream) send(rpc grpc.BidiStreamingClient[pb.StreamingTranscriptionRequest, pb.StreamingTranscriptionResponse], source AudioSource) error {
	ctx := stream.op.ctx
	if err := rpc.Send(&pb.StreamingTranscriptionRequest{Payload: &pb.StreamingTranscriptionRequest_Config{Config: stream.config}}); err != nil {
		if err == io.EOF {
			return nil
		} // Recv supplies the terminal gRPC status.
		return err
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-stream.accepted:
	}
	input := newPCMInput(stream.format)
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		data, sourceErr := source(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if sourceErr != nil && !errors.Is(sourceErr, io.EOF) {
			return &Error{Kind: ErrInput, Message: "audio source failed", Cause: sourceErr}
		}
		err := input.feed(ctx, data, func(chunk []byte) error {
			return rpc.Send(&pb.StreamingTranscriptionRequest{Payload: &pb.StreamingTranscriptionRequest_Audio{Audio: chunk}})
		})
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if errors.Is(sourceErr, io.EOF) {
			if err := input.finish(); err != nil {
				return err
			}
			stream.mu.Lock()
			if !stream.stopped {
				stream.completionTimer = time.AfterFunc(stream.limits.completion, func() { stream.fail(failure(ErrTimeout, "transcription completion timeout")) })
			}
			stream.mu.Unlock()
			stream.inputDone.Store(true)
			if err := rpc.CloseSend(); err != io.EOF {
				return err
			}
			return nil
		}
	}
}

func (stream *TranscriptionStream) receive(rpc grpc.BidiStreamingClient[pb.StreamingTranscriptionRequest, pb.StreamingTranscriptionResponse]) (TranscriptionFinal, error) {
	headers, err := rpc.Header()
	stream.setRequestID(requestID(headers, nil))
	defer func() { stream.setRequestID(requestID(headers, rpc.Trailer())) }()
	if err != nil {
		return TranscriptionFinal{}, err
	}
	state := transcriptState{textLimit: stream.limits.transcriptBytes}
	for {
		message, err := rpc.Recv()
		if err == io.EOF {
			return state.finish()
		}
		if err != nil {
			return TranscriptionFinal{}, err
		}
		partial, err := state.accept(message, stream.inputDone.Load())
		if err != nil {
			return TranscriptionFinal{}, err
		}
		if message.GetAccepted() != nil {
			stream.mu.Lock()
			if stream.acceptanceTimer != nil {
				stream.acceptanceTimer.Stop()
			}
			stream.mu.Unlock()
			close(stream.accepted)
		}
		if partial != nil {
			if err := stream.queue.put(stream.op.ctx, *partial); err != nil {
				return TranscriptionFinal{}, err
			}
		}
	}
}
