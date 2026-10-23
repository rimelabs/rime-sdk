package rime

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"
)

// ConversationSource supplies complete PCM chunks. Read must return on cancellation.
// The application owns the source and its device or file.
type ConversationSource interface {
	Read(context.Context) (AudioChunk, error)
}

// ConversationOutput is one independent playback receipt. Write and End must not
// block. Completion reports measured played milliseconds, even after interruption.
// Cancellation releases the wait without changing the device receipt.
// Completion runs concurrently with Write, End, and playback Interrupt or Stop.
// Do not hold a lock across the wait if those operations need the same lock.
type ConversationOutput interface {
	Write(AudioChunk) error
	End() error
	Completion(context.Context) (float64, error)
}

// ConversationPlayback queues outputs in Open order. Interrupt discards selected
// audio as a batch and resumes the remaining FIFO queue. Stop confirms all audio
// has stopped. It must finish cancellation cleanup before returning. Adapter
// resources remain owned by the application.
// Open, Interrupt, and Stop are serialized by the runner, but can overlap
// outstanding Completion calls. Source.Read also runs independently.
type ConversationPlayback interface {
	Open(OutputRef) (ConversationOutput, error)
	Interrupt([]OutputRef) error
	Stop(context.Context) error
}

// ConversationRunner owns a session's active lifetime and its event consumer.
// Applications execute tools and call session methods from their own goroutines.
// OnEvent runs inline and must not block. Run closes the session on every exit.
type ConversationRunner struct {
	Session  *RealtimeSession
	Source   ConversationSource
	Playback ConversationPlayback
	OnEvent  func(SessionEvent) error
}
type rtReceipt struct {
	output OutputRef
	played float64
	err    error
}
type rtPlayOutput struct {
	ref                   OutputRef
	stream                ConversationOutput
	sealed, measured, cut bool
	played                float64
}
type rtPlayReply struct {
	ref                           ResponseRef
	outputs                       []*rtPlayOutput
	ended, interrupted, reporting bool
	stopReport                    context.CancelFunc
}

// Run ends at source EOF, session closure, failure, or cancellation. It waits for
// adapters and session work to stop. Limits: 64 replies, 128 outputs, 256 pending
// operations. All adapters must honor cancellation.
// The five-second Stop context is cooperative, not a hard return deadline.
// Run keeps waiting for adapter cleanup; an adapter that ignores cancellation
// can prevent Run from returning. Go cannot forcibly stop an adapter goroutine.
func (r *ConversationRunner) Run(ctx context.Context) (result error) {
	if ctx == nil || r.Session == nil || r.Source == nil || r.Playback == nil {
		return failure(ErrInput, "runner requires context, session, source, and playback")
	}
	running, cancel := context.WithCancel(ctx)
	receipts, stopReceipts := context.WithCancel(context.Background())
	var tasks sync.WaitGroup
	failures := make(chan error, 256)
	events := make(chan SessionEvent, 1)
	measured := make(chan rtReceipt, 128)
	reported := make(chan string, 64)
	slots := make(chan struct{}, 256)
	start := func(work func() error) error {
		select {
		case slots <- struct{}{}:
		default:
			return failure(ErrResourceLimit, "conversation pending operation limit exceeded")
		}
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			defer func() { <-slots }()
			if err := work(); err != nil {
				if errors.Is(err, errRealtimeClosed) {
					err = io.EOF
				}
				if running.Err() != nil && errors.Is(err, context.Canceled) {
					return
				}
				select {
				case failures <- err:
				default:
				}
			}
		}()
		return nil
	}
	defer func() {
		cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		err := r.Playback.Stop(shutdown)
		if shutdown.Err() != nil {
			err = operationError(context.Cause(shutdown), "")
		}
		stop()
		stopReceipts()
		_ = r.Session.Close()
		tasks.Wait()
		// Retain adapter failures that raced with EOF or closure.
		for len(failures) > 0 {
			failure := <-failures
			if result == nil && !errors.Is(failure, io.EOF) {
				result = failure
			}
		}
		for len(measured) > 0 {
			receipt := <-measured
			if result == nil && receipt.err == nil && (math.IsNaN(receipt.played) || math.IsInf(receipt.played, 0) || receipt.played < 0) {
				result = failure(ErrInput, "playback measurement must be finite and nonnegative")
			}
			if result == nil && receipt.err != nil && !errors.Is(receipt.err, context.Canceled) {
				result = receipt.err
			}
		}
		if result == nil && ctx.Err() != nil {
			result = operationError(context.Cause(ctx), "")
		}
		if result == nil {
			result = err
		}
	}()
	_ = start(func() error {
		for {
			chunk, err := r.Source.Read(running)
			if len(chunk.Data) > 0 {
				if e := r.Session.SendAudio(running, chunk); e != nil {
					return e
				}
			}
			if err != nil {
				return err
			}
		}
	})
	_ = start(func() error {
		for {
			event, err := r.Session.Recv(running)
			if err != nil {
				return err
			}
			select {
			case events <- event:
			case <-running.Done():
				return running.Err()
			}
		}
	})
	replies := []*rtPlayReply{}
	parents := map[string]string{}
	blocked := map[string]bool{}
	history := []string{}
	wasInterrupted := func(id string) bool {
		for i := 0; id != "" && i < 128; i++ {
			if blocked[id] {
				return true
			}
			id = parents[id]
		}
		return false
	}
	end := func(o *rtPlayOutput) error {
		if o.sealed {
			return nil
		}
		o.sealed = true
		return o.stream.End()
	}
	cut := func(o *rtPlayOutput) error {
		if o.cut || !o.measured {
			return nil
		}
		o.cut = true
		if o.played > math.MaxUint32 {
			return failure(ErrInput, "interrupted playback exceeds uint32 milliseconds")
		}
		return start(func() error {
			e := r.Session.ReportPlayback(running, PlaybackInterrupted{o.ref, uint32(o.played)})
			var f *RealtimeFault
			if errors.As(e, &f) && f.Scope == "event" && f.Code == "truncate_not_current" {
				return nil
			}
			return e
		})
	}
	interrupt := func(selected []*rtPlayReply) error {
		var outputs []OutputRef
		for _, reply := range selected {
			if reply.interrupted {
				continue
			}
			reply.interrupted = true
			if reply.stopReport != nil {
				reply.stopReport()
			}
			blocked[reply.ref.responseID] = true
			for _, o := range reply.outputs {
				outputs = append(outputs, o.ref)
			}
		}
		if len(outputs) > 0 {
			if e := r.Playback.Interrupt(outputs); e != nil {
				return e
			}
		}
		for _, reply := range selected {
			for _, o := range reply.outputs {
				if e := end(o); e != nil {
					return e
				}
				if e := cut(o); e != nil {
					return e
				}
			}
		}
		return nil
	}
	for {
		// Later drain tokens must not clear audio still playing in earlier responses.
		for _, reply := range replies {
			all := reply.ended
			total := float64(0)
			for _, o := range reply.outputs {
				all = all && o.measured
				total += o.played
			}
			if !all {
				break
			}
			if reply.reporting {
				break
			}
			reply.reporting = true
			if reply.interrupted {
				select {
				case reported <- reply.ref.responseID:
				default:
					return failure(ErrResourceLimit, "conversation report overflow")
				}
			} else {
				ref := reply.ref
				reportCtx, stopReport := context.WithCancel(running)
				reply.stopReport = stopReport
				if e := start(func() error {
					defer stopReport()
					e := r.Session.ReportPlayback(reportCtx, PlaybackFinished{Response: ref, PlayedMS: &total})
					if reportCtx.Err() != nil && errors.Is(e, context.Canceled) {
						e = nil
					}
					if e == nil {
						select {
						case reported <- ref.responseID:
						case <-running.Done():
						}
					}
					return e
				}); e != nil {
					stopReport()
					return e
				}
			}
			break
		}
		select {
		case <-ctx.Done():
			return operationError(context.Cause(ctx), "")
		case err := <-failures:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case id := <-reported:
			for i, reply := range replies {
				if reply.ref.responseID == id {
					replies = append(replies[:i], replies[i+1:]...)
					break
				}
			}
		case receipt := <-measured:
			if receipt.err != nil {
				return receipt.err
			}
			if math.IsNaN(receipt.played) || math.IsInf(receipt.played, 0) || receipt.played < 0 {
				return failure(ErrInput, "playback measurement must be finite and nonnegative")
			}
			for _, reply := range replies {
				for _, o := range reply.outputs {
					if o.ref == receipt.output {
						o.measured = true
						o.played = receipt.played
						if reply.interrupted {
							if e := cut(o); e != nil {
								return e
							}
						}
					}
				}
			}
		case event := <-events:
			switch p := event.Payload.(type) {
			case ResponseStarted:
				if len(replies) >= 64 {
					return failure(ErrResourceLimit, "conversation response limit exceeded")
				}
				id := p.Response.responseID
				parent := ""
				if p.Parent != nil {
					parent = p.Parent.responseID
				}
				parents[id] = parent
				history = append(history, id)
				if len(history) > 128 {
					delete(parents, history[0])
					delete(blocked, history[0])
					history = history[1:]
				}
				interrupted := wasInterrupted(id)
				replies = append(replies, &rtPlayReply{ref: p.Response, interrupted: interrupted})
				if interrupted {
					blocked[id] = true
					if e := start(func() error { return r.Session.Cancel(running, p.Response) }); e != nil {
						return e
					}
				}
			case SpeechStarted:
				if r.Session.Info().InterruptOnSpeech {
					if e := interrupt(replies); e != nil {
						return e
					}
				}
			case ResponseAbandoned:
				blocked[p.Response.responseID] = true
				var selected []*rtPlayReply
				for _, reply := range replies {
					if wasInterrupted(reply.ref.responseID) {
						selected = append(selected, reply)
					}
				}
				if e := interrupt(selected); e != nil {
					return e
				}
			case ResponseEnded:
				for _, reply := range replies {
					if reply.ref == p.Response {
						reply.ended = true
						for _, o := range reply.outputs {
							if e := end(o); e != nil {
								return e
							}
						}
					}
				}
			default:
				var ref OutputRef
				var audio *AudioChunk
				seal := false
				switch p := event.Payload.(type) {
				case MessageStarted:
					ref = p.Output
				case AudioDelta:
					ref = p.Output
					audio = &p.Audio
				case AudioDone:
					ref = p.Output
					seal = true
				}
				if ref.itemID != "" {
					for _, reply := range replies {
						if reply.ref != ref.response || reply.ended {
							continue
						}
						var output *rtPlayOutput
						count := 0
						for _, q := range replies {
							count += len(q.outputs)
						}
						for _, o := range reply.outputs {
							if o.ref == ref {
								output = o
								break
							}
						}
						if output == nil {
							if count >= 128 {
								return failure(ErrResourceLimit, "conversation output limit exceeded")
							}
							stream, e := r.Playback.Open(ref)
							if e != nil {
								return e
							}
							output = &rtPlayOutput{ref: ref, stream: stream}
							reply.outputs = append(reply.outputs, output)
							if e = start(func() error {
								played, err := stream.Completion(receipts)
								measured <- rtReceipt{ref, played, err}
								return nil
							}); e != nil {
								return e
							}
							if reply.interrupted {
								if e = r.Playback.Interrupt([]OutputRef{ref}); e != nil {
									return e
								}
								if e = end(output); e != nil {
									return e
								}
							}
						}
						if audio != nil && !reply.interrupted && !output.sealed {
							if e := output.stream.Write(*audio); e != nil {
								return e
							}
						}
						if seal {
							if e := end(output); e != nil {
								return e
							}
						}
					}
				}
			}
			if r.OnEvent != nil {
				if e := r.OnEvent(event); e != nil {
					return e
				}
			}
		}
	}
}
