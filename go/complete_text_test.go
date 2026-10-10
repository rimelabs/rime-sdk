package rime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type synthesisRPC = grpc.ServerStreamingServer[pb.SynthesisResponseStream]

func completeTextService(handle func(*pb.SynthesisRequest, synthesisRPC) error) *testService {
	return &testService{completeSynthesis: func(request *pb.SynthesisRequest, rpc synthesisRPC) error {
		if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "complete-request")); err != nil {
			return err
		}
		return handle(request, rpc)
	}}
}

func TestCompleteTextRequest(t *testing.T) {
	requests := make(chan *pb.SynthesisRequest, 1)
	service := &testService{completeSynthesis: func(request *pb.SynthesisRequest, rpc grpc.ServerStreamingServer[pb.SynthesisResponseStream]) error {
		requests <- request
		auth, _ := metadata.FromIncomingContext(rpc.Context())
		if auth.Get("authorization")[0] != "Bearer test-key" {
			return status.Error(codes.Unauthenticated, "bad key")
		}
		if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "complete-request")); err != nil {
			return err
		}
		return rpc.Send(audioResponse([]byte{1, 0, 2, 0}))
	}}
	client := setupClient(t, service)
	entries := []PronunciationEntry{{"read", `" r\ E d`}}
	stream := mustStream(t, client, "Hello. Please read the pages.", SynthesisOptions{CompleteText: true, CustomLexicon: entries})
	entries[0].Spelling = "changed"
	if audio, err := drain(stream); err != nil || len(audio) != 4 {
		t.Fatalf("audio=%v err=%v", audio, err)
	}
	request := <-requests
	if request.Text != "Hello. Please read the pages." || request.CustomLexicon[0].Spelling != "read" || request.AudioParameters.GetSamplingRate() != 24000 || stream.RequestID() != "complete-request" {
		t.Fatalf("request=%v id=%s", request, stream.RequestID())
	}
	if service.calls.Load() != 1 {
		t.Fatal("unexpected additional RPC")
	}
}

func TestCompleteTextRejectionAndRecovery(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(map[bool]string{false: "trailers", true: "headers"}[headers], func(t *testing.T) {
			service := &testService{}
			service.completeSynthesis = func(_ *pb.SynthesisRequest, rpc grpc.ServerStreamingServer[pb.SynthesisResponseStream]) error {
				rpc.SetTrailer(metadata.Pairs("x-request-id", "rejected-request"))
				if headers || service.calls.Load() > 1 {
					if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "complete-request")); err != nil {
						return err
					}
				}
				if service.calls.Load() == 1 {
					return status.Error(codes.InvalidArgument, `custom-lexicon entry "hello": no-primary-stress`)
				}
				return rpc.Send(audioResponse([]byte{1, 0}))
			}
			client := setupClient(t, service)
			stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true})
			audio, err := drain(stream)
			var failure *Error
			if len(audio) != 0 || !errors.Is(err, ErrInput) || !errors.As(err, &failure) || failure.RequestID == "" || !strings.Contains(failure.Message, "no-primary-stress") {
				t.Fatalf("audio=%v err=%v", audio, err)
			}
			if service.calls.Load() != 1 {
				t.Fatal("retried rejection")
			}
			if _, err := drain(mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true})); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompleteTextCancellationAndValidation(t *testing.T) {
	started := make(chan struct{})
	service := &testService{completeSynthesis: func(_ *pb.SynthesisRequest, rpc grpc.ServerStreamingServer[pb.SynthesisResponseStream]) error {
		close(started)
		<-rpc.Context().Done()
		return rpc.Context().Err()
	}}
	client := setupClient(t, service)
	if _, err := client.TTS.StreamSource(context.Background(), func(context.Context) (string, error) { return "Hello.", nil }, SynthesisOptions{CompleteText: true}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := client.TTS.Stream(context.Background(), strings.Repeat("é", 32769), SynthesisOptions{CompleteText: true}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if service.calls.Load() != 0 {
		t.Fatal("invalid input reached server")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.TTS.Stream(ctx, "Hello.", SynthesisOptions{CompleteText: true})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	if _, err := drain(stream); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
}

func TestCompleteTextTimestampsAndAudioProfiles(t *testing.T) {
	for _, format := range []AudioFormat{PCM24000, MULAW8000} {
		t.Run(format.Encoding(), func(t *testing.T) {
			service := &testService{completeSynthesis: func(request *pb.SynthesisRequest, rpc grpc.ServerStreamingServer[pb.SynthesisResponseStream]) error {
				if !request.GetTimestamps().GetEnable() {
					return status.Error(codes.InvalidArgument, "missing timestamps")
				}
				if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
					return err
				}
				if err := rpc.Send(audioResponse(make([]byte, 4800))); err != nil {
					return err
				}
				return rpc.Send(wordTrailer(codes.OK, timestampWords()...))
			}}
			client := timestampClient(t, service)
			stream := mustStream(t, client, "Twenty two.", SynthesisOptions{CompleteText: true, Timestamps: true, AudioFormat: format})
			if _, err := stream.Timestamps(); !errors.Is(err, ErrInput) {
				t.Fatal(err)
			}
			audio, err := drain(stream)
			if err != nil || len(audio) == 0 {
				t.Fatal(err)
			}
			stamps, err := stream.Timestamps()
			if err != nil || len(stamps.Spans) != 2 || stamps.Spans[0].Text != "twenty" {
				t.Fatalf("%+v %v", stamps, err)
			}
		})
	}
}

func TestCompleteTextPartialFailureAndRecovery(t *testing.T) {
	release := make(chan struct{})
	service := completeTextService(func(_ *pb.SynthesisRequest, rpc synthesisRPC) error {
		if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
			return err
		}
		select {
		case <-release:
			return status.Error(codes.Unavailable, "disconnected after audio")
		case <-rpc.Context().Done():
			return rpc.Context().Err()
		}
	})
	client := setupClient(t, service)
	stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true})
	if audio, err := stream.Recv(); err != nil || !bytes.Equal(audio, []byte{1, 0}) {
		t.Fatalf("audio=%v err=%v", audio, err)
	}
	close(release)
	_, err := drain(stream)
	var detail *Error
	if !errors.Is(err, ErrUnavailable) || !errors.As(err, &detail) || detail.Message != "disconnected after audio" || detail.RequestID != "complete-request" {
		t.Fatalf("%v", err)
	}
	if service.calls.Load() != 1 {
		t.Fatal("synthesis was retried")
	}
	// The same client can still use the streaming-input RPC after a failure.
	if audio, err := drain(mustStream(t, client, "Again.", SynthesisOptions{})); err != nil || len(audio) == 0 {
		t.Fatalf("recovery: %v", err)
	}
}

func TestCompleteTextStallTimeoutCancelsRPC(t *testing.T) {
	for _, afterAudio := range []bool{false, true} {
		t.Run(map[bool]string{false: "first audio", true: "progress"}[afterAudio], func(t *testing.T) {
			stopped := make(chan struct{})
			client := setupClient(t, completeTextService(func(_ *pb.SynthesisRequest, rpc synthesisRPC) error {
				defer close(stopped)
				if afterAudio {
					if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
						return err
					}
				}
				<-rpc.Context().Done()
				return rpc.Context().Err()
			}))
			client.limits.firstAudio, client.limits.progress = 100*time.Millisecond, 5*time.Second
			if afterAudio {
				client.limits.firstAudio, client.limits.progress = 5*time.Second, 100*time.Millisecond
			}
			noDeadline := time.Duration(0)
			stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true, Timeout: &noDeadline})
			if afterAudio {
				if _, err := stream.Recv(); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan error, 1)
			go func() { _, err := drain(stream); result <- err }()
			select {
			case err := <-result:
				var detail *Error
				if !errors.Is(err, ErrTimeout) || !errors.As(err, &detail) || detail.RequestID != "complete-request" || detail.Message != "synthesis output stopped making progress" {
					t.Fatalf("%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("stall watchdog did not stop synthesis")
			}
			awaitSignal(t, stopped)
			awaitSignal(t, stream.workersDone)
		})
	}
}

func TestCompleteTextCancellationKeepsActiveSibling(t *testing.T) {
	release, stopped := make(chan struct{}), make(chan struct{})
	client := setupClient(t, completeTextService(func(request *pb.SynthesisRequest, rpc synthesisRPC) error {
		if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
			return err
		}
		if request.Text == "First." {
			defer close(stopped)
			<-rpc.Context().Done()
			return rpc.Context().Err()
		}
		select {
		case <-release:
			return nil
		case <-rpc.Context().Done():
			return rpc.Context().Err()
		}
	}))
	first := mustStream(t, client, "First.", SynthesisOptions{CompleteText: true})
	sibling := mustStream(t, client, "Sibling.", SynthesisOptions{CompleteText: true})
	for _, stream := range []*AudioStream{first, sibling} {
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	first.Close()
	if _, err := first.Recv(); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	awaitSignal(t, stopped)
	close(release)
	if _, err := sibling.Recv(); err != io.EOF {
		t.Fatalf("sibling: %v", err)
	}
}

func TestCompleteTextClientCloseWakesPendingReader(t *testing.T) {
	for _, afterAudio := range []bool{false, true} {
		t.Run(map[bool]string{false: "before audio", true: "after audio"}[afterAudio], func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			client := setupClient(t, completeTextService(func(_ *pb.SynthesisRequest, rpc synthesisRPC) error {
				close(started)
				defer close(stopped)
				if afterAudio {
					if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
						return err
					}
				}
				<-rpc.Context().Done()
				return rpc.Context().Err()
			}))
			stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true})
			if afterAudio {
				if _, err := stream.Recv(); err != nil {
					t.Fatal(err)
				}
			}
			pending := make(chan error, 1)
			go func() { _, err := stream.Recv(); pending <- err }()
			awaitSignal(t, started)
			client.Close()
			select {
			case err := <-pending:
				if !errors.Is(err, ErrCancelled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("client close did not wake reader")
			}
			awaitSignal(t, stopped)
			awaitSignal(t, stream.workersDone)
		})
	}
}

func TestCompleteTextSlowConsumerAndOverallDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		for _, format := range []AudioFormat{PCM24000, MULAW8000} {
			t.Run(map[bool]string{false: "slow consumer/", true: "deadline/"}[deadline]+format.Encoding(), func(t *testing.T) {
				release, stopped := make(chan struct{}), make(chan struct{})
				payload := make([]byte, 6*(96000+1))
				client := setupClient(t, completeTextService(func(_ *pb.SynthesisRequest, rpc synthesisRPC) error {
					defer close(stopped)
					// Keep individual conversions short even under the race detector.
					for offset := 0; offset < len(payload); offset += 4800 {
						if err := rpc.Send(audioResponse(payload[offset:min(offset+4800, len(payload))])); err != nil {
							return err
						}
					}
					select {
					case <-release:
						return nil
					case <-rpc.Context().Done():
						return rpc.Context().Err()
					}
				}))
				client.limits.progress = 50 * time.Millisecond
				timeout := time.Duration(0)
				if deadline {
					timeout = 750 * time.Millisecond
				}
				stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true, AudioFormat: format, Timeout: &timeout})
				first, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				pause := 250 * time.Millisecond
				if deadline {
					pause = timeout + 100*time.Millisecond
				}
				time.Sleep(pause)
				stream.queue.mu.Lock()
				size := stream.queue.bytes
				stream.queue.mu.Unlock()
				if size > 96000 {
					t.Fatalf("unbounded queue: %d", size)
				}
				if deadline {
					if _, err := stream.Recv(); !errors.Is(err, ErrTimeout) {
						t.Fatalf("deadline while paused: %v", err)
					}
				} else {
					close(release)
					audio := append([]byte(nil), first...)
					for {
						chunk, err := stream.Recv()
						if err == io.EOF {
							break
						}
						if err != nil || len(chunk) == 0 || len(chunk) > 9600 {
							t.Fatalf("chunk=%d err=%v", len(chunk), err)
						}
						audio = append(audio, chunk...)
					}
					expected := payload
					if format == MULAW8000 {
						expected = bytes.Repeat([]byte{255}, len(payload)/6)
					}
					if !bytes.Equal(audio, expected) {
						t.Fatalf("audio mismatch: got %d bytes, want %d", len(audio), len(expected))
					}
				}
				awaitSignal(t, stopped)
				awaitSignal(t, stream.workersDone)
			})
		}
	}
}

func TestCompleteTextFinalFailureOverridesTimestamps(t *testing.T) {
	service := completeTextService(func(_ *pb.SynthesisRequest, rpc synthesisRPC) error {
		if err := rpc.Send(audioResponse([]byte{1, 0})); err != nil {
			return err
		}
		if err := rpc.Send(wordTrailer(codes.OK, timestampWords()...)); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "failure after timestamps")
	})
	client := timestampClient(t, service)
	stream := mustStream(t, client, "Hello.", SynthesisOptions{CompleteText: true, Timestamps: true})
	if _, err := drain(stream); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := stream.Timestamps(); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
