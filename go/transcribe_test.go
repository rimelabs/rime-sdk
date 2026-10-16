package rime

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type unaryRecognitionService struct {
	pb.UnimplementedSpeechToTextServer
	handle func(context.Context, *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error)
	calls  atomic.Int32
}

func (s *unaryRecognitionService) Transcribe(ctx context.Context, request *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error) {
	s.calls.Add(1)
	return s.handle(ctx, request)
}

func setupUnaryRecognition(t *testing.T, handle func(context.Context, *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error)) (*Client, *unaryRecognitionService) {
	t.Helper()
	service := &unaryRecognitionService{handle: handle}
	client := setupConfiguredServices(t, Config{APIKey: "test-key", Timeout: time.Nanosecond}, func(server *grpc.Server) {
		pb.RegisterSpeechToTextServer(server, service)
	})
	return client, service
}

func TestTranscribeForwardsRecording(t *testing.T) {
	audio := append([]byte("RIFF"), make([]byte, 256*1024)...)
	client, service := setupUnaryRecognition(t, func(ctx context.Context, request *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if md.Get("authorization")[0] != "Bearer test-key" || !bytes.Equal(request.Audio, audio) || request.Options.GetLanguage() != "en-GB" || request.Options.Mode != pb.TranscriptionMode_TRANSCRIPTION_MODE_VERBATIM || request.Options.ContextTerms[0] != " Super-G " {
			t.Error("unary request changed")
		}
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-request-id", "unary-request"))
		return &pb.TranscriptionResponse{Text: "Ice cream", Language: &pb.ResolvedLanguage{Tag: "en", Source: pb.LanguageSource_LANGUAGE_SOURCE_SELECTED}}, nil
	})
	result, err := client.STT.Transcribe(context.Background(), audio, TranscribeOptions{Language: "en-GB", Mode: TranscriptionVerbatim, ContextTerms: []string{" Super-G "}})
	if err != nil || result != (Transcription{Text: "Ice cream", Language: "en", RequestID: "unary-request"}) || service.calls.Load() != 1 {
		t.Fatalf("result=%+v error=%v calls=%d", result, err, service.calls.Load())
	}
}

func TestTranscribeFailureAndSilence(t *testing.T) {
	for _, mode := range []string{"reject", "missing-language", "silence"} {
		t.Run(mode, func(t *testing.T) {
			client, service := setupUnaryRecognition(t, func(ctx context.Context, request *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error) {
				_ = grpc.SetTrailer(ctx, metadata.Pairs("x-request-id", "unary-request"))
				if request.Options.Mode != pb.TranscriptionMode_TRANSCRIPTION_MODE_WRITTEN {
					t.Error("expected written default")
				}
				if mode == "reject" {
					return nil, status.Error(codes.Unavailable, "test rejection")
				}
				result := &pb.TranscriptionResponse{}
				if mode == "silence" {
					result.Language = &pb.ResolvedLanguage{Tag: "en", Source: pb.LanguageSource_LANGUAGE_SOURCE_SELECTED}
				}
				return result, nil
			})
			result, err := client.STT.Transcribe(context.Background(), []byte("wav"), TranscribeOptions{Language: "en"})
			if mode == "silence" {
				if err != nil || result.Text != "" || result.Language != "en" {
					t.Fatalf("%+v %v", result, err)
				}
			} else {
				kind := ErrUnavailable
				if mode == "missing-language" {
					kind = ErrStream
				}
				var sdkError *Error
				if !errors.Is(err, kind) || !errors.As(err, &sdkError) || sdkError.RequestID != "unary-request" {
					t.Fatalf("%v", err)
				}
			}
			if service.calls.Load() != 1 {
				t.Fatal("request was retried")
			}
		})
	}
}

func TestTranscribeCancellation(t *testing.T) {
	for _, action := range []string{"caller", "close", "deadline", "rpc-deadline"} {
		t.Run(action, func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			client, _ := setupUnaryRecognition(t, func(ctx context.Context, _ *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error) {
				_ = grpc.SendHeader(ctx, metadata.Pairs("x-request-id", "unary-request"))
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			})
			options := TranscribeOptions{Language: "en"}
			budget := 80 * time.Millisecond
			if action == "deadline" {
				options.Timeout = &budget
			}
			if action == "rpc-deadline" {
				client.sttLimits.completion = budget
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { _, err := client.STT.Transcribe(ctx, []byte("wav"), options); finished <- err }()
			awaitSignal(t, started)
			time.Sleep(10 * time.Millisecond)
			if action == "caller" {
				cancel()
			}
			if action == "close" {
				client.Close()
			}
			err := <-finished
			kind := ErrCancelled
			if action == "deadline" || action == "rpc-deadline" {
				kind = ErrTimeout
			}
			var sdkError *Error
			if !errors.Is(err, kind) || !errors.As(err, &sdkError) || sdkError.RequestID != "unary-request" {
				t.Fatalf("%v", err)
			}
			awaitSignal(t, stopped)
		})
	}
}

func TestTranscribeValidation(t *testing.T) {
	client, service := setupUnaryRecognition(t, func(context.Context, *pb.TranscriptionRequest) (*pb.TranscriptionResponse, error) {
		t.Error("unexpected request")
		return nil, nil
	})
	for _, test := range []struct {
		audio   []byte
		options TranscribeOptions
		kind    error
	}{
		{nil, TranscribeOptions{Language: "en"}, ErrAudioFormat},
		{make([]byte, maxTranscriptionWAVBytes+1), TranscribeOptions{Language: "en"}, ErrResourceLimit},
		{[]byte("wav"), TranscribeOptions{Language: "en", Mode: "other"}, ErrInput},
	} {
		if _, err := client.STT.Transcribe(context.Background(), test.audio, test.options); !errors.Is(err, test.kind) {
			t.Fatalf("%v", err)
		}
	}
	if service.calls.Load() != 0 {
		t.Fatal("unexpected request")
	}
}
