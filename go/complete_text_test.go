package rime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
