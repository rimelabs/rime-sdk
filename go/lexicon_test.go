package rime

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLexiconSnapshotAndRequestIsolation(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		for _, format := range []AudioFormat{PCM24000, MULAW8000} {
			t.Run(format.Encoding()+map[bool]string{false: "/complete", true: "/incremental"}[incremental], func(t *testing.T) {
				headers := make(chan *pb.SynthesisRequest, 2)
				client := setupClient(t, &testService{header: func(request *pb.SynthesisRequest) error {
					headers <- request
					return nil
				}})
				entries := []PronunciationEntry{{"Hello", `h @ . " l oU`}, {"cafe\u0301 au lait", `" k { S`}, {"Hello", `" k { S`}}
				expected := append([]PronunciationEntry(nil), entries...)
				options := SynthesisOptions{CustomLexicon: entries, AudioFormat: format}
				var stream *AudioStream
				var err error
				if incremental {
					parts := []string{"Hello. ", "Hello again."}
					stream, err = client.TTS.StreamSource(context.Background(), func(context.Context) (string, error) {
						if len(parts) == 0 {
							return "", io.EOF
						}
						part := parts[0]
						parts = parts[1:]
						return part, nil
					}, options)
				} else {
					stream, err = client.TTS.Stream(context.Background(), "Hello. Hello again.", options)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				entries[0].Spelling = "changed"
				entries[1].Pronunciation = "changed"
				if audio, err := drain(stream); err != nil || len(audio) == 0 {
					t.Fatalf("audio=%d, err=%v", len(audio), err)
				}
				request := <-headers
				var actual []PronunciationEntry
				for _, entry := range request.CustomLexicon {
					actual = append(actual, PronunciationEntry{entry.Spelling, entry.Pronunciation})
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("lexicon: %+v", actual)
				}
				plain := mustStream(t, client, "Hello.", SynthesisOptions{})
				if _, err := drain(plain); err != nil {
					t.Fatal(err)
				}
				if len((<-headers).CustomLexicon) != 0 {
					t.Fatal("lexicon leaked into another request")
				}
			})
		}
	}
}

func TestLexiconInvalidUTF8BeforeNetwork(t *testing.T) {
	service := &testService{}
	client := setupClient(t, service)
	for _, entry := range []PronunciationEntry{{"\xff", "h"}, {"hello", "\xff"}} {
		_, err := client.TTS.Stream(context.Background(), "Hello.", SynthesisOptions{CustomLexicon: []PronunciationEntry{entry}})
		if !errors.Is(err, ErrInput) || service.calls.Load() != 0 {
			t.Fatalf("%v", err)
		}
	}
}

func TestPronunciationRejectionPreservesMessageAndRequestID(t *testing.T) {
	messages := []string{
		`custom-lexicon entry "hello": "h @ . l oU" is not well-formed (no-primary-stress)`,
		`custom-lexicon entry "hello": "q" is not well-formed (unknown-phone); custom-lexicon entry "": "h" is not well-formed (empty-spelling)`,
		"custom lexicon is not supported by this model",
		`custom lexicon is not supported for language "ja"`,
		"custom lexicon has 501 entries; the maximum is 500",
	}
	for _, withHeaders := range []bool{false, true} {
		for _, message := range messages {
			t.Run(map[bool]string{false: "trailers/", true: "headers/"}[withHeaders]+message, func(t *testing.T) {
				service := &testService{
					header: func(request *pb.SynthesisRequest) error {
						if len(request.CustomLexicon) != 1 || request.CustomLexicon[0].Pronunciation != "h @ . l oU" {
							return status.Error(codes.Internal, "lexicon was not forwarded")
						}
						return nil
					},
					synthesis: func(rpc grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
						rpc.SetTrailer(metadata.Pairs("x-request-id", "rejected-request"))
						if withHeaders {
							if err := rpc.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "test-request")); err != nil {
								return err
							}
						}
						return status.Error(codes.InvalidArgument, message)
					},
				}
				client := setupClient(t, service)
				stream, err := client.TTS.StreamSource(context.Background(), func(ctx context.Context) (string, error) {
					<-ctx.Done()
					return "", context.Cause(ctx)
				}, SynthesisOptions{CustomLexicon: []PronunciationEntry{{"hello", "h @ . l oU"}}})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				audio, err := drain(stream)
				var sdkError *Error
				id := "rejected-request"
				if withHeaders {
					id = "test-request"
				}
				if len(audio) != 0 || !errors.Is(err, ErrInput) || !errors.As(err, &sdkError) || sdkError.Message != message || sdkError.RequestID != id {
					t.Fatalf("audio=%d err=%+v", len(audio), err)
				}
				if service.calls.Load() != 1 {
					t.Fatal("rejection was retried")
				}
			})
		}
	}
}

func TestServiceMessageFallback(t *testing.T) {
	for _, message := range []string{"", "   "} {
		err := operationError(status.Error(codes.InvalidArgument, message), "id")
		if err.Message != "operation failed" || err.RequestID != "id" || !errors.Is(err, ErrInput) {
			t.Fatalf("%+v", err)
		}
	}
	// A non-gRPC cause is retained without exposing an arbitrary internal message.
	err := operationError(errors.New("internal detail"), "id")
	if err.Message != "operation failed" || err.Cause == nil {
		t.Fatalf("%+v", err)
	}
}

func TestDiscoveryPreservesServiceMessage(t *testing.T) {
	message := `unsupported language "xx"`
	client := setupClient(t, &testService{discovery: func(ctx context.Context) error {
		grpc.SetTrailer(ctx, metadata.Pairs("x-request-id", "discovery-request"))
		return status.Error(codes.InvalidArgument, message)
	}})
	_, err := client.Voices.List(context.Background(), VoiceListOptions{Language: "xx"})
	var sdkError *Error
	if !errors.Is(err, ErrInput) || !errors.As(err, &sdkError) || sdkError.Message != message || sdkError.RequestID != "discovery-request" {
		t.Fatalf("%+v", err)
	}
}
