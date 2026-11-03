package rime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func wordTrailer(code codes.Code, spans ...*pb.SpanTimestamp) *pb.SynthesisResponseStream {
	message := ""
	if code != codes.OK {
		message = "alignment unavailable"
	}
	return &pb.SynthesisResponseStream{Payload: &pb.SynthesisResponseStream_Trailer{Trailer: &pb.SynthesisResponseTrailer{Timestamps: &pb.Timestamps{
		Status: &rpcstatus.Status{Code: int32(code), Message: message}, Spans: spans,
	}}}}
}

func timestampWords() []*pb.SpanTimestamp {
	return []*pb.SpanTimestamp{
		{Text: "twenty", Start: durationpb.New(90 * time.Millisecond), End: durationpb.New(325 * time.Millisecond)},
		{Text: "two", Start: durationpb.New(1125 * time.Millisecond), End: durationpb.New(2 * time.Second)},
	}
}

func timestampService(responses []*pb.SynthesisResponseStream, gate <-chan struct{}, finalError error) *testService {
	service := &testService{
		header: func(h *pb.SynthesisRequest) error {
			if !h.GetTimestamps().GetEnable() {
				return status.Error(codes.InvalidArgument, "timestamps not enabled")
			}
			return nil
		},
	}
	service.synthesis = func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "test-request")); err != nil {
			return err
		}
		for {
			message, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			serviceText := message.GetTextChunk()
			service.mu.Lock()
			service.texts = append(service.texts, serviceText)
			service.mu.Unlock()
			if err := stream.Send(audioResponse(bytes.Repeat([]byte{1, 0}, 2400))); err != nil {
				return err
			}
		}
		if gate != nil {
			select {
			case <-gate:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		for _, response := range responses {
			if err := stream.Send(response); err != nil {
				return err
			}
		}
		return finalError
	}
	return service
}

func timestampClient(t *testing.T, model string, service *testService) *Client {
	t.Helper()
	return setupConfiguredServices(t, Config{APIKey: "test-key", Model: model}, func(server *grpc.Server) { pb.RegisterTextToSpeechServer(server, service) })
}

func TestTimestampsPreserveAudioAndSynthesisOffsets(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			for _, format := range []AudioFormat{PCM24000, MULAW8000} {
				t.Run(format.Encoding(), func(t *testing.T) {
					service := timestampService([]*pb.SynthesisResponseStream{wordTrailer(codes.OK, timestampWords()...)}, nil, nil)
					client := timestampClient(t, model, service)
					parts := []string{"First. ", "Twenty two."}
					stream, err := client.TTS.Stream(context.Background(), func(context.Context) (string, error) {
						if len(parts) == 0 {
							return "", io.EOF
						}
						text := parts[0]
						parts = parts[1:]
						return text, nil
					}, SynthesisOptions{Timestamps: true, AudioFormat: format})
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
					audio, err := drain(stream)
					if err != nil {
						t.Fatal(err)
					}
					converter := converter{format: format}
					expected, err := converter.process(bytes.Repeat([]byte{1, 0}, 4800), true)
					if err != nil || !bytes.Equal(audio, expected) {
						t.Fatalf("audio mismatch: %d bytes, %v", len(audio), err)
					}
					result, err := stream.Timestamps()
					want := TimestampResult{Status: TimestampStatus{Code: codes.OK}, Spans: []WordTimestamp{{Text: "twenty", Start: .09, End: .325}, {Text: "two", Start: 1.125, End: 2}}}
					if err != nil || !reflect.DeepEqual(result, want) {
						t.Fatalf("%+v %v", result, err)
					}
					result.Spans[0].Text = "changed"
					stream.Close()
					client.Close()
					result, err = stream.Timestamps()
					if err != nil || !reflect.DeepEqual(result, want) {
						t.Fatalf("result changed: %+v %v", result, err)
					}
				})
			}
		})
	}
}

func TestTimestampAlignmentStatusKeepsAudio(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			for _, code := range []codes.Code{codes.OK, codes.Canceled, codes.InvalidArgument, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Unimplemented, codes.Internal, codes.Unavailable} {
				t.Run(code.String(), func(t *testing.T) {
					client := timestampClient(t, model, timestampService([]*pb.SynthesisResponseStream{wordTrailer(code)}, nil, nil))
					stream := mustStream(t, client, "Hello.", SynthesisOptions{Timestamps: true})
					audio, err := drain(stream)
					if err != nil || len(audio) != 4800 {
						t.Fatalf("audio %d: %v", len(audio), err)
					}
					result, err := stream.Timestamps()
					if err != nil || result.Status.Code != code || result.Status.Message != wordTrailer(code).GetTrailer().GetTimestamps().GetStatus().GetMessage() || len(result.Spans) != 0 {
						t.Fatalf("%+v %v", result, err)
					}
				})
			}
		})
	}
}

func TestBadTimestampResultDoesNotFailAudio(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			cases := map[string][]*pb.SynthesisResponseStream{
				"missing":             nil,
				"empty trailer":       {{Payload: &pb.SynthesisResponseStream_Trailer{Trailer: &pb.SynthesisResponseTrailer{}}}},
				"missing status":      {{Payload: &pb.SynthesisResponseStream_Trailer{Trailer: &pb.SynthesisResponseTrailer{Timestamps: &pb.Timestamps{}}}}},
				"duplicate":           {wordTrailer(codes.OK), wordTrailer(codes.OK)},
				"audio after trailer": {wordTrailer(codes.OK), audioResponse([]byte{0, 0})},
				"failed with spans":   {wordTrailer(codes.Unavailable, timestampWords()...)},
				"missing durations":   {wordTrailer(codes.OK, &pb.SpanTimestamp{Text: "missing"})},
				"negative":            {wordTrailer(codes.OK, &pb.SpanTimestamp{Text: "negative", Start: &durationpb.Duration{Nanos: -1}, End: &durationpb.Duration{}})},
				"invalid nanos":       {wordTrailer(codes.OK, &pb.SpanTimestamp{Text: "invalid", Start: &durationpb.Duration{}, End: &durationpb.Duration{Nanos: 1000000000}})},
				"reversed":            {wordTrailer(codes.OK, &pb.SpanTimestamp{Text: "reversed", Start: durationpb.New(2 * time.Second), End: durationpb.New(time.Second)})},
			}
			for name, responses := range cases {
				t.Run(name, func(t *testing.T) {
					client := timestampClient(t, model, timestampService(responses, nil, nil))
					stream := mustStream(t, client, "Hello.", SynthesisOptions{Timestamps: true})
					audio, err := drain(stream)
					if err != nil || len(audio) < 4800 {
						t.Fatalf("audio %d: %v", len(audio), err)
					}
					_, err = stream.Timestamps()
					var sdkError *Error
					if !errors.As(err, &sdkError) || !errors.Is(err, ErrStream) || sdkError.RequestID != "test-request" {
						t.Fatalf("%v", err)
					}
				})
			}
		})
	}
}

func TestTimestampOptIn(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			service := &testService{header: func(h *pb.SynthesisRequest) error {
				if h.Timestamps != nil {
					return status.Error(codes.InvalidArgument, "unsolicited timestamps")
				}
				return nil
			}}
			client := timestampClient(t, model, service)
			stream := mustStream(t, client, "Hello.", SynthesisOptions{})
			if _, err := drain(stream); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Timestamps(); !errors.Is(err, ErrInput) {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestAudioBeforeTimestampsAndEarlyAccess(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			gate := make(chan struct{})
			client := timestampClient(t, model, timestampService([]*pb.SynthesisResponseStream{wordTrailer(codes.OK, timestampWords()...)}, gate, nil))
			timeout := 2 * time.Second
			stream := mustStream(t, client, "Hello.", SynthesisOptions{Timestamps: true, Timeout: &timeout})
			if _, err := stream.Timestamps(); !errors.Is(err, ErrInput) {
				t.Fatalf("early access: %v", err)
			}
			if chunk, err := stream.Recv(); err != nil || len(chunk) != 4800 {
				t.Fatalf("%d %v", len(chunk), err)
			}
			if _, err := stream.Timestamps(); !errors.Is(err, ErrInput) {
				t.Fatalf("early access: %v", err)
			}
			close(gate)
			if _, err := drain(stream); err != nil {
				t.Fatal(err)
			}
			if result, err := stream.Timestamps(); err != nil || len(result.Spans) != 2 {
				t.Fatalf("%+v %v", result, err)
			}
		})
	}
}

func TestCancelledTimestamps(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			for _, mode := range []string{"stream", "client", "deadline"} {
				t.Run(mode, func(t *testing.T) {
					client := timestampClient(t, model, timestampService(nil, make(chan struct{}), nil))
					ctx, cancel := context.WithCancelCause(context.Background())
					defer cancel(context.Canceled)
					stream, err := client.TTS.Synthesize(ctx, "Hello.", SynthesisOptions{Timestamps: true})
					if err != nil {
						t.Fatal(err)
					}
					defer stream.Close()
			if _, err := stream.Recv(); err != nil {
						t.Fatal(err)
					}
					expected := ErrCancelled
					switch mode {
					case "stream":
						stream.Close()
					case "client":
						client.Close()
					case "deadline":
						expected = ErrTimeout
						cancel(context.DeadlineExceeded)
					}
					if _, err := drain(stream); !errors.Is(err, expected) {
						t.Fatalf("audio: %v", err)
					}
					if _, err := stream.Timestamps(); !errors.Is(err, expected) {
						t.Fatalf("timestamps: %v", err)
					}
				})
			}
		})
	}
}

func TestFinalRPCFailureOverridesTimestamps(t *testing.T) {
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			client := timestampClient(t, model, timestampService([]*pb.SynthesisResponseStream{wordTrailer(codes.OK, timestampWords()...)}, nil, status.Error(codes.Unavailable, "final failure")))
			stream := mustStream(t, client, "Hello.", SynthesisOptions{Timestamps: true})
			if _, err := drain(stream); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("audio: %v", err)
			}
			if _, err := stream.Timestamps(); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("timestamps: %v", err)
			}
		})
	}
}

func TestCodaPreservesRequestWordsAndDeliveredAudioOffsets(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		t.Run(fmt.Sprint(incremental), func(t *testing.T) {
			spans := []*pb.SpanTimestamp{
				{Text: "22", Start: durationpb.New(0), End: durationpb.New(time.Second)},
				{Text: "LiveKit", Start: durationpb.New(time.Second), End: durationpb.New(2 * time.Second)},
				{Text: "to", Start: durationpb.New(2 * time.Second), End: durationpb.New(2 * time.Second)},
			}
			service := timestampService([]*pb.SynthesisResponseStream{wordTrailer(codes.OK, spans...)}, nil, nil)
			service.header = func(h *pb.SynthesisRequest) error {
				if !h.GetTimestamps().GetEnable() || h.GetLanguage() != "de" || h.GetAudioParameters().GetTimeScaleFactor() != 2 || len(h.CustomLexicon) != 1 || h.CustomLexicon[0].Spelling != "LiveKit" {
					return status.Error(codes.InvalidArgument, "request options were not preserved")
				}
				return nil
			}
			client := timestampClient(t, "coda", service)
			factor := 2.0
			options := SynthesisOptions{Timestamps: true, Language: "de", CustomLexicon: []PronunciationEntry{{"LiveKit", "l aI v k I t"}}, AudioFormat: MULAW8000, TimeScaleFactor: &factor}
			var stream *AudioStream
			var err error
			if incremental {
				parts := []string{"22 Live", "Kit to missing."}
				stream, err = client.TTS.Stream(context.Background(), func(context.Context) (string, error) {
					if len(parts) == 0 {
						return "", io.EOF
					}
					text := parts[0]
					parts = parts[1:]
					return text, nil
				}, options)
			} else {
				stream, err = client.TTS.Synthesize(context.Background(), "22 LiveKit to missing.", options)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			if audio, err := drain(stream); err != nil || len(audio) == 0 {
				t.Fatalf("audio: %d, %v", len(audio), err)
			}
			service.mu.Lock()
			text := strings.Join(service.texts, "")
			service.mu.Unlock()
			if text != "22 LiveKit to missing." {
				t.Fatalf("text: %q", text)
			}
			result, err := stream.Timestamps()
			want := TimestampResult{Status: TimestampStatus{Code: codes.OK}, Spans: []WordTimestamp{{Text: "22", Start: 0, End: 1}, {Text: "LiveKit", Start: 1, End: 2}, {Text: "to", Start: 2, End: 2}}}
			if err != nil || !reflect.DeepEqual(result, want) {
				t.Fatalf("%+v, %v", result, err)
			}
		})
	}
}
