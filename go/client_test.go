package rime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testService struct {
	pb.UnimplementedTextToSpeechServer
	synthesis   func(grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error
	discovery   func(context.Context) error
	calls       atomic.Int32
	discoveries atomic.Int32
	mu          sync.Mutex
	texts       []string
}

func (s *testService) SynthesizeStreaming(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
	s.calls.Add(1)
	md, _ := metadata.FromIncomingContext(stream.Context())
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-key" {
		return status.Error(codes.Unauthenticated, "bad key")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader() == nil || first.GetHeader().GetAudioParameters().GetSamplingRate() != 24000 {
		return status.Error(codes.InvalidArgument, "bad header")
	}
	if s.synthesis != nil {
		return s.synthesis(stream)
	}
	if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "test-request")); err != nil {
		return err
	}
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.texts = append(s.texts, message.GetTextChunk())
		s.mu.Unlock()
		if err := stream.Send(audioResponse(bytes.Repeat([]byte{1, 0}, 2400))); err != nil {
			return err
		}
	}
}

func audioResponse(data []byte) *pb.SynthesisResponseStream {
	return &pb.SynthesisResponseStream{Payload: &pb.SynthesisResponseStream_Audio{Audio: data}}
}
func (s *testService) GetSupportedLanguages(ctx context.Context, _ *pb.GetSupportedLanguagesRequest) (*pb.GetSupportedLanguagesResponse, error) {
	s.discoveries.Add(1)
	if s.discovery != nil {
		if err := s.discovery(ctx); err != nil {
			return nil, err
		}
	}
	return &pb.GetSupportedLanguagesResponse{Languages: []string{"en", "de"}}, nil
}
func (s *testService) GetSupportedSpeakers(ctx context.Context, request *pb.GetSupportedSpeakersRequest) (*pb.GetSupportedSpeakersResponse, error) {
	if s.discovery != nil {
		if err := s.discovery(ctx); err != nil {
			return nil, err
		}
	}
	return &pb.GetSupportedSpeakersResponse{Speakers: []string{"astra"}}, nil
}

func setupClient(t *testing.T, service *testService) *Client {
	t.Helper()
	return setupServices(t, func(server *grpc.Server) { pb.RegisterTextToSpeechServer(server, service) })
}

func setupServices(t *testing.T, register func(*grpc.Server)) *Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	c, err := NewClient(Config{APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	// Send targets straight to the in-memory dialer, without public DNS lookups.
	c.target = "passthrough:///tts"
	c.sttTarget = "passthrough:///stt"
	c.dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })}
	c.sttDialOptions = c.dialOptions
	t.Cleanup(func() { c.Close(); server.Stop(); listener.Close() })
	return c
}

func TestInMemoryServicesWithoutDNS(t *testing.T) {
	for _, service := range []string{"tts", "stt"} {
		t.Run(service, func(t *testing.T) {
			client, _ := setupRecognition(t, recognizeSilence)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			// Leave DNS without any addresses. In-memory RPCs must bypass it.
			dns := manual.NewBuilderWithScheme("dns")
			dns.BuildCallback = func(target resolver.Target, _ resolver.ClientConn, _ resolver.BuildOptions) {
				t.Errorf("in-memory service attempted DNS resolution for %v", target.URL)
				cancel()
			}
			client.dialOptions = append(client.dialOptions, grpc.WithResolvers(dns))
			client.sttDialOptions = append(client.sttDialOptions, grpc.WithResolvers(dns))
			if service == "tts" {
				languages, err := client.Languages.List(ctx, DiscoveryOptions{})
				if err != nil || !reflect.DeepEqual(languages, []string{"en", "de"}) {
					t.Fatalf("in-memory TTS discovery: %v, %v", languages, err)
				}
			} else {
				stream, err := client.STT.Stream(ctx, emptyAudio, TranscriptionOptions{Language: "en"})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				updates, err := drainTranscripts(stream)
				if err != nil || !reflect.DeepEqual(updates, []TranscriptionUpdate{TranscriptionFinal{Language: "en"}}) {
					t.Fatalf("in-memory transcription: %v, %v", updates, err)
				}
			}
		})
	}
}

func drain(stream *AudioStream) ([]byte, error) {
	var audio []byte
	for {
		chunk, err := stream.Recv()
		audio = append(audio, chunk...)
		if err == io.EOF {
			return audio, nil
		}
		if err != nil {
			return audio, err
		}
	}
}
func mustStream(t *testing.T, c *Client, text string, options SynthesisOptions) *AudioStream {
	t.Helper()
	s, err := c.TTS.Synthesize(context.Background(), text, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSynthesisAndDiscovery(t *testing.T) {
	service := &testService{}
	c := setupClient(t, service)
	s := mustStream(t, c, "Hello. This is a second sentence with enough context.", SynthesisOptions{})
	audio, err := drain(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(audio) != 9600 || s.RequestID() != "test-request" {
		t.Fatalf("bytes=%d id=%s", len(audio), s.RequestID())
	}
	if _, err := s.Recv(); err != io.EOF {
		t.Fatalf("repeat read: %v", err)
	}
	voices, err := c.Voices.List(context.Background(), VoiceListOptions{Language: "en"})
	if err != nil || len(voices) != 1 {
		t.Fatalf("%v %v", voices, err)
	}
	languages, err := c.Languages.List(context.Background(), DiscoveryOptions{})
	if err != nil || len(languages) != 2 {
		t.Fatalf("%v %v", languages, err)
	}
}

func TestIncrementalAudioBeforeInputEnd(t *testing.T) {
	c := setupClient(t, &testService{})
	release := make(chan struct{})
	count := 0
	s, err := c.TTS.Stream(context.Background(), func(ctx context.Context) (string, error) {
		count++
		if count == 1 {
			return "Hello. This next sentence has enough trailing context", nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return ".", io.EOF
		}
	}, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if chunk, err := s.Recv(); err != nil || len(chunk) == 0 {
		t.Fatalf("%d %v", len(chunk), err)
	}
	close(release)
	if _, err := drain(s); err != nil {
		t.Fatal(err)
	}
}

func TestPartialAudioThenErrorAndNoReplay(t *testing.T) {
	release := make(chan struct{})
	service := &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "partial"))
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(audioResponse([]byte{1, 0})); err != nil {
			return err
		}
		select {
		case <-release:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		return status.Error(codes.Unavailable, "partial failure")
	}}
	c := setupClient(t, service)
	s := mustStream(t, c, "Hello.", SynthesisOptions{})
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	close(release)
	_, err := drain(s)
	var sdkErr *Error
	if !errors.Is(err, ErrUnavailable) || !errors.As(err, &sdkErr) || sdkErr.RequestID != "partial" {
		t.Fatalf("%+v", err)
	}
	if service.calls.Load() != 1 {
		t.Fatal("synthesis was replayed")
	}
}

func TestServerRejectionPreservesStatusAndTrailers(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
				stream.SetTrailer(metadata.Pairs("x-request-id", "rejected"))
				return status.Error(code, "rejection")
			}})
			s := mustStream(t, c, strings.Repeat("Hello world. ", 1000), SynthesisOptions{})
			_, err := drain(s)
			var sdkErr *Error
			if !errors.As(err, &sdkErr) || sdkErr.RequestID != "rejected" || sdkErr.Kind != operationError(status.Error(code, ""), "").Kind {
				t.Fatalf("%+v", err)
			}
		})
	}
}

func TestCancelKeepsSibling(t *testing.T) {
	c := setupClient(t, &testService{})
	slow, err := c.TTS.Stream(context.Background(), func(ctx context.Context) (string, error) { <-ctx.Done(); return "", ctx.Err() }, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fast := mustStream(t, c, "Hello.", SynthesisOptions{})
	slow.Close()
	if _, err := slow.Recv(); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	if _, err := drain(fast); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutWhilePausedAndClientClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "close"}[closeClient], func(t *testing.T) {
			c := setupClient(t, &testService{})
			timeout := 5 * time.Second
			if closeClient {
				timeout = 0
			}
			deadline := time.Now().Add(timeout)
			s := mustStream(t, c, "Hello.", SynthesisOptions{Timeout: &timeout})
			if _, err := s.Recv(); err != nil {
				t.Fatal(err)
			}
			kind := ErrTimeout
			if closeClient {
				kind = ErrCancelled
				c.Close()
			} else {
				time.Sleep(time.Until(deadline) + 50*time.Millisecond)
			}
			if _, err := s.Recv(); !errors.Is(err, kind) {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestFirstAudioTimeout(t *testing.T) {
	c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm"))
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	c.limits.firstAudio = 60 * time.Millisecond
	s := mustStream(t, c, "Hello.", SynthesisOptions{})
	if _, err := drain(s); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
}

func TestSlowSourceAndConsumerAreNotServerStalls(t *testing.T) {
	c := setupClient(t, &testService{})
	c.limits.firstAudio = 40 * time.Millisecond
	c.limits.progress = 40 * time.Millisecond
	n := 0
	s, err := c.TTS.Stream(context.Background(), func(ctx context.Context) (string, error) {
		n++
		if n == 1 {
			return "Hello. Next sentence has sufficient trailing context", nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(150 * time.Millisecond):
			return ".", io.EOF
		}
	}, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := drain(s); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryRetryAndDeadline(t *testing.T) {
	service := &testService{}
	service.discovery = func(ctx context.Context) error {
		if service.discoveries.Load() < 3 {
			return status.Error(codes.Unavailable, "retry")
		}
		return nil
	}
	c := setupClient(t, service)
	if _, err := c.Languages.List(context.Background(), DiscoveryOptions{}); err != nil {
		t.Fatal(err)
	}
	if service.discoveries.Load() != 3 {
		t.Fatal("wrong attempts")
	}
	service.discovery = func(ctx context.Context) error {
		grpc.SendHeader(ctx, metadata.Pairs("x-request-id", "discovery"))
		<-ctx.Done()
		return ctx.Err()
	}
	deadline := 40 * time.Millisecond
	_, err := c.Languages.List(context.Background(), DiscoveryOptions{Timeout: &deadline})
	var sdkErr *Error
	if !errors.Is(err, ErrTimeout) || !errors.As(err, &sdkErr) || sdkErr.RequestID != "discovery" {
		t.Fatalf("%+v", err)
	}
}

func TestAudioValidationAndAlignment(t *testing.T) {
	for _, mode := range []string{"odd", "wrong", "missing", "empty"} {
		t.Run(mode, func(t *testing.T) {
			c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
				format := "audio/pcm"
				if mode == "wrong" {
					format = "audio/wav"
				}
				if mode != "missing" {
					stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", format))
				}
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
				}
				if mode == "empty" {
					return nil
				}
				return stream.Send(audioResponse([]byte{1}))
			}})
			s := mustStream(t, c, "Hello.", SynthesisOptions{})
			_, err := drain(s)
			if mode == "empty" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrAudioFormat) {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidConfigurationAndInput(t *testing.T) {
	for _, config := range []Config{{APIKey: " "}, {APIKey: "key", Model: "mistv2"}, {APIKey: "key", Endpoint: "https://host"}, {APIKey: "key", Endpoint: "host:0"}, {APIKey: "key", Endpoint: "host:+443"}, {APIKey: "key", Timeout: -1}} {
		if _, err := NewClient(config); err == nil {
			t.Fatalf("accepted %+v", config)
		}
	}
	c := setupClient(t, &testService{})
	for _, input := range []string{"", " \n", "\xff"} {
		if _, err := c.TTS.Synthesize(context.Background(), input, SynthesisOptions{}); err == nil {
			t.Fatal("accepted invalid text")
		}
	}
	c.Close()
	if _, err := c.TTS.Synthesize(context.Background(), "Hello", SynthesisOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestSourceFailuresAndSentenceLimit(t *testing.T) {
	for _, mode := range []string{"error", "blank", "limit"} {
		t.Run(mode, func(t *testing.T) {
			c := setupClient(t, &testService{})
			source := func(context.Context) (string, error) {
				switch mode {
				case "error":
					return "", errors.New("source failed")
				case "limit":
					return strings.Repeat("x", 65537), io.EOF
				default:
					return " ", io.EOF
				}
			}
			s, err := c.TTS.Stream(context.Background(), source, SynthesisOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, err = drain(s)
			kind := ErrInput
			if mode == "limit" {
				kind = ErrResourceLimit
			}
			if !errors.Is(err, kind) {
				t.Fatal(err)
			}
		})
	}
}

func TestSharedErrorContract(t *testing.T) {
	data, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Errors map[string]string `json:"grpc_errors"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]ErrorKind{"RimeAuthenticationError": ErrAuthentication, "RimePermissionError": ErrPermission, "RimeInputError": ErrInput, "RimeResourceLimitError": ErrResourceLimit, "RimeUnavailableError": ErrUnavailable, "RimeTimeoutError": ErrTimeout, "RimeCancelledError": ErrCancelled, "RimeStreamError": ErrStream}
	codesByName := map[string]codes.Code{"UNAUTHENTICATED": codes.Unauthenticated, "PERMISSION_DENIED": codes.PermissionDenied, "INVALID_ARGUMENT": codes.InvalidArgument, "RESOURCE_EXHAUSTED": codes.ResourceExhausted, "UNAVAILABLE": codes.Unavailable, "DEADLINE_EXCEEDED": codes.DeadlineExceeded, "CANCELLED": codes.Canceled, "INTERNAL": codes.Internal}
	for name, expected := range contract.Errors {
		if got := operationError(status.Error(codesByName[name], "failure"), "id"); got.Kind != kinds[expected] || got.RequestID != "id" {
			t.Fatalf("%s: %+v", name, got)
		}
	}
}

func TestHeadersAfterTextAndOddFrames(t *testing.T) {
	c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm"))
		for _, data := range [][]byte{{1}, {0, 2}, {0}} {
			if err := stream.Send(audioResponse(data)); err != nil {
				return err
			}
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}})
	s := mustStream(t, c, "Hello.", SynthesisOptions{})
	audio, err := drain(s)
	if err != nil || !bytes.Equal(audio, []byte{1, 0, 2, 0}) {
		t.Fatalf("%x %v", audio, err)
	}
}

func TestLargeAudioBackpressureAndCancellation(t *testing.T) {
	c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm"))
		if _, err := stream.Recv(); err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := stream.Send(audioResponse(make([]byte, 1000000))); err != nil {
				return err
			}
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}})
	s := mustStream(t, c, "Hello.", SynthesisOptions{})
	chunk, err := s.Recv()
	if err != nil || len(chunk) > 9600 {
		t.Fatalf("%d %v", len(chunk), err)
	}
	time.Sleep(50 * time.Millisecond)
	s.queue.mu.Lock()
	size := s.queue.bytes
	s.queue.mu.Unlock()
	if size > 96000 {
		t.Fatalf("unbounded queue: %d", size)
	}
	s.Close()
	select {
	case <-s.workersDone:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
	if _, err := s.Recv(); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
}

func TestContextCancellationAndConcurrentReader(t *testing.T) {
	c := setupClient(t, &testService{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	s, err := c.TTS.Stream(ctx, func(ctx context.Context) (string, error) { close(entered); <-ctx.Done(); return "", ctx.Err() }, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	done := make(chan error, 1)
	go func() { _, err := s.Recv(); done <- err }()
	<-entered
	// The source can start before the reader goroutine. Wait until Recv owns
	// the reader slot so this check does not depend on scheduler order.
	deadline := time.After(5 * time.Second)
	for !s.reading.Load() {
		select {
		case err := <-done:
			t.Fatalf("reader stopped before cancellation: %v", err)
		case <-deadline:
			t.Fatal("reader did not start")
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := s.Recv(); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
}
