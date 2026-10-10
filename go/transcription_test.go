package rime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
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
	"google.golang.org/grpc/status"
)

type transcriptionRPC = grpc.BidiStreamingServer[pb.StreamingTranscriptionRequest, pb.StreamingTranscriptionResponse]
type recognitionService struct {
	pb.UnimplementedSpeechToTextServer
	handle func(transcriptionRPC, *pb.StreamingConfig) error
	calls  atomic.Int32
}

func (service *recognitionService) TranscribeStreaming(rpc transcriptionRPC) error {
	service.calls.Add(1)
	metadata, _ := metadata.FromIncomingContext(rpc.Context())
	if got := metadata.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-key" {
		return status.Error(codes.Unauthenticated, "bad key")
	}
	first, err := rpc.Recv()
	if err != nil {
		return err
	}
	config := first.GetConfig()
	if config == nil || config.Language == nil || config.OutputContract != pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES {
		return status.Error(codes.InvalidArgument, "bad config")
	}
	return service.handle(rpc, config)
}
func setupRecognition(t *testing.T, handle func(transcriptionRPC, *pb.StreamingConfig) error) (*Client, *recognitionService) {
	service := &recognitionService{handle: handle}
	client := setupServices(t, func(server *grpc.Server) {
		pb.RegisterSpeechToTextServer(server, service)
		pb.RegisterTextToSpeechServer(server, &testService{})
	})
	return client, service
}
func acceptance(language string) *pb.StreamingTranscriptionResponse {
	return &pb.StreamingTranscriptionResponse{Payload: &pb.StreamingTranscriptionResponse_Accepted{Accepted: &pb.StreamingAccepted{
		Language:       &pb.ResolvedLanguage{Tag: language, Source: pb.LanguageSource_LANGUAGE_SOURCE_SELECTED},
		OutputContract: pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES,
	}}}
}
func hypothesis(revision uint64, text string) *pb.StreamingTranscriptionResponse {
	return &pb.StreamingTranscriptionResponse{Payload: &pb.StreamingTranscriptionResponse_Hypothesis{Hypothesis: &pb.TranscriptionHypothesis{Text: text, Revision: revision}}}
}
func completion(revision uint64, text, language string) *pb.StreamingTranscriptionResponse {
	return &pb.StreamingTranscriptionResponse{Payload: &pb.StreamingTranscriptionResponse_Done{Done: &pb.TranscriptionDone{Text: text, Revision: revision, Language: &pb.ResolvedLanguage{Tag: language, Source: pb.LanguageSource_LANGUAGE_SOURCE_SELECTED}}}}
}
func readAudio(rpc transcriptionRPC) ([]byte, error) {
	var audio []byte
	for {
		message, err := rpc.Recv()
		if err == io.EOF {
			return audio, nil
		}
		if err != nil {
			return nil, err
		}
		data := message.GetAudio()
		if len(data) == 0 || len(data) > 65536 || len(data)%2 != 0 {
			return nil, status.Error(codes.InvalidArgument, "bad audio framing")
		}
		audio = append(audio, data...)
	}
}
func recognizeSilence(rpc transcriptionRPC, config *pb.StreamingConfig) error {
	if err := rpc.Send(acceptance(config.GetLanguage())); err != nil {
		return err
	}
	if _, err := readAudio(rpc); err != nil {
		return err
	}
	return rpc.Send(completion(0, "", config.GetLanguage()))
}
func emptyAudio(context.Context) ([]byte, error) { return nil, io.EOF }
func mustTranscribe(t *testing.T, client *Client, source AudioSource, options TranscriptionOptions) *TranscriptionStream {
	t.Helper()
	stream, err := client.STT.Stream(context.Background(), source, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	return stream
}
func drainTranscripts(stream *TranscriptionStream) ([]TranscriptionUpdate, error) {
	var updates []TranscriptionUpdate
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			return updates, nil
		}
		if err != nil {
			return updates, err
		}
		updates = append(updates, update)
	}
}
func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test signal")
	}
}

func TestTranscriptionLazyAcceptanceAndSnapshots(t *testing.T) {
	startReceived, accept, finishInput := make(chan struct{}), make(chan struct{}), make(chan struct{})
	terms := []string{" Rime ", "Rime", ""}
	expectedTerms := append([]string(nil), terms...)
	client, service := setupRecognition(t, func(rpc transcriptionRPC, config *pb.StreamingConfig) error {
		if config.GetLanguage() != " es " || config.Mode != pb.TranscriptionMode_TRANSCRIPTION_MODE_VERBATIM || !reflect.DeepEqual(config.ContextTerms, expectedTerms) {
			t.Errorf("config changed: %v", config)
		}
		close(startReceived)
		select {
		case <-accept:
		case <-rpc.Context().Done():
			return rpc.Context().Err()
		}
		if err := rpc.SendHeader(metadata.Pairs("x-request-id", "recognition")); err != nil {
			return err
		}
		if err := rpc.Send(acceptance("es")); err != nil {
			return err
		}
		message, err := rpc.Recv()
		if err != nil {
			return err
		}
		if !bytes.Equal(message.GetAudio(), []byte{1, 0, 2, 0}) {
			t.Errorf("audio: %v", message.GetAudio())
		}
		if err := rpc.Send(hypothesis(1, "I scream")); err != nil {
			return err
		}
		if err := rpc.Send(hypothesis(3, "Ice cream")); err != nil {
			return err
		}
		if _, err := readAudio(rpc); err != nil {
			return err
		}
		return rpc.Send(completion(3, "Ice cream", "es"))
	})
	var reads atomic.Int32
	stream := mustTranscribe(t, client, func(ctx context.Context) ([]byte, error) {
		if reads.Add(1) == 1 {
			return []byte{1, 0, 2, 0}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-finishInput:
			return nil, io.EOF
		}
	}, TranscriptionOptions{Language: " es ", Mode: TranscriptionVerbatim, ContextTerms: terms})
	terms[0] = "mutated"
	if service.calls.Load() != 0 || reads.Load() != 0 {
		t.Fatal("stream eagerly started")
	}
	first := make(chan TranscriptionUpdate, 1)
	go func() {
		update, err := stream.Recv()
		if err != nil {
			t.Error(err)
		}
		first <- update
	}()
	awaitSignal(t, startReceived)
	if reads.Load() != 0 {
		t.Fatal("source read before acceptance")
	}
	close(accept)
	select {
	case update := <-first:
		if update != (TranscriptionPartial{Text: "I scream"}) {
			t.Fatalf("%v", update)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no partial while input open")
	}
	if update, err := stream.Recv(); err != nil || update != (TranscriptionPartial{Text: "Ice cream"}) {
		t.Fatalf("%v %v", update, err)
	}
	close(finishInput)
	updates, err := drainTranscripts(stream)
	if err != nil || !reflect.DeepEqual(updates, []TranscriptionUpdate{TranscriptionFinal{Text: "Ice cream", Language: "es"}}) {
		t.Fatalf("%v %v", updates, err)
	}
	if stream.RequestID() != "recognition" || service.calls.Load() != 1 {
		t.Fatal("request metadata/retry mismatch")
	}
	stream.Close()
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("repeat read: %v", err)
	}
}

func TestTranscriptionStatusAndNoReplay(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			client, service := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
				rpc.SetTrailer(metadata.Pairs("x-request-id", "rejected"))
				return status.Error(code, "rejected")
			})
			stream := mustTranscribe(t, client, func(context.Context) ([]byte, error) { t.Error("rejected service read audio"); return nil, io.EOF }, TranscriptionOptions{Language: "invalid-language"})
			updates, err := drainTranscripts(stream)
			var sdkError *Error
			if len(updates) != 0 || !errors.As(err, &sdkError) || sdkError.RequestID != "rejected" || sdkError.Kind != operationError(status.Error(code, ""), "").Kind {
				t.Fatalf("%v %+v", updates, err)
			}
			if service.calls.Load() != 1 {
				t.Fatal("recognition replayed")
			}
		})
	}
}

func TestTranscriptionFinalRequiresSuccessfulStatus(t *testing.T) {
	for _, scenario := range []string{"missing done", "error after done", "message after done", "early done"} {
		t.Run(scenario, func(t *testing.T) {
			client, _ := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
				rpc.SendHeader(metadata.Pairs("x-request-id", "terminal"))
				if err := rpc.Send(acceptance("en")); err != nil {
					return err
				}
				if scenario != "early done" {
					if _, err := readAudio(rpc); err != nil {
						return err
					}
				}
				if scenario == "missing done" {
					return nil
				}
				if err := rpc.Send(completion(0, "", "en")); err != nil {
					return err
				}
				if scenario == "error after done" {
					return status.Error(codes.Unavailable, "late error")
				}
				if scenario == "message after done" {
					return rpc.Send(hypothesis(1, "late"))
				}
				return nil
			})
			source := AudioSource(emptyAudio)
			if scenario == "early done" {
				source = func(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
			}
			stream := mustTranscribe(t, client, source, TranscriptionOptions{Language: "en"})
			updates, err := drainTranscripts(stream)
			kind := ErrStream
			if scenario == "error after done" {
				kind = ErrUnavailable
			}
			if len(updates) != 0 || !errors.Is(err, kind) {
				t.Fatalf("%v %v", updates, err)
			}
		})
	}
}

func TestTranscriptionInputErrorsNeverCommit(t *testing.T) {
	for _, scenario := range []string{"truncated mono", "truncated stereo", "source error"} {
		t.Run(scenario, func(t *testing.T) {
			committed := make(chan bool, 1)
			client, _ := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
				if err := rpc.Send(acceptance("en")); err != nil {
					return err
				}
				_, err := readAudio(rpc)
				committed <- err == nil
				return err
			})
			options := TranscriptionOptions{Language: "en"}
			if scenario == "truncated stereo" {
				options.InputFormat.Channels = 2
			}
			sourceFailure := errors.New("microphone failed")
			source := func(context.Context) ([]byte, error) {
				if scenario == "source error" {
					return nil, sourceFailure
				}
				return []byte{1}, io.EOF
			}
			updates, err := drainTranscripts(mustTranscribe(t, client, source, options))
			kind := ErrAudioFormat
			if scenario == "source error" {
				kind = ErrInput
				if !errors.Is(err, sourceFailure) {
					t.Fatal("lost cause")
				}
			}
			if len(updates) != 0 || !errors.Is(err, kind) {
				t.Fatalf("%v %v", updates, err)
			}
			select {
			case got := <-committed:
				if got {
					t.Fatal("invalid input half-closed")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("server did not observe cancellation")
			}
		})
	}
}

func TestTranscriptionFinalSourceBytesAndBoundedAudio(t *testing.T) {
	for _, size := range []int{0, 1 << 20} {
		client, _ := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
			if err := rpc.Send(acceptance("en")); err != nil {
				return err
			}
			audio, err := readAudio(rpc)
			if err != nil {
				return err
			}
			if len(audio) != size {
				t.Errorf("audio length %d, want %d", len(audio), size)
			}
			return rpc.Send(completion(0, "", "en"))
		})
		stream := mustTranscribe(t, client, func(context.Context) ([]byte, error) { return make([]byte, size), io.EOF }, TranscriptionOptions{Language: "en"})
		updates, err := drainTranscripts(stream)
		if err != nil || !reflect.DeepEqual(updates, []TranscriptionUpdate{TranscriptionFinal{Language: "en"}}) {
			t.Fatalf("%v %v", updates, err)
		}
	}
}

func TestTranscriptionDeadlines(t *testing.T) {
	for _, stage := range []string{"connection", "acceptance", "completion", "source", "consumer"} {
		t.Run(stage, func(t *testing.T) {
			client, _ := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
				if stage == "acceptance" {
					<-rpc.Context().Done()
					return rpc.Context().Err()
				}
				if err := rpc.Send(acceptance("en")); err != nil {
					return err
				}
				if stage == "consumer" {
					if err := rpc.Send(hypothesis(1, "partial")); err != nil {
						return err
					}
				}
				if _, err := readAudio(rpc); err != nil {
					return err
				}
				<-rpc.Context().Done()
				return rpc.Context().Err()
			})
			budget := 50 * time.Millisecond
			if stage == "consumer" {
				budget = 500 * time.Millisecond
			}
			options := TranscriptionOptions{Language: "en"}
			source := AudioSource(emptyAudio)
			switch stage {
			case "connection":
				client.sttLimits.connection = budget
				client.sttDialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() })}
			case "acceptance":
				client.sttLimits.acceptance = budget
			case "completion":
				client.sttLimits.completion = budget
			case "source", "consumer":
				options.Timeout = &budget
				source = func(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
			}
			stream := mustTranscribe(t, client, source, options)
			if stage == "consumer" {
				if _, err := stream.Recv(); err != nil {
					t.Fatal(err)
				}
				awaitSignal(t, stream.op.ctx.Done())
			}
			_, err := drainTranscripts(stream)
			if !errors.Is(err, ErrTimeout) {
				t.Fatalf("%v", err)
			}
		})
	}
}

func TestTranscriptionLazyTimeoutAndTTSIndependence(t *testing.T) {
	client, _ := setupRecognition(t, recognizeSilence)
	client.timeout = time.Nanosecond
	budget := time.Second
	stream := mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en", Timeout: &budget})
	budget = time.Nanosecond // The caller's options cannot mutate an existing stream.
	if stream.overallTimer != nil {
		t.Fatal("lazy deadline started at construction")
	}
	if _, err := drainTranscripts(stream); err != nil {
		t.Fatal(err)
	}
	if _, err := drainTranscripts(mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en"})); err != nil {
		t.Fatal("inherited TTS deadline:", err)
	}
}

func TestTranscriptionCancellationAndClientClose(t *testing.T) {
	client, _ := setupRecognition(t, recognizeSilence)
	entered := make(chan struct{})
	slow := mustTranscribe(t, client, func(ctx context.Context) ([]byte, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }, TranscriptionOptions{Language: "en"})
	result := make(chan error, 1)
	go func() { _, err := slow.Recv(); result <- err }()
	awaitSignal(t, entered)
	if _, err := slow.Recv(); !errors.Is(err, ErrInput) {
		t.Fatalf("concurrent reader: %v", err)
	}
	slow.Close()
	if err := <-result; !errors.Is(err, ErrCancelled) {
		t.Fatalf("%v", err)
	}
	if _, err := drainTranscripts(mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en"})); err != nil {
		t.Fatal("sibling STT:", err)
	}
	if _, err := drain(mustStream(t, client, "Hello.", SynthesisOptions{})); err != nil {
		t.Fatal("sibling TTS:", err)
	}
	lazy := mustTranscribe(t, client, func(context.Context) ([]byte, error) { t.Error("closed source read"); return nil, io.EOF }, TranscriptionOptions{Language: "en"})
	client.Close()
	if _, err := lazy.Recv(); !errors.Is(err, ErrCancelled) {
		t.Fatalf("root close: %v", err)
	}
	awaitSignal(t, lazy.workersDone)
	client.mu.Lock()
	remaining := len(client.operations)
	client.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("operations retained: %d", remaining)
	}
}

func TestTranscriptionPreCancelledAndUncooperativeSource(t *testing.T) {
	client, _ := setupRecognition(t, recognizeSilence)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream, err := client.STT.Stream(ctx, func(context.Context) ([]byte, error) { t.Error("cancelled source called"); return nil, io.EOF }, TranscriptionOptions{Language: "en"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	stream.Close()
	stream.mu.Lock()
	retained := stream.source != nil
	stream.mu.Unlock()
	if retained {
		t.Fatal("unstarted stream retained its source")
	}
	client.sttLimits.cleanup = 20 * time.Millisecond
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release); awaitSignal(t, exited) })
	stream = mustTranscribe(t, client, func(context.Context) ([]byte, error) { close(entered); <-release; close(exited); return nil, io.EOF }, TranscriptionOptions{Language: "en"})
	readDone := make(chan struct{})
	go func() { defer close(readDone); stream.Recv() }()
	awaitSignal(t, entered)
	start := time.Now()
	stream.Close()
	if time.Since(start) > time.Second {
		t.Fatal("cleanup waited for uncooperative source")
	}
	awaitSignal(t, readDone)
}

func TestTranscriptionPreservesContextCauseBeforeFailure(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, path := range []string{"transport", "close"} {
			t.Run(cause.Error()+"/"+path, func(t *testing.T) {
				client, _ := setupRecognition(t, recognizeSilence)
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				stream, err := client.STT.Stream(ctx, emptyAudio, TranscriptionOptions{Language: "en"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { stream.Close() })
				stream.setRequestID("stt-request")
				// Force failure handling ahead of the asynchronous context callback.
				stream.stopCancellation()
				cancel(cause)
				if path == "transport" {
					stream.fail(status.Error(codes.Canceled, "transport cancelled"))
				} else {
					stream.Close()
				}
				_, err = stream.Recv()
				kind := ErrCancelled
				if cause == context.DeadlineExceeded {
					kind = ErrTimeout
				}
				var sdkError *Error
				if !errors.Is(err, cause) || !errors.Is(err, kind) || !errors.As(err, &sdkError) || sdkError.RequestID != "stt-request" {
					t.Fatalf("context cause or request ID lost: %v (cause %v)", err, errors.Unwrap(err))
				}
			})
		}
	}
}

func TestTranscriptionPreservesServiceFailureDuringCleanup(t *testing.T) {
	client, _ := setupRecognition(t, recognizeSilence)
	stream := mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en"})
	serviceError := status.Error(codes.Unavailable, "service unavailable")
	stream.fail(serviceError)
	stream.Close()
	_, err := stream.Recv()
	if !errors.Is(err, serviceError) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("service failure lost: %v", err)
	}
}

func TestTranscriptionValidationAndEndpoint(t *testing.T) {
	client, err := NewClient(Config{APIKey: "test-key", Model: "mistv3", Endpoint: "TTS.Example:8443", STTEndpoint: "STT.Example"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.target != "tts.example:8443" || client.sttTarget != "stt.example:443" {
		t.Fatal("endpoint routing")
	}
	for _, endpoint := range []string{"https://host", "host/path", "host:0", "host:65536", "host:abc", "host:443:1", "bad host", "-bad"} {
		if _, err := NewClient(Config{APIKey: "test-key", STTEndpoint: endpoint}); !errors.Is(err, ErrInput) {
			t.Fatalf("%q: %v", endpoint, err)
		}
	}
	negative := -time.Second
	for _, options := range []TranscriptionOptions{{Mode: "other"}, {Timeout: &negative}, {InputFormat: PCMFormat{SampleRate: 44100}}, {InputFormat: PCMFormat{Channels: 3}}} {
		if _, err := client.STT.Stream(context.Background(), emptyAudio, options); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
	if _, err := client.STT.Stream(nil, emptyAudio, TranscriptionOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, err := client.STT.Stream(context.Background(), nil, TranscriptionOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	client.Close()
	if _, err := client.STT.Stream(context.Background(), emptyAudio, TranscriptionOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestTranscriptionOversizeResponse(t *testing.T) {
	for _, size := range []int{65537, 300000} {
		client, _ := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
			if err := rpc.Send(acceptance("en")); err != nil {
				return err
			}
			return rpc.Send(hypothesis(1, strings.Repeat("x", size)))
		})
		updates, err := drainTranscripts(mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en"}))
		if len(updates) != 0 || !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("%d: %v %v", size, updates, err)
		}
	}
}

func TestTranscriptionPartialThenFailurePreservesRequestID(t *testing.T) {
	release := make(chan struct{})
	client, service := setupRecognition(t, func(rpc transcriptionRPC, _ *pb.StreamingConfig) error {
		if err := rpc.SendHeader(metadata.Pairs("x-request-id", "partial-request")); err != nil {
			return err
		}
		if err := rpc.Send(acceptance("en")); err != nil {
			return err
		}
		if err := rpc.Send(hypothesis(1, "provisional")); err != nil {
			return err
		}
		select {
		case <-release:
		case <-rpc.Context().Done():
			return rpc.Context().Err()
		}
		return status.Error(codes.Unavailable, "failure after partial")
	})
	stream := mustTranscribe(t, client, emptyAudio, TranscriptionOptions{Language: "en"})
	if update, err := stream.Recv(); err != nil || update != (TranscriptionPartial{Text: "provisional"}) {
		t.Fatalf("%v %v", update, err)
	}
	close(release)
	updates, err := drainTranscripts(stream)
	var sdkError *Error
	if len(updates) != 0 || !errors.Is(err, ErrUnavailable) || !errors.As(err, &sdkError) || sdkError.RequestID != "partial-request" || service.calls.Load() != 1 {
		t.Fatalf("%v %v", updates, err)
	}
}

func TestTranscriptionConcurrentStreams(t *testing.T) {
	client, service := setupRecognition(t, recognizeSilence)
	var workers sync.WaitGroup
	for index := 0; index < 12; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			stream, err := client.STT.Stream(context.Background(), emptyAudio, TranscriptionOptions{Language: "en"})
			if err != nil {
				t.Error(err)
				return
			}
			defer stream.Close()
			updates, err := drainTranscripts(stream)
			if err != nil || !reflect.DeepEqual(updates, []TranscriptionUpdate{TranscriptionFinal{Language: "en"}}) {
				t.Errorf("%v %v", updates, err)
			}
		}()
	}
	workers.Wait()
	if service.calls.Load() != 12 {
		t.Fatalf("calls: %d", service.calls.Load())
	}
	client.mu.Lock()
	remaining := len(client.operations)
	client.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("completed streams retained: %d", remaining)
	}
}

func TestTranscriptionClientCloseDuringConnectionAndSource(t *testing.T) {
	for _, stage := range []string{"connection", "source"} {
		t.Run(stage, func(t *testing.T) {
			client, _ := setupRecognition(t, recognizeSilence)
			entered := make(chan struct{})
			var enteredOnce sync.Once
			source := AudioSource(emptyAudio)
			if stage == "connection" {
				client.sttDialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					enteredOnce.Do(func() { close(entered) })
					<-ctx.Done()
					return nil, ctx.Err()
				})}
			} else {
				source = func(ctx context.Context) ([]byte, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }
			}
			stream := mustTranscribe(t, client, source, TranscriptionOptions{Language: "en"})
			readDone := make(chan error, 1)
			go func() { _, err := stream.Recv(); readDone <- err }()
			awaitSignal(t, entered)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-readDone:
				if !errors.Is(err, ErrCancelled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("client close left Recv blocked")
			}
			awaitSignal(t, stream.workersDone)
		})
	}
}
