package rime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-sdk/go/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestServiceCompletionBeforeSourceEnd(t *testing.T) {
	sourceEntered := make(chan struct{})
	c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
			return err
		}
		select {
		case <-sourceEntered:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}})
	s, err := c.TTS.StreamSource(context.Background(), func(ctx context.Context) (string, error) {
		close(sourceEntered)
		<-ctx.Done()
		return "", ctx.Err()
	}, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := drain(s); !errors.Is(err, ErrStream) {
		t.Fatalf("early service completion: %v", err)
	}
	select {
	case <-s.workersDone:
	case <-time.After(time.Second):
		t.Fatal("source did not stop after early service completion")
	}
}

func TestProgressTimeoutAfterFirstAudio(t *testing.T) {
	c := setupClient(t, &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
			return err
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
		if err := stream.Send(audioResponse([]byte{1, 0})); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	c.limits.firstAudio = 5 * time.Second
	c.limits.progress = 80 * time.Millisecond
	s := mustStream(t, c, "Hello.", SynthesisOptions{})
	if chunk, err := s.Recv(); err != nil || len(chunk) != 2 {
		t.Fatalf("first audio: %x, %v", chunk, err)
	}
	result := make(chan error, 1)
	go func() { _, err := drain(s); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("progress timeout: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream used the first-audio limit after audio arrived")
	}
}
