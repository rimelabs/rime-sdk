package rime

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTextInputPreservesSource(t *testing.T) {
	data, err := os.ReadFile("testdata/text-input.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct{ ID, Text string }
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.ID, func(t *testing.T) {
			service := &testService{}
			client := setupClient(t, service)
			points := []rune(fixture.Text)
			index := 0
			source := func(context.Context) (string, error) {
				if index == len(points) {
					return "", io.EOF
				}
				fragment := string(points[index])
				index++
				return fragment, nil
			}
			stream, err := client.TTS.Stream(context.Background(), source, SynthesisOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := drain(stream); err != nil {
				t.Fatal(err)
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if strings.Join(service.texts, "") != fixture.Text {
				t.Fatalf("changed text: %q", service.texts)
			}
		})
	}
}

func TestLargeTextHasTransportLimitsOnly(t *testing.T) {
	for _, text := range []string{strings.Repeat("x", 70000), strings.Repeat("🚀", 20000), "word " + strings.Repeat(" ", 70000) + "end"} {
		service := &testService{}
		client := setupClient(t, service)
		if _, err := drain(mustStream(t, client, text, SynthesisOptions{})); err != nil {
			t.Fatal(err)
		}
		service.mu.Lock()
		if strings.Join(service.texts, "") != text {
			t.Fatal("changed text")
		}
		for _, fragment := range service.texts {
			if len(fragment) == 0 || len(fragment) > 4096 || !utf8.ValidString(fragment) {
				t.Fatal("invalid transport chunk")
			}
		}
		service.mu.Unlock()
	}
}

func TestTextForwardedBeforeSourceContinuesAndAudioAfterHalfClose(t *testing.T) {
	received := make(chan struct{}, 1)
	completed := make(chan []string, 1)
	service := &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		var fragments []string
		if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm")); err != nil {
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
			fragments = append(fragments, message.GetTextChunk())
			select {
			case received <- struct{}{}:
			default:
			}
		}
		completed <- fragments
		return stream.Send(audioResponse([]byte{1, 0}))
	}}
	client := setupClient(t, service)
	client.limits.firstAudio = 50 * time.Millisecond
	index := 0
	source := func(ctx context.Context) (string, error) {
		index++
		if index == 1 {
			return "The value is 1.", nil
		}
		select {
		case <-received:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		// Go sources may return their final text together with EOF.
		return "7 percent without final punctuation", io.EOF
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.TTS.Stream(ctx, source, SynthesisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	audio, err := drain(stream)
	if err != nil || len(audio) != 2 {
		t.Fatalf("audio=%v err=%v", audio, err)
	}
	fragments := <-completed
	if len(fragments) != 2 || strings.Join(fragments, "") != "The value is 1.7 percent without final punctuation" {
		t.Fatalf("fragments=%q", fragments)
	}
}
