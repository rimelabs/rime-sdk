package rime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/protobuf/proto"
)

func TestCompleteTextUTF8BoundaryAndRecovery(t *testing.T) {
	for _, character := range []string{"a", "é", "🙂"} {
		t.Run(character, func(t *testing.T) {
			requests := make(chan string, 2)
			service := completeTextService(func(request *pb.SynthesisRequest, rpc synthesisRPC) error {
				requests <- request.Text
				return rpc.Send(audioResponse([]byte{1, 0}))
			})
			client := setupClient(t, service)
			text := strings.Repeat(character, 65536/len(character))
			if _, err := drain(mustStream(t, client, text, SynthesisOptions{CompleteText: true})); err != nil {
				t.Fatal(err)
			}
			if got := <-requests; got != text {
				t.Fatal("complete text was changed")
			}
			if _, err := client.TTS.Stream(context.Background(), text+"a", SynthesisOptions{CompleteText: true}); !errors.Is(err, ErrInput) {
				t.Fatal(err)
			}
			if service.calls.Load() != 1 {
				t.Fatal("oversized text reached the service")
			}
			if _, err := drain(mustStream(t, client, "Again.", SynthesisOptions{CompleteText: true})); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSerializedRequestLimitAndRecovery(t *testing.T) {
	for _, completeText := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%v", completeText), func(t *testing.T) {
			requests := make(chan *pb.SynthesisRequest, 3)
			service := completeTextService(func(request *pb.SynthesisRequest, rpc synthesisRPC) error {
				requests <- request
				return rpc.Send(audioResponse([]byte{1, 0}))
			})
			service.header = func(request *pb.SynthesisRequest) error { requests <- request; return nil }
			client := setupClient(t, service)
			text := "Hello."
			if completeText {
				text = strings.Repeat("é", 32768)
			}
			entries := make([]PronunciationEntry, 500)
			for i := range entries {
				entries[i] = PronunciationEntry{Spelling: fmt.Sprintf("word%d", i), Pronunciation: `" k { S`}
			}
			size := func() int {
				voice, language, format, rate := "clementine", "en", "audio/pcm", int32(24000)
				request := &pb.SynthesisRequest{Speaker: &voice, Language: &language, AudioParameters: &pb.AudioParameters{AudioFormat: &format, SamplingRate: &rate}}
				for _, entry := range entries {
					request.CustomLexicon = append(request.CustomLexicon, &pb.PronunciationEntry{Spelling: entry.Spelling, Pronunciation: entry.Pronunciation})
				}
				if completeText {
					request.Text = text
					return proto.Size(request)
				}
				return proto.Size(&pb.StreamingSynthesisRequest{Payload: &pb.StreamingSynthesisRequest_Header{Header: request}})
			}
			// Pad a spelling to isolate the wire limit from linguistic/entry-count validation.
			for difference := 131072 - size(); difference != 0; difference = 131072 - size() {
				last := &entries[len(entries)-1]
				if difference > 0 {
					last.Spelling += strings.Repeat("x", difference)
				} else {
					last.Spelling = last.Spelling[:len(last.Spelling)+difference]
				}
			}
			// Allow cold sentence-detector startup under the race detector.
			timeout := 10 * time.Second
			options := SynthesisOptions{CompleteText: completeText, CustomLexicon: entries, Timeout: &timeout}
			if _, err := drain(mustStream(t, client, text, options)); err != nil {
				t.Fatal(err)
			}
			if len((<-requests).CustomLexicon) != 500 {
				t.Fatal("lexicon was not forwarded")
			}
			entries[len(entries)-1].Spelling += "x"
			if size() != 131073 {
				t.Fatal("expected one byte over the wire limit")
			}
			if audio, err := drain(mustStream(t, client, text, options)); !errors.Is(err, ErrResourceLimit) || len(audio) != 0 {
				t.Fatalf("oversized request: audio=%d err=%v", len(audio), err)
			}
			if _, err := drain(mustStream(t, client, "Again.", SynthesisOptions{CompleteText: completeText, Timeout: &timeout})); err != nil {
				t.Fatal(err)
			}
			if request := <-requests; request.GetText() != map[bool]string{false: "", true: "Again."}[completeText] || len(request.CustomLexicon) != 0 {
				t.Fatal("oversized request reached the service")
			}
			if len(requests) != 0 {
				t.Fatal("unexpected replay")
			}
		})
	}
}
