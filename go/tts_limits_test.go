package rime

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/protobuf/proto"
)

func TestSerializedRequestLimitAndRecovery(t *testing.T) {
	requests := make(chan *pb.SynthesisRequest, 3)
	service := &testService{header: func(request *pb.SynthesisRequest) error { requests <- request; return nil }}
	client := setupClient(t, service)
	text := "Hello."
	entries := make([]PronunciationEntry, 500)
	for i := range entries {
		entries[i] = PronunciationEntry{Spelling: fmt.Sprintf("word%d", i), Pronunciation: `" k { S`}
	}
	size := func() int {
		voice, language, format, rate := "clementine", "en", "audio/pcm", int32(24000)
		strategy := pb.SplitStrategy_SPLIT_STRATEGY_ACCUMULATE_SENTENCE
		request := &pb.SynthesisRequest{SplitStrategy: &strategy, Speaker: &voice, Language: &language, AudioParameters: &pb.AudioParameters{AudioFormat: &format, SamplingRate: &rate}}
		for _, entry := range entries {
			request.CustomLexicon = append(request.CustomLexicon, &pb.PronunciationEntry{Spelling: entry.Spelling, Pronunciation: entry.Pronunciation})
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
	// Allow transport startup under the race detector.
	timeout := 10 * time.Second
	options := SynthesisOptions{CustomLexicon: entries, Timeout: &timeout}
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
	if _, err := drain(mustStream(t, client, "Again.", SynthesisOptions{Timeout: &timeout})); err != nil {
		t.Fatal(err)
	}
	if request := <-requests; request.GetText() != "" || len(request.CustomLexicon) != 0 {
		t.Fatal("oversized request reached the service")
	}
	if len(requests) != 0 {
		t.Fatal("unexpected replay")
	}
}
