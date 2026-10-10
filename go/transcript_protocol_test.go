package rime

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestSharedTranscripts(t *testing.T) {
	data, err := os.ReadFile("testdata/stt/transcripts.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Name, Error string
			Messages    []json.RawMessage
			InputDone   *bool
			Expected    []map[string]string
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Name, func(t *testing.T) {
			state := transcriptState{textLimit: 65536}
			inputDone := test.InputDone == nil || *test.InputDone
			var actual []map[string]string
			var err error
			for _, raw := range test.Messages {
				message := &pb.StreamingTranscriptionResponse{}
				if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, message); err != nil {
					t.Fatal(err)
				}
				var update *TranscriptionPartial
				update, err = state.accept(message, inputDone)
				if err != nil {
					break
				}
				if update != nil {
					actual = append(actual, map[string]string{"kind": "partial", "text": update.Text})
				}
			}
			if err == nil {
				var final TranscriptionFinal
				final, err = state.finish()
				if err == nil {
					actual = append(actual, map[string]string{"kind": "final", "text": final.Text, "language": final.Language})
				}
			}
			if test.Error != "" {
				if !errors.Is(err, ErrStream) {
					t.Fatalf("wanted protocol error, got %v", err)
				}
			} else if err != nil || !reflect.DeepEqual(actual, test.Expected) {
				t.Fatalf("got %v %v, want %v", actual, err, test.Expected)
			}
		})
	}
}

func TestTranscriptLimitCountsUTF8Bytes(t *testing.T) {
	state := transcriptState{language: "en", textLimit: 65536}
	if _, err := state.accept(hypothesis(1, strings.Repeat("é", 32768)), false); err != nil {
		t.Fatal(err)
	}
	if _, err := state.accept(hypothesis(2, strings.Repeat("é", 32769)), false); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("%v", err)
	}
}
