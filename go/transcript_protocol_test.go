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
			Name, Error, OutputContract string
			Messages                    []json.RawMessage
			InputDone                   *bool
			Expected                    []map[string]string
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Name, func(t *testing.T) {
			contract, contractErr := transcriptionContract(TranscriptionOutputContract(test.OutputContract))
			if contractErr != nil {
				t.Fatal(contractErr)
			}
			state := transcriptState{textLimit: 1048576, outputContract: contract}
			inputDone := test.InputDone == nil || *test.InputDone
			var actual []map[string]string
			var err error
			for _, raw := range test.Messages {
				message := &pb.StreamingTranscriptionResponse{}
				if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, message); err != nil {
					t.Fatal(err)
				}
				var update TranscriptionUpdate
				update, err = state.accept(message, inputDone)
				if err != nil {
					break
				}
				if update != nil {
					switch value := update.(type) {
					case TranscriptionPartial:
						actual = append(actual, map[string]string{"kind": "partial", "text": value.Text})
					case TranscriptionDelta:
						actual = append(actual, map[string]string{"kind": "delta", "text": value.Text})
					default:
						t.Fatalf("unexpected update %T", update)
					}
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

func TestAppendCumulativeUTF8Limit(t *testing.T) {
	state := transcriptState{language: "en", textLimit: 4, outputContract: pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_APPEND_FRAGMENTS}
	message := func(text string) *pb.StreamingTranscriptionResponse {
		return &pb.StreamingTranscriptionResponse{Payload: &pb.StreamingTranscriptionResponse_Delta{Delta: &pb.TranscriptionDelta{Text: text}}}
	}
	for i := 0; i < 2; i++ {
		if _, err := state.accept(message("é"), false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := state.accept(message("x"), false); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("%v", err)
	}
}
