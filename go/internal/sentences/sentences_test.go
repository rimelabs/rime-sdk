package sentences

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestSharedSentences(t *testing.T) {
	data, err := os.ReadFile("../../testdata/sentences.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		ID, Text  string
		Sentences []string
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	detector, err := New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer detector.Close()
	for _, c := range cases {
		for _, size := range []int{1, 2, 7, 17, 1024} {
			t.Run(c.ID+"/"+strconv.Itoa(size), func(t *testing.T) {
				buffer := NewBuffer(detector)
				var got []string
				emit := func(s string) error { got = append(got, s); return nil }
				runes := []rune(c.Text)
				for len(runes) > 0 {
					n := min(size, len(runes))
					if err := buffer.Feed(context.Background(), string(runes[:n]), false, emit); err != nil {
						t.Fatal(err)
					}
					runes = runes[n:]
				}
				if err := buffer.Feed(context.Background(), "", true, emit); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, c.Sentences) {
					t.Fatalf("got %#v; want %#v", got, c.Sentences)
				}
			})
		}
	}
}

func TestSentenceLimit(t *testing.T) {
	d, err := New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	err = NewBuffer(d).Feed(context.Background(), strings.Repeat("x", 65537), true, func(string) error { return nil })
	if _, ok := err.(LimitError); !ok {
		t.Fatalf("got %v", err)
	}
}
