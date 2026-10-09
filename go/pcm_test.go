package rime

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestSharedPCMInput(t *testing.T) {
	data, err := os.ReadFile("testdata/pcm-input.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Vectors []struct {
			SampleRate, Channels int
			Input, Output        []int16
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, vector := range fixture.Vectors {
		for _, size := range []int{1, 3, 7, 65536} {
			t.Run(fmt.Sprintf("%d/%d/%d", vector.SampleRate, vector.Channels, size), func(t *testing.T) {
				var source, expected, actual []byte
				for _, sample := range vector.Input {
					source = binary.LittleEndian.AppendUint16(source, uint16(sample))
				}
				for _, sample := range vector.Output {
					expected = binary.LittleEndian.AppendUint16(expected, uint16(sample))
				}
				input := newPCMInput(PCMFormat{SampleRate: vector.SampleRate, Channels: vector.Channels})
				for offset := 0; offset < len(source); offset += size {
					if err := input.feed(context.Background(), source[offset:min(offset+size, len(source))], func(chunk []byte) error { actual = append(actual, chunk...); return nil }); err != nil {
						t.Fatal(err)
					}
				}
				if err := input.finish(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(actual, expected) {
					t.Fatalf("got %x, want %x", actual, expected)
				}
			})
		}
	}
}

func TestPCMInputBoundsAndCancellation(t *testing.T) {
	input := newPCMInput(PCMFormat{SampleRate: 8000, Channels: 1})
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := input.feed(ctx, make([]byte, 2<<20), func(data []byte) error {
		calls++
		if len(data) > 65536 || len(data)%2 != 0 {
			t.Fatalf("wire size %d", len(data))
		}
		if cap(input.pending) > 32768 {
			t.Fatalf("retained source buffer %d", cap(input.pending))
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("%d %v", calls, err)
	}
}

func TestPCMTruncationAndStateIsolation(t *testing.T) {
	for _, channels := range []int{1, 2} {
		for count := 1; count < channels*2; count++ {
			input := newPCMInput(PCMFormat{SampleRate: 16000, Channels: channels})
			if err := input.feed(context.Background(), make([]byte, count), func([]byte) error { t.Fatal("partial frame emitted"); return nil }); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(input.finish(), ErrAudioFormat) {
				t.Fatal("truncated frame accepted")
			}
			fresh := newPCMInput(input.format)
			if err := fresh.finish(); err != nil {
				t.Fatalf("state shared: %v", err)
			}
		}
	}
}

func TestPCMFragmentedInputAllocation(t *testing.T) {
	input := newPCMInput(PCMFormat{SampleRate: 16000, Channels: 1})
	input.pending = make([]byte, 0, 2)
	ctx := context.Background()
	fragment := []byte{0}
	allocations := testing.AllocsPerRun(100, func() {
		input.pending = input.pending[:0]
		if err := input.feed(ctx, fragment, func([]byte) error {
			t.Fatal("incomplete frame emitted")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("incomplete frame allocated %.0f times", allocations)
	}
	emitted := false
	if err := input.feed(ctx, fragment, func(chunk []byte) error {
		emitted = true
		if len(chunk) != 2 || cap(chunk) > 64 {
			t.Fatalf("one sample used %d bytes with capacity %d", len(chunk), cap(chunk))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !emitted {
		t.Fatal("complete frame was not emitted")
	}
}

func BenchmarkPCMByteFragments(b *testing.B) {
	input := newPCMInput(PCMFormat{SampleRate: 16000, Channels: 1})
	fragment := []byte{0}
	ctx := context.Background()
	emit := func([]byte) error { return nil }
	b.ReportAllocs()
	b.SetBytes(1)
	for range b.N {
		if err := input.feed(ctx, fragment, emit); err != nil {
			b.Fatal(err)
		}
	}
}
