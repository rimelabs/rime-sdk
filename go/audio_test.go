package rime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestSharedAudio(t *testing.T) {
	data, err := os.ReadFile("testdata/audio.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		PCM   string `json:"pcm_hex"`
		Mulaw string `json:"mulaw_hex"`
		Sizes []int  `json:"chunk_bytes"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	pcm, _ := hex.DecodeString(fixture.PCM)
	ulaw, _ := hex.DecodeString(fixture.Mulaw)
	for _, format := range []AudioFormat{PCM24000, MULAW8000} {
		for _, size := range fixture.Sizes {
			t.Run(fmt.Sprintf("%d/%d", format, size), func(t *testing.T) {
				c := converter{format: format}
				var got []byte
				for offset := 0; offset < len(pcm); offset += size {
					chunk, err := c.process(pcm[offset:min(offset+size, len(pcm))], false)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, chunk...)
				}
				last, err := c.process(nil, true)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, last...)
				want := pcm
				if format == MULAW8000 {
					want = ulaw
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("audio mismatch: %x", got)
				}
			})
		}
	}
}

func TestMulawAllInputs(t *testing.T) {
	data, err := os.ReadFile("testdata/mulaw.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Hash string `json:"all_inputs_sha256"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	for sample := -32768; sample <= 32767; sample++ {
		encoded = append(encoded, mulaw(sample))
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != fixture.Hash {
		t.Fatalf("got %s want %s", got, fixture.Hash)
	}
}
