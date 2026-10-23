package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rimelabs/rime-sdk/go/examples/realtime/internal/testpeer"
)

func TestMissingRecordingPreservesFileError(t *testing.T) {
	_ = testpeer.New(t)
	args := os.Args
	os.Args = []string{"recorded", filepath.Join(t.TempDir(), "missing.pcm")}
	t.Cleanup(func() { os.Args = args })
	if err := testpeer.Result(t, testpeer.Run(run)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want missing file", err)
	}
}
