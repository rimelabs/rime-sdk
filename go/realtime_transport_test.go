package rime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRealtimeControlledAudioWrites(t *testing.T) {
	t.Run("cancel before writer starts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		writes := 0
		write := func(context.Context, []byte) error { writes++; return nil }
		result := rtWriteFrames(context.Background(), rtWrite{caller: ctx, audio: true, frames: [][]byte{{1}, {2}}}, time.Second, write)
		if !result.skipped || writes != 0 {
			t.Fatalf("result=%+v writes=%d", result, writes)
		}
		result = rtWriteFrames(context.Background(), rtWrite{caller: context.Background(), frames: [][]byte{{3}}}, time.Second, write)
		if result.err != nil || writes != 1 {
			t.Fatal("unrelated write failed", result.err)
		}
	})
	t.Run("cancel during first append", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		done := make(chan rtWritten, 1)
		writes := 0
		go func() {
			done <- rtWriteFrames(context.Background(), rtWrite{caller: ctx, audio: true, frames: [][]byte{{1}, {2}}}, time.Second, func(writeCtx context.Context, _ []byte) error {
				writes++
				close(started)
				<-writeCtx.Done()
				return writeCtx.Err()
			})
		}()
		<-started
		cancel()
		result := <-done
		if result.skipped || !errors.Is(result.err, ErrStream) || writes != 1 {
			t.Fatalf("result=%+v writes=%d", result, writes)
		}
	})
	t.Run("control retains submitted write", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := rtWriteFrames(context.Background(), rtWrite{caller: ctx, frames: [][]byte{{1}}}, time.Second, func(writeCtx context.Context, _ []byte) error {
			cancel()
			return writeCtx.Err()
		})
		if result.err != nil || result.skipped {
			t.Fatal(result)
		}
	})
}
