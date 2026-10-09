// Audio capture follows the terminal voice example's lazy SoX lifecycle.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// PCM pipes carry headerless signed little-endian 16-bit mono samples.
func pcmOptions(rate int) []string {
	return []string{"-t", "raw", "-e", "signed-integer", "-b", "16", "-L", "-r", strconv.Itoa(rate), "-c", "1"}
}

var audioCommand = exec.CommandContext

func validateCaptureMode(input, operatingSystem string) error {
	if operatingSystem == "windows" && input == "" {
		return fmt.Errorf("microphone capture is not supported on Windows; use --input recording.wav")
	}
	return nil
}

type capture struct {
	input            string
	lines            <-chan string
	mu               sync.Mutex
	command          *exec.Cmd
	stdout           io.ReadCloser
	finished         chan struct{}
	stopping, forced atomic.Bool
	waitOnce         sync.Once
	waitError        error
}

func (input *capture) start(ctx context.Context) error {
	if err := validateCaptureMode(input.input, runtime.GOOS); err != nil {
		return err
	}
	name := "-d"
	if input.input != "" {
		var err error
		name, err = filepath.Abs(input.input)
		if err != nil {
			return err
		}
	}
	args := append([]string{"-q", "--buffer", "1280", name}, pcmOptions(16000)...)
	command := audioCommand(ctx, "sox", append(args, "-")...)
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	input.mu.Lock()
	defer input.mu.Unlock()
	if err := command.Start(); err != nil {
		stdout.Close()
		return err
	}
	input.command, input.stdout, input.finished = command, stdout, make(chan struct{})
	if input.lines != nil {
		fmt.Println("Listening. Press Enter to finish.")
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-input.finished:
				return
			case <-input.lines:
				input.stopping.Store(true)
				_ = command.Process.Signal(os.Interrupt)
			}
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
			case <-input.finished:
			case <-timer.C:
				input.forced.Store(true)
				_ = command.Process.Kill()
			}
		}()
	}
	return nil
}

func (input *capture) next(ctx context.Context) ([]byte, error) {
	if input.command == nil {
		if err := input.start(ctx); err != nil {
			return nil, err
		}
	}
	data := make([]byte, 1280)
	count, err := input.stdout.Read(data)
	if err != io.EOF {
		return data[:count], err
	}
	err = input.wait()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if input.forced.Load() {
		return nil, fmt.Errorf("microphone did not stop cleanly")
	}
	if err != nil {
		// SoX may exit successfully or report SIGINT when the caller ends capture.
		var exitError *exec.ExitError
		interrupted := false
		if errors.As(err, &exitError) {
			if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
				interrupted = status.Signaled() && status.Signal() == syscall.SIGINT
			}
		}
		if !input.stopping.Load() || !interrupted {
			return nil, fmt.Errorf("SoX capture failed: %w", err)
		}
	}
	if input.lines != nil && !input.stopping.Load() {
		return nil, fmt.Errorf("microphone stopped before the turn was finished")
	}
	return data[:count], io.EOF
}

func (input *capture) wait() error {
	input.waitOnce.Do(func() { input.waitError = input.command.Wait(); close(input.finished) })
	return input.waitError
}

func (input *capture) close() {
	input.mu.Lock()
	defer input.mu.Unlock()
	if input.command != nil {
		_ = input.command.Process.Kill()
		_ = input.wait()
	}
}
