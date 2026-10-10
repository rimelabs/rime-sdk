// Command voice transcribes a terminal microphone turn and speaks its final text.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

type terms []string

func (values *terms) String() string         { return strings.Join(*values, ", ") }
func (values *terms) Set(value string) error { *values = append(*values, value); return nil }

type options struct {
	language, mode, voice, input, output string
	hints                                terms
}

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

func speak(ctx context.Context, client *rime.Client, text string, settings options) error {
	budget := 60 * time.Second
	audio, err := client.TTS.Stream(ctx, text, rime.SynthesisOptions{Language: settings.language, Voice: settings.voice, Timeout: &budget})
	if err != nil {
		return err
	}
	defer audio.Close()
	output := "-d"
	if settings.output != "" {
		output, err = filepath.Abs(settings.output)
		if err != nil {
			return err
		}
	}
	// A completed WAV lets the macOS player manage Bluetooth device format changes.
	nativePlayback := runtime.GOOS == "darwin" && settings.output == ""
	if nativePlayback {
		directory, err := os.MkdirTemp("", "rime-voice-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(directory)
		output = filepath.Join(directory, "reply.wav")
	}
	args := append([]string{"-q"}, pcmOptions(24000)...)
	playbackCtx, cancel := context.WithTimeout(ctx, 125*time.Second)
	defer cancel()
	command := audioCommand(playbackCtx, "sox", append(args, "-", output)...)
	command.Stderr = os.Stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		stdin.Close()
		return err
	}
	waited := false
	defer func() {
		stdin.Close()
		if !waited {
			command.Process.Kill()
			command.Wait()
		}
	}()
	for {
		chunk, err := audio.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := stdin.Write(chunk); err != nil {
			return err
		}
	}
	if err := stdin.Close(); err != nil {
		return err
	}
	err = command.Wait()
	waited = true
	if err != nil {
		return fmt.Errorf("SoX playback failed: %w", err)
	}
	if nativePlayback {
		player := audioCommand(playbackCtx, "afplay", output)
		player.Stderr = os.Stderr
		if err := player.Run(); err != nil {
			return fmt.Errorf("audio playback failed: %w", err)
		}
	}
	fmt.Printf("TTS request: %s\n", audio.RequestID())
	return nil
}

func turn(ctx context.Context, client *rime.Client, settings options, lines <-chan string) error {
	input := &capture{input: settings.input, lines: lines}
	defer input.close()
	budget := 120 * time.Second
	stream, err := client.STT.Stream(ctx, input.next, rime.TranscriptionOptions{
		Language: settings.language, Mode: rime.TranscriptionMode(settings.mode), ContextTerms: settings.hints, Timeout: &budget,
	})
	if err != nil {
		return err
	}
	defer stream.Close()
	var final *rime.TranscriptionFinal
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch value := update.(type) {
		case rime.TranscriptionPartial:
			fmt.Printf("partial: %s\n", value.Text)
		case rime.TranscriptionFinal:
			final = &value
			fmt.Printf("final: %s\n", value.Text)
		}
	}
	fmt.Printf("STT request: %s\n", stream.RequestID())
	if final == nil {
		return fmt.Errorf("transcription ended without a final result")
	}
	if strings.TrimSpace(final.Text) == "" {
		fmt.Println("No speech recognized.")
		return nil
	}
	// The response step echoes the final transcript so recognition stays visible.
	return speak(ctx, client, final.Text, settings)
}

func terminalLines(ctx context.Context, reader io.Reader) <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case lines <- scanner.Text():
			}
		}
	}()
	return lines
}

func run() error {
	var settings options
	flag.StringVar(&settings.language, "language", "", "spoken language, e.g. en or es (required)")
	flag.StringVar(&settings.mode, "mode", "written", "written or verbatim")
	flag.StringVar(&settings.voice, "voice", "", "TTS voice; defaults to the SDK's Coda voice")
	flag.StringVar(&settings.input, "input", "", "transcribe one audio file instead of the microphone")
	flag.StringVar(&settings.output, "output", "", "save the spoken reply to a WAV file instead of playing")
	flag.Var(&settings.hints, "term", "recognition hint; repeatable")
	flag.Parse()
	if settings.language == "" || flag.NArg() != 0 {
		return fmt.Errorf("usage: voice --language en [--mode written|verbatim] [--term Rime] [--voice clementine] [--input audio.wav --output reply.wav]")
	}
	if settings.output != "" && settings.input == "" {
		return fmt.Errorf("--output requires --input")
	}
	if err := validateCaptureMode(settings.input, runtime.GOOS); err != nil {
		return err
	}
	if _, err := exec.LookPath("sox"); err != nil {
		return fmt.Errorf("install SoX first: brew install sox (macOS)")
	}
	client, err := rime.NewClient(rime.Config{})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if settings.input != "" {
		return turn(ctx, client, settings, nil)
	}
	fmt.Println("Voice echo: microphone → STT → TTS. Ctrl+C stops everything.")
	lines := terminalLines(ctx, os.Stdin)
	for {
		fmt.Println("\nPress Enter to talk, or type q then Enter to quit.")
		select {
		case <-ctx.Done():
			return nil
		case line, open := <-lines:
			if !open || strings.ToLower(strings.TrimSpace(line)) == "q" {
				return nil
			}
		}
		if err := turn(ctx, client, settings, lines); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		if cause := errors.Unwrap(err); cause != nil {
			fmt.Fprintln(os.Stderr, "Cause:", cause)
		}
		os.Exit(1)
	}
}
