package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestAudioHelper(t *testing.T) {
	if os.Getenv("RIME_TEST_AUDIO_PROCESS") != "1" {
		return
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	_, _ = os.Stdout.Write(make([]byte, 1280))
	if os.Getenv("RIME_TEST_AUDIO_FAILURE") == "1" {
		os.Exit(7)
	}
	if os.Getenv("RIME_TEST_AUDIO_LIVE") == "1" {
		<-interrupt
	}
	os.Exit(0)
}

func fakeAudio(t *testing.T, live, fail bool) {
	t.Helper()
	original := audioCommand
	t.Cleanup(func() { audioCommand = original })
	audioCommand = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAudioHelper$")
		command.Env = append(os.Environ(), "RIME_TEST_AUDIO_PROCESS=1")
		if live {
			command.Env = append(command.Env, "RIME_TEST_AUDIO_LIVE=1")
		}
		if fail {
			command.Env = append(command.Env, "RIME_TEST_AUDIO_FAILURE=1")
		}
		return command
	}
}

func TestCaptureFileAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "failure"}[fail], func(t *testing.T) {
			fakeAudio(t, false, fail)
			input := &capture{input: "fixture.wav"}
			defer input.close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			bytes := 0
			for {
				chunk, err := input.next(ctx)
				bytes += len(chunk)
				if err == nil {
					continue
				}
				if fail {
					if err == io.EOF {
						t.Fatal("failed recorder committed its utterance")
					}
				} else if err != io.EOF {
					t.Fatal(err)
				}
				break
			}
			if bytes != 1280 {
				t.Fatalf("got %d input bytes", bytes)
			}
			if input.command.ProcessState == nil {
				t.Fatal("recorder not reaped")
			}
		})
	}
}

func TestCaptureEnterAndCancel(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("terminal capture uses POSIX interrupt signals")
	}
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enter", true: "cancel"}[cancelled], func(t *testing.T) {
			fakeAudio(t, true, false)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			lines := make(chan string, 1)
			input := &capture{lines: lines}
			defer input.close()
			if chunk, err := input.next(ctx); err != nil || len(chunk) == 0 {
				t.Fatalf("%d %v", len(chunk), err)
			}
			if cancelled {
				cancel()
			} else {
				lines <- ""
			}
			_, err := input.next(ctx)
			if cancelled {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("%v", err)
				}
			} else if err != io.EOF {
				t.Fatal(err)
			}
			input.close()
			if input.command.ProcessState == nil {
				t.Fatal("recorder not reaped")
			}
		})
	}
}

func TestUnexpectedMicrophoneExitIsFailure(t *testing.T) {
	fakeAudio(t, false, false)
	input := &capture{lines: make(chan string)}
	defer input.close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := input.next(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := input.next(ctx); err == nil || err == io.EOF {
		t.Fatal("unexpected microphone exit accepted")
	}
}

func TestTerminalInputEndsAtEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lines := terminalLines(ctx, strings.NewReader("\nq\n"))
	if value := <-lines; value != "" {
		t.Fatal(value)
	}
	if value := <-lines; value != "q" {
		t.Fatal(value)
	}
	if _, open := <-lines; open {
		t.Fatal("stdin EOF was ignored")
	}
}
