// Command agent runs a local Rime STT → OpenAI → Rime TTS conversation.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	rime "github.com/rimelabs/rime-sdk/go"
)

const instructions = "You are a concise voice assistant. Reply in one or two short sentences, without markdown. When asked to repeat text, repeat it exactly. Reply in the user's language."

type terms []string

func (values *terms) String() string         { return strings.Join(*values, ", ") }
func (values *terms) Set(value string) error { *values = append(*values, value); return nil }

type options struct {
	model, language, voice, lexicon, llmModel, instructions, mode, endpoint, sttEndpoint, input, say, outputDir string
	timestamps, completeText, noPlayback, direct                                                                bool
	hints                                                                                                       terms
}
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func redact(value string) string {
	for _, name := range []string{"RIME_API_KEY", "OPENAI_API_KEY"} {
		if key := os.Getenv(name); key != "" {
			value = strings.ReplaceAll(value, key, "[redacted]")
		}
	}
	return value
}
func loadLexicon(path string) ([]rime.PronunciationEntry, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var rows []struct {
		Spelling      *string `json:"spelling"`
		Pronunciation *string `json:"pronunciation"`
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rows); err != nil {
		return nil, fmt.Errorf("lexicon JSON: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("lexicon must be a JSON array")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("lexicon must contain one JSON array")
	}
	entries := make([]rime.PronunciationEntry, len(rows))
	for i, row := range rows {
		if row.Spelling == nil || row.Pronunciation == nil {
			return nil, fmt.Errorf("lexicon entry %d needs string spelling and pronunciation", i)
		}
		entries[i] = rime.PronunciationEntry{Spelling: *row.Spelling, Pronunciation: *row.Pronunciation}
	}
	return entries, nil
}

func respond(ctx context.Context, client *http.Client, settings options, history []message, text string) (string, string, error) {
	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		return "", "", fmt.Errorf("set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY")
	}
	start := max(0, len(history)-20)
	input := append(append([]message(nil), history[start:]...), message{"user", text})
	data, err := json.Marshal(map[string]any{"model": settings.llmModel, "instructions": settings.instructions, "input": input, "max_output_tokens": 512, "store": false})
	if err != nil {
		return "", "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewReader(data))
	if err != nil {
		return "", "", err
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return "", "", err
	}
	defer response.Body.Close()
	id := response.Header.Get("x-request-id")
	var body struct {
		Status string `json:"status"`
		Error  struct {
			Message string `json:"message"`
		} `json:"error"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail := body.Error.Message
		if detail == "" {
			detail = response.Status
		}
		return "", id, fmt.Errorf("%s", redact(fmt.Sprintf("OpenAI HTTP %d: %s; request_id=%s", response.StatusCode, detail, id)))
	}
	if decodeErr != nil {
		return "", id, fmt.Errorf("OpenAI response JSON: %w", decodeErr)
	}
	if body.Status != "completed" {
		return "", id, fmt.Errorf("OpenAI response did not complete: %s; request_id=%s", body.Status, id)
	}
	var reply strings.Builder
	for _, item := range body.Output {
		if item.Type == "message" {
			for _, part := range item.Content {
				if part.Type == "output_text" {
					reply.WriteString(part.Text)
				}
			}
		}
	}
	answer := strings.TrimSpace(reply.String())
	if answer == "" {
		return "", id, fmt.Errorf("OpenAI returned no speakable text; request_id=%s", id)
	}
	return answer, id, nil
}

func transcribe(ctx context.Context, client *rime.Client, settings options, lines <-chan string) (string, string, error) {
	input := &capture{input: settings.input, lines: lines}
	defer input.close()
	budget := 120 * time.Second
	stream, err := client.STT.Stream(ctx, input.next, rime.TranscriptionOptions{Language: settings.language, Mode: rime.TranscriptionMode(settings.mode), ContextTerms: settings.hints, Timeout: &budget})
	if err != nil {
		return "", "", err
	}
	defer stream.Close()
	var final *rime.TranscriptionFinal
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", stream.RequestID(), err
		}
		switch value := update.(type) {
		case rime.TranscriptionPartial:
			fmt.Println("partial:", value.Text)
		case rime.TranscriptionFinal:
			final = &value
			fmt.Println("final:", value.Text)
		}
	}
	if final == nil {
		return "", stream.RequestID(), fmt.Errorf("transcription ended without a final result")
	}
	return final.Text, stream.RequestID(), nil
}

func saveAudio(audio *rime.AudioStream, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.Write(make([]byte, 44)); err != nil {
		return err
	}
	count := 0
	for {
		chunk, readErr := audio.Recv()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
		n, writeErr := file.Write(chunk)
		if writeErr != nil {
			return writeErr
		}
		count += n
	}
	if uint64(count) > uint64(^uint32(0))-36 {
		return fmt.Errorf("audio exceeds WAV size limit")
	}
	header := make([]byte, 44)
	copy(header, "RIFF")
	binary.LittleEndian.PutUint32(header[4:], uint32(count+36))
	copy(header[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(header[16:], 16)
	binary.LittleEndian.PutUint16(header[20:], 1)
	binary.LittleEndian.PutUint16(header[22:], 1)
	binary.LittleEndian.PutUint32(header[24:], 24000)
	binary.LittleEndian.PutUint32(header[28:], 48000)
	binary.LittleEndian.PutUint16(header[32:], 2)
	binary.LittleEndian.PutUint16(header[34:], 16)
	copy(header[36:], "data")
	binary.LittleEndian.PutUint32(header[40:], uint32(count))
	if _, err = file.WriteAt(header, 0); err != nil {
		return err
	}
	return file.Close()
}

func playback(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 65*time.Second)
	defer cancel()
	name, args := "sox", []string{"-q", path, "-d"}
	if runtime.GOOS == "darwin" {
		name, args = "afplay", []string{path}
	}
	command := audioCommand(ctx, name, args...)
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("playback failed: %w", err)
	}
	return nil
}

func turn(ctx context.Context, client *rime.Client, httpClient *http.Client, settings options, history *[]message, number int, lines <-chan string, say, ask *string) (resultErr error) {
	stem := filepath.Join(settings.outputDir, fmt.Sprintf("turn-%03d", number))
	partial := stem + ".partial.wav"
	report := map[string]any{"model": settings.model, "language": settings.language, "voice": settings.voice, "llm_model": settings.llmModel, "stage": "configuration", "status": "pending"}
	defer func() {
		os.Remove(partial)
		if resultErr != nil {
			report["status"] = "error"
			if ctx.Err() != nil {
				report["status"] = "cancelled"
			}
			kind, id := "Error", ""
			var sdkError *rime.Error
			if errors.As(resultErr, &sdkError) {
				kind, id = string(sdkError.Kind), sdkError.RequestID
			}
			detail := map[string]string{"type": kind, "message": redact(resultErr.Error()), "request_id": id}
			report["error"] = detail
			fmt.Fprintf(os.Stderr, "%s error: %s; request_id=%s\n", report["stage"], detail["message"], id)
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(stem+".json", append(data, '\n'), 0600)
		}
		if err != nil {
			resultErr = errors.Join(resultErr, err)
		} else {
			fmt.Println("Report:", stem+".json")
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	lexicon, err := loadLexicon(settings.lexicon)
	if err != nil {
		return err
	}
	// Keep reports in the same JSON shape as Python and TypeScript.
	entries := make([]map[string]string, 0, len(lexicon))
	for _, entry := range lexicon {
		entries = append(entries, map[string]string{"spelling": entry.Spelling, "pronunciation": entry.Pronunciation})
	}
	report["custom_lexicon"], report["timestamps_requested"] = entries, settings.timestamps
	report["complete_text"] = settings.completeText
	var transcript, reply string
	if say != nil {
		reply = *say
	} else {
		if os.Getenv("OPENAI_API_KEY") == "" {
			return fmt.Errorf("set OPENAI_API_KEY for conversation turns; /say only needs RIME_API_KEY")
		}
		if ask != nil {
			transcript = *ask
		} else {
			report["stage"] = "stt"
			var id string
			transcript, id, err = transcribe(ctx, client, settings, lines)
			report["stt_request_id"] = id
			if err != nil {
				return err
			}
			fmt.Println("STT request:", id)
		}
		report["transcript"] = transcript
		if strings.TrimSpace(transcript) == "" {
			report["status"] = "silence"
			fmt.Println("No speech recognized.")
			return nil
		}
		report["stage"] = "llm"
		var id string
		reply, id, err = respond(ctx, httpClient, settings, *history, transcript)
		report["llm_request_id"] = id
		if err != nil {
			return err
		}
		fmt.Println("OpenAI request:", id)
	}
	report["reply"] = reply
	fmt.Println("Assistant:", reply)
	report["stage"] = "tts"
	budget := 60 * time.Second
	audio, err := client.TTS.Stream(ctx, reply, rime.SynthesisOptions{Language: settings.language, Voice: settings.voice, CustomLexicon: lexicon, Timestamps: settings.timestamps, CompleteText: settings.completeText, Timeout: &budget})
	if err != nil {
		return err
	}
	defer audio.Close()
	if err = saveAudio(audio, partial); err != nil {
		return err
	}
	report["tts_request_id"] = audio.RequestID()
	fmt.Println("TTS request:", audio.RequestID())
	if err = os.Rename(partial, stem+".wav"); err != nil {
		return err
	}
	report["audio_file"] = stem + ".wav"
	if settings.timestamps {
		report["stage"] = "timestamps"
		timings, err := audio.Timestamps()
		if err != nil {
			return err
		}
		spans := make([]map[string]any, 0, len(timings.Spans))
		fmt.Printf("Timestamp status: %d %s\n", timings.Status.Code, timings.Status.Message)
		for _, word := range timings.Spans {
			spans = append(spans, map[string]any{"text": word.Text, "start": word.Start, "end": word.End})
			fmt.Printf("  %8.3f–%8.3f  %s\n", word.Start, word.End, word.Text)
		}
		report["timestamps"] = map[string]any{"status": map[string]any{"code": int(timings.Status.Code), "message": timings.Status.Message}, "spans": spans}
	}
	if !settings.noPlayback {
		report["stage"] = "playback"
		if err = playback(ctx, stem+".wav"); err != nil {
			return err
		}
	}
	if say == nil {
		*history = append(*history, message{"user", transcript}, message{"assistant", reply})
		if len(*history) > 20 {
			*history = (*history)[len(*history)-20:]
		}
	}
	report["stage"], report["status"] = "done", "ok"
	return nil
}

func arguments(argv []string) (options, error) {
	var settings options
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	model := os.Getenv("OPENAI_MODEL")
	if model == "" {
		model = "gpt-4.1-mini"
	}
	flags.StringVar(&settings.model, "model", "coda", "coda or mistv3")
	flags.StringVar(&settings.language, "language", "en", "spoken language")
	flags.StringVar(&settings.voice, "voice", "", "TTS voice; defaults to selected model's voice")
	flags.BoolVar(&settings.timestamps, "timestamps", false, "request final Mist v3 word timestamps")
	flags.BoolVar(&settings.completeText, "complete-text", false, "send complete text with Synthesize; audio still streams")
	flags.StringVar(&settings.lexicon, "lexicon", "", "JSON entries; reloaded before every turn")
	flags.StringVar(&settings.llmModel, "llm-model", model, "OpenAI model")
	flags.StringVar(&settings.instructions, "instructions", instructions, "LLM instructions")
	flags.StringVar(&settings.mode, "mode", "written", "written or verbatim STT")
	flags.Var(&settings.hints, "term", "recognition hint; repeatable")
	flags.StringVar(&settings.endpoint, "endpoint", "", "optional Rime TTS host:port")
	flags.StringVar(&settings.sttEndpoint, "stt-endpoint", "", "optional Rime STT host:port")
	flags.StringVar(&settings.input, "input", "", "one recorded-audio conversation turn")
	flags.StringVar(&settings.say, "say", "", "one direct TTS test; bypasses STT and OpenAI")
	flags.BoolVar(&settings.noPlayback, "no-playback", false, "save audio without opening speakers")
	flags.StringVar(&settings.outputDir, "output-dir", "", "parent for a new run directory; default is OS temp directory")
	if err := flags.Parse(argv); err != nil {
		return settings, err
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "say" {
			settings.direct = true
		}
	})
	if flags.NArg() != 0 {
		return settings, fmt.Errorf("unexpected positional arguments")
	}
	if settings.model != "coda" && settings.model != "mistv3" {
		return settings, fmt.Errorf("--model must be coda or mistv3")
	}
	if settings.mode != "written" && settings.mode != "verbatim" {
		return settings, fmt.Errorf("--mode must be written or verbatim")
	}
	if settings.input != "" && settings.direct {
		return settings, fmt.Errorf("use either --input or --say")
	}
	return settings, nil
}

func terminalLines(ctx context.Context, reader io.Reader) <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 1<<20)
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
	settings, err := arguments(os.Args[1:])
	if err != nil {
		return err
	}
	if settings.outputDir != "" {
		if err = os.MkdirAll(settings.outputDir, 0700); err != nil {
			return err
		}
	}
	settings.outputDir, err = os.MkdirTemp(settings.outputDir, "rime-agent-go-")
	if err != nil {
		return err
	}
	settings.outputDir, err = filepath.Abs(settings.outputDir)
	if err != nil {
		return err
	}
	fmt.Println("Artifacts:", settings.outputDir)
	client, err := rime.NewClient(rime.Config{Model: settings.model, Endpoint: settings.endpoint, STTEndpoint: settings.sttEndpoint})
	if err != nil {
		return err
	}
	defer client.Close()
	httpClient := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer httpClient.CloseIdleConnections()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	history := []message{}
	if settings.input != "" || settings.direct {
		var say *string
		if settings.direct {
			say = &settings.say
		}
		return turn(ctx, client, httpClient, settings, &history, 1, nil, say, nil)
	}
	fmt.Println("Enter to talk; /say TEXT tests TTS; /ask TEXT talks to the LLM; /reset; /quit. Ctrl+C exits.")
	lines := terminalLines(ctx, os.Stdin)
	number := 0
	for {
		fmt.Print("\nReady > ")
		var command string
		select {
		case <-ctx.Done():
			return nil
		case line, open := <-lines:
			if !open {
				return nil
			}
			command = strings.TrimSpace(line)
		}
		if command == "q" || command == "/quit" {
			return nil
		}
		if command == "/reset" {
			history = nil
			fmt.Println("Conversation cleared.")
			continue
		}
		var say, ask *string
		if strings.HasPrefix(command, "/say ") {
			value := strings.TrimSpace(command[5:])
			say = &value
		}
		if strings.HasPrefix(command, "/ask ") {
			value := strings.TrimSpace(command[5:])
			ask = &value
		}
		if command != "" && say == nil && ask == nil {
			fmt.Println("Use Enter, /say TEXT, /ask TEXT, /reset, or /quit.")
			continue
		}
		number++
		if err = turn(ctx, client, httpClient, settings, &history, number, lines, say, ask); err != nil && ctx.Err() != nil {
			return nil
		}
	}
}
func main() {
	if err := run(); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, redact("Error: "+err.Error()))
		os.Exit(1)
	}
}
