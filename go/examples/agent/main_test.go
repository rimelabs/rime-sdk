package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rime "github.com/rimelabs/rime-sdk/go"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (fn roundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }
func llmResponse(body string, status int) *http.Response {
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{"X-Request-Id": []string{"llm-request"}}, Body: io.NopCloser(strings.NewReader(body))}
}

const llmAnswer = `{"status":"completed","output":[{"type":"reasoning"},{"type":"message","content":[{"type":"output_text","text":"Hello. I read 22 pages."}]}]}`

func TestResponseUsesBoundedHistoryAndMessageOutput(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	history := make([]message, 24)
	for i := range history {
		history[i] = message{"user", "Hi"}
	}
	client := &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://api.openai.com/v1/responses" || request.Header.Get("Authorization") != "Bearer test-openai-key" {
			t.Fatal("wrong request destination or auth")
		}
		var input struct {
			Input []message `json:"input"`
			Store bool      `json:"store"`
			Model string    `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if len(input.Input) != 21 || input.Input[20].Content != "Again" || input.Store || input.Model != "gpt-4.1-mini" {
			t.Fatalf("unexpected body: %+v", input)
		}
		return llmResponse(llmAnswer, 200), nil
	})}
	text, id, err := respond(context.Background(), client, options{llmModel: "gpt-4.1-mini", instructions: instructions}, history, "Again")
	if err != nil || text != "Hello. I read 22 pages." || id != "llm-request" {
		t.Fatalf("text=%q id=%q err=%v", text, id, err)
	}
}

func TestBadLLMResponses(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	for _, test := range []struct {
		body   string
		status int
		want   string
	}{
		{`{"status":"incomplete"}`, 200, "did not complete"},
		{`{"status":"completed","output":[]}`, 200, "no speakable text"},
		{`{"error":{"message":"Invalid test-openai-key"}}`, 401, "[redacted]"},
	} {
		t.Run(test.want, func(t *testing.T) {
			client := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) { return llmResponse(test.body, test.status), nil })}
			_, _, err := respond(context.Background(), client, options{}, nil, "Hi")
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "test-openai-key") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLexiconShapeAndPronunciationPassthrough(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lexicon.json")
	for _, body := range []string{`null`, `{}`, `[{}]`, `[{"spelling":"hello","pronunciation":3}]`, `[{"spelling":"hello","pronunciation":"h","extra":true}]`, `[] []`} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadLexicon(path); err == nil {
			t.Fatalf("accepted bad JSON: %s", body)
		}
	}
	if err := os.WriteFile(path, []byte(`[{"spelling":"Hello","pronunciation":"h @ . l oU"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	entries, err := loadLexicon(path)
	if err != nil || len(entries) != 1 || entries[0].Pronunciation != "h @ . l oU" {
		t.Fatalf("server should validate pronunciation: %+v %v", entries, err)
	}
}

func TestMissingOpenAIKeyDoesNotStartSpeech(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	client, err := rime.NewClient(rime.Config{APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	settings := options{model: "coda", language: "en", mode: "written", noPlayback: true, outputDir: t.TempDir()}
	history := []message{}
	err = turn(context.Background(), client, &http.Client{}, settings, &history, 1, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("%v", err)
	}
	data, err := os.ReadFile(filepath.Join(settings.outputDir, "turn-001.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err = json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report["stage"] != "configuration" || report["status"] != "error" {
		t.Fatalf("%s", data)
	}
	if files, _ := filepath.Glob(filepath.Join(settings.outputDir, "*.wav")); len(files) != 0 {
		t.Fatal("failed turn produced audio")
	}
}

// Opt-in qualification uses production Rime with the checked-in speech fixture.
// The OpenAI transport is simulated; no OpenAI request or key is needed.
func TestLiveCascade(t *testing.T) {
	if os.Getenv("RIME_AGENT_LIVE") != "1" {
		t.Skip("set RIME_AGENT_LIVE=1 and RIME_API_KEY to qualify production Rime")
	}
	if os.Getenv("RIME_API_KEY") == "" {
		t.Fatal("RIME_API_KEY is required")
	}
	t.Setenv("OPENAI_API_KEY", "simulated-openai-key")
	for _, model := range []string{"coda", "mistv3"} {
		t.Run(model, func(t *testing.T) {
			settings := options{model: model, language: "en", mode: "written", noPlayback: true, outputDir: t.TempDir(), input: "../../../examples/audio/france.wav", llmModel: "gpt-4.1-mini", instructions: instructions, timestamps: model == "mistv3"}
			if model == "coda" {
				settings.lexicon = filepath.Join(settings.outputDir, "lexicon.json")
				if err := os.WriteFile(settings.lexicon, []byte(`[{"spelling":"hello","pronunciation":"h @ . \" l oU"}]`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			client, err := rime.NewClient(rime.Config{Model: model})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			called := false
			llm := &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
				var input struct {
					Input []message `json:"input"`
				}
				if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
					t.Fatal(err)
				}
				if len(input.Input) != 1 || !strings.Contains(strings.ToLower(input.Input[0].Content), "france") {
					t.Fatalf("bad final STT transcript: %+v", input)
				}
				called = true
				return llmResponse(llmAnswer, 200), nil
			})}
			history := []message{}
			if err := turn(context.Background(), client, llm, settings, &history, 1, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if !called || len(history) != 2 {
				t.Fatal("incomplete cascade")
			}
			data, err := os.ReadFile(filepath.Join(settings.outputDir, "turn-001.wav"))
			if err != nil {
				t.Fatal(err)
			}
			if len(data) <= 44 || string(data[:4]) != "RIFF" || binary.LittleEndian.Uint32(data[24:]) != 24000 {
				t.Fatal("bad output WAV")
			}
		})
	}
}
