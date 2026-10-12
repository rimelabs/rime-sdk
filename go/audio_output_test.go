package rime

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type outputContract struct {
	Outputs []struct {
		Format     string
		SampleRate int    `json:"sample_rate"`
		WireFormat string `json:"wire_format"`
		WireRate   int    `json:"wire_rate"`
	} `json:"output_cases"`
	WAVs []struct {
		Name   string
		Input  string `json:"input_hex"`
		Output string `json:"output_hex"`
		Error  bool
	} `json:"wav_cases"`
	InvalidRates   []int     `json:"invalid_sample_rates"`
	InvalidFactors []float64 `json:"invalid_time_scale_factors"`
}

func readOutputContract(t *testing.T) outputContract {
	t.Helper()
	data, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var result outputContract
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func outputService(mime string, payload []byte, final error, hold bool) *testService {
	return &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
		if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", mime, "x-request-id", "test-request")); err != nil {
			return err
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
		// Split inside the PCM sample / container header.
		for _, part := range [][]byte{payload[:1], payload[1:]} {
			if err := stream.Send(audioResponse(part)); err != nil {
				return err
			}
		}
		if hold {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		return final
	}}
}
func TestOutputHeadersAndChunkBoundaries(t *testing.T) {
	formats := map[string]AudioFormat{"PCM": PCM, "PCM_24000": PCM24000, "MULAW_8000": MULAW8000, "WAV": WAV, "MP3": MP3}
	for _, entry := range readOutputContract(t).Outputs {
		for _, incremental := range []bool{false, true} {
			t.Run(entry.Format+"/"+strconv.Itoa(entry.SampleRate)+"/"+map[bool]string{true: "stream", false: "string"}[incremental], func(t *testing.T) {
				payload := []byte{1, 0, 2, 0, 3, 0}
				if entry.Format == "WAV" || entry.Format == "MP3" {
					payload = []byte("encoded")
				}
				service := outputService(entry.WireFormat, payload, nil, false)
				factor := 0.8
				service.header = func(header *pb.SynthesisRequest) error {
					p := header.GetAudioParameters()
					if p.GetAudioFormat() != entry.WireFormat || p.GetSamplingRate() != int32(entry.WireRate) || math.Abs(float64(p.GetTimeScaleFactor())-factor) > 1e-6 {
						return status.Error(codes.InvalidArgument, "incorrect audio options")
					}
					return nil
				}
				client := setupClient(t, service)
				opts := SynthesisOptions{AudioFormat: formats[entry.Format], SampleRate: entry.SampleRate, TimeScaleFactor: &factor}
				var audio *AudioStream
				var err error
				if incremental {
					sent := false
					audio, err = client.TTS.Stream(context.Background(), func(context.Context) (string, error) {
						if sent {
							return "", io.EOF
						}
						sent = true
						return "Hello.", nil
					}, opts)
				} else {
					audio, err = client.TTS.Synthesize(context.Background(), "Hello.", opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer audio.Close()
				if audio.SampleRate() != entry.SampleRate {
					t.Fatal("wrong output sample rate")
				}
				data, err := drain(audio)
				if err != nil {
					t.Fatal(err)
				}
				if entry.Format == "MULAW_8000" {
					if len(data) != 1 {
						t.Fatal("wrong mu-law length")
					}
				} else if !bytes.Equal(data, payload) {
					t.Fatal("output bytes changed")
				}
			})
		}
	}
}
func TestBufferedWAVConformance(t *testing.T) {
	for _, entry := range readOutputContract(t).WAVs {
		t.Run(entry.Name, func(t *testing.T) {
			payload, _ := hex.DecodeString(entry.Input)
			service := outputService("audio/wav", payload, nil, false)
			service.header = func(h *pb.SynthesisRequest) error {
				if h.GetAudioParameters().TimeScaleFactor != nil {
					return status.Error(codes.InvalidArgument, "unexpected factor")
				}
				return nil
			}
			client := setupClient(t, service)
			data, err := client.TTS.SynthesizeToBuffer(context.Background(), "Hello.", SynthesisOptions{AudioFormat: WAV, SampleRate: 16000})
			if entry.Error {
				if !errors.Is(err, ErrAudioFormat) || data != nil {
					t.Fatalf("expected format error, got %x %v", data, err)
				}
			} else if err != nil || hex.EncodeToString(data) != entry.Output {
				t.Fatalf("wrong WAV: %x %v", data, err)
			}
		})
	}
}
func TestOutputValidation(t *testing.T) {
	service := &testService{}
	client := setupClient(t, service)
	for _, rate := range readOutputContract(t).InvalidRates {
		if rate == 0 {
			continue
		}
		if _, err := client.TTS.Synthesize(context.Background(), "Hello.", SynthesisOptions{SampleRate: rate}); !errors.Is(err, ErrAudioFormat) {
			t.Fatalf("rate %d: %v", rate, err)
		}
	}
	factors := append(readOutputContract(t).InvalidFactors, math.NaN(), math.Inf(1))
	for _, factor := range factors {
		if _, err := client.TTS.Synthesize(context.Background(), "Hello.", SynthesisOptions{TimeScaleFactor: &factor}); !errors.Is(err, ErrInput) {
			t.Fatalf("factor %v: %v", factor, err)
		}
	}
	for _, opts := range []SynthesisOptions{{AudioFormat: PCM24000, SampleRate: 8000}, {AudioFormat: MULAW8000, SampleRate: 16000}} {
		if _, err := client.TTS.Synthesize(context.Background(), "Hello.", opts); !errors.Is(err, ErrAudioFormat) {
			t.Fatal(err)
		}
	}
	if service.calls.Load() != 0 {
		t.Fatal("invalid input made an RPC")
	}
}
func TestBufferFailures(t *testing.T) {
	for _, mode := range []string{"late_error", "timeout", "cancel", "mismatch"} {
		t.Run(mode, func(t *testing.T) {
			var final error
			hold := false
			mime := "audio/pcm"
			opts := SynthesisOptions{}
			kind := ErrResourceLimit
			switch mode {
			case "late_error":
				final = status.Error(codes.Unavailable, "late failure")
				kind = ErrUnavailable
			case "timeout":
				hold = true
				budget := 100 * time.Millisecond
				opts.Timeout = &budget
				kind = ErrTimeout
			case "cancel":
				hold = true
				kind = ErrCancelled
			case "mismatch":
				mime = "audio/wav"
				kind = ErrAudioFormat
			}
			service := outputService(mime, []byte{1, 0}, final, hold)
			client := setupClient(t, service)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				service.header = func(*pb.SynthesisRequest) error { cancel(); return nil }
			}
			data, err := client.TTS.SynthesizeToBuffer(ctx, "Hello.", opts)
			if data != nil || !errors.Is(err, kind) {
				t.Fatalf("partial result or wrong error: %x %v", data, err)
			}
			if mode != "cancel" {
				var e *Error
				if !errors.As(err, &e) || e.RequestID != "test-request" {
					t.Fatalf("missing request ID: %v", err)
				}
			}
		})
	}
}
func TestBufferedMP3(t *testing.T) {
	payload := []byte("odd-mp3")
	client := setupClient(t, outputService("audio/mpeg", payload, nil, false))
	data, err := client.TTS.SynthesizeToBuffer(context.Background(), "Hello.", SynthesisOptions{AudioFormat: MP3})
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("MP3 changed: %x %v", data, err)
	}
}

func TestSampleRateWithoutFormatAndFactorBoundaries(t *testing.T) {
	for i, rate := range []int{8000, 16000, 24000} {
		factor := []float64{0.4, 1, 2.5}[i]
		service := &testService{header: func(h *pb.SynthesisRequest) error {
			p := h.GetAudioParameters()
			if p.GetSamplingRate() != int32(rate) || math.Abs(float64(p.GetTimeScaleFactor())-factor) > 1e-6 {
				return status.Error(codes.InvalidArgument, "wrong audio options")
			}
			return nil
		}}
		client := setupClient(t, service)
		audio, err := client.TTS.SynthesizeToBuffer(context.Background(), "Hello.", SynthesisOptions{SampleRate: rate, TimeScaleFactor: &factor})
		if err != nil || len(audio) != 4800 {
			t.Fatalf("rate %d: %v, %d bytes", rate, err, len(audio))
		}
	}
}

func TestFixedBufferLimit(t *testing.T) {
	for _, extra := range []int{0, 2} {
		t.Run(strconv.Itoa(extra), func(t *testing.T) {
			service := &testService{synthesis: func(stream grpc.BidiStreamingServer[pb.StreamingSynthesisRequest, pb.SynthesisResponseStream]) error {
				if err := stream.SendHeader(metadata.Pairs("x-rime-audio-content-type", "audio/pcm", "x-request-id", "test-request")); err != nil {
					return err
				}
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
				}
				chunk := make([]byte, 1024*1024)
				for i := 0; i < 64; i++ {
					if err := stream.Send(audioResponse(chunk)); err != nil {
						return err
					}
				}
				if extra > 0 {
					if err := stream.Send(audioResponse(make([]byte, extra))); err != nil {
						return err
					}
					<-stream.Context().Done()
					return stream.Context().Err()
				}
				return nil
			}}
			client := setupClient(t, service)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			data, err := client.TTS.SynthesizeToBuffer(ctx, "Hello.", SynthesisOptions{})
			if extra > 0 {
				var sdkError *Error
				if data != nil || !errors.Is(err, ErrResourceLimit) || !errors.As(err, &sdkError) || sdkError.RequestID != "test-request" {
					t.Fatalf("expected buffer-limit error with no audio, got %d bytes and %v", len(data), err)
				}
			} else if err != nil || len(data) != 64*1024*1024 {
				t.Fatalf("expected exact limit to succeed, got %d bytes and %v", len(data), err)
			}
		})
	}
}
