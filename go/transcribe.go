package rime

import (
	"context"
	"time"

	pb "github.com/rimelabs/rime-api/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const maxTranscriptionWAVBytes = 16 * 1024 * 1024

// TranscribeOptions configures a complete WAV recording independently of TTS defaults.
type TranscribeOptions struct {
	Language     string            // Required BCP-47 language tag, passed unchanged to the service.
	Mode         TranscriptionMode // Zero selects written formatting.
	ContextTerms []string          // Recognition hints; the service validates their contents.
	Timeout      *time.Duration    // Overall budget including upload; nil or zero disables it.
}

// Transcription is a successful whole-recording result. Text may be empty for silence.
type Transcription struct {
	Text      string
	Language  string
	RequestID string
}

// Transcribe sends a complete WAV file, including its header, without retries.
// PCM16 mono 16 kHz is the portable input format. The service validates the file;
// the SDK sends it unchanged, with a 16 MiB maximum. Audio must remain unchanged
// until this call returns. Context cancellation and Client.Close cancel the call.
func (service *STTService) Transcribe(ctx context.Context, audio []byte, options TranscribeOptions) (Transcription, error) {
	if len(audio) == 0 {
		return Transcription{}, failure(ErrAudioFormat, "audio must contain complete WAV bytes")
	}
	if len(audio) > maxTranscriptionWAVBytes {
		return Transcription{}, failure(ErrResourceLimit, "WAV input exceeds 16 MiB")
	}
	mode, err := transcriptionMode(options.Mode)
	if err != nil {
		return Transcription{}, err
	}
	var budget time.Duration
	if options.Timeout != nil {
		budget = *options.Timeout
	}
	op, err := service.client.operation(ctx, &budget, 0)
	if err != nil {
		return Transcription{}, err
	}
	defer op.close()
	request := &pb.TranscriptionRequest{Audio: audio, Options: &pb.TranscriptionOptions{
		Language: &options.Language, Mode: mode, ContextTerms: append([]string(nil), options.ContextTerms...),
	}}
	conn, rpcContext, err := service.client.prepareService(op.ctx, transcriptionService)
	if err != nil {
		return Transcription{}, err
	}
	rpcContext, cancel := context.WithTimeout(rpcContext, service.client.sttLimits.completion)
	defer cancel()
	var headers, trailers metadata.MD
	response, err := pb.NewSpeechToTextClient(conn).Transcribe(rpcContext, request,
		grpc.Header(&headers), grpc.Trailer(&trailers), grpc.MaxCallSendMsgSize(maxTranscriptionWAVBytes+65536))
	identity := requestID(headers, trailers)
	if cause := context.Cause(op.ctx); cause != nil {
		return Transcription{}, operationError(cause, identity)
	}
	if err != nil {
		return Transcription{}, operationError(err, identity)
	}
	if response.Language == nil || response.Language.Tag == "" || response.Language.Source == pb.LanguageSource_LANGUAGE_SOURCE_UNSPECIFIED {
		return Transcription{}, operationError(failure(ErrStream, "the service did not confirm a resolved language"), identity)
	}
	if len(response.Text) > service.client.sttLimits.transcriptBytes {
		return Transcription{}, operationError(failure(ErrResourceLimit, "the transcript exceeds the SDK text limit"), identity)
	}
	return Transcription{Text: response.Text, Language: response.Language.Tag, RequestID: identity}, nil
}

func transcriptionMode(mode TranscriptionMode) (pb.TranscriptionMode, error) {
	switch mode {
	case "", TranscriptionWritten:
		return pb.TranscriptionMode_TRANSCRIPTION_MODE_WRITTEN, nil
	case TranscriptionVerbatim:
		return pb.TranscriptionMode_TRANSCRIPTION_MODE_VERBATIM, nil
	default:
		return 0, failure(ErrInput, "mode must be written or verbatim")
	}
}
