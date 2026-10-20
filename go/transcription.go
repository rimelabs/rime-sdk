package rime

import (
	"context"
	"time"

	pb "github.com/rimelabs/rime-api/go"
)

// AudioSource supplies headerless signed PCM16 little-endian audio, with stereo
// channels interleaved. Return io.EOF to finish the utterance; bytes returned
// alongside io.EOF are sent first. Calls are serial, after service acceptance.
// The source must honor cancellation and owns its file or device. Returned bytes
// must remain unchanged until the next call.
type AudioSource func(context.Context) ([]byte, error)

// PCMFormat describes raw audio without a file header.
type PCMFormat struct {
	SampleRate int // Frames per second: 8000, 16000, 24000 or 48000. Zero means 16000.
	Channels   int // One (mono) or two (interleaved stereo). Zero means one.
}

// TranscriptionMode selects transcript presentation.
type TranscriptionMode string

const (
	TranscriptionWritten  TranscriptionMode = "written"  // Normalized written text.
	TranscriptionVerbatim TranscriptionMode = "verbatim" // Preserve spoken wording.
)

// TranscriptionOutputContract selects the meaning of streaming updates.
type TranscriptionOutputContract string

const (
	// TranscriptionRevisedHypotheses returns replacement partial transcripts.
	TranscriptionRevisedHypotheses TranscriptionOutputContract = "revised_hypotheses"
	// TranscriptionAppendFragments returns permanent deltas, including separators.
	TranscriptionAppendFragments TranscriptionOutputContract = "append_fragments"
)

func transcriptionContract(value TranscriptionOutputContract) (pb.StreamingOutputContract, error) {
	switch value {
	case "", TranscriptionRevisedHypotheses:
		return pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES, nil
	case TranscriptionAppendFragments:
		return pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_APPEND_FRAGMENTS, nil
	default:
		return 0, failure(ErrInput, "unknown transcription output contract")
	}
}

// TranscriptionOptions configures one utterance independently of TTS defaults.
type TranscriptionOptions struct {
	OutputContract TranscriptionOutputContract // Zero selects revised hypotheses.
	Language       string                      // Required language tag, such as "en" or "es"; passed unchanged.
	Mode           TranscriptionMode           // Zero means TranscriptionWritten.
	ContextTerms   []string                    // Optional recognition hints, copied without normalization.
	InputFormat    PCMFormat                   // Zero means mono PCM16 at 16000 frames per second.
	Timeout        *time.Duration              // Overall budget from first Recv; nil or zero disables it.
}

// TranscriptionUpdate is a TranscriptionPartial, TranscriptionDelta or TranscriptionFinal.
// Use a type switch: partials replace text, deltas append, finals confirm completion.
type TranscriptionUpdate interface{ isTranscriptionUpdate() }

// TranscriptionPartial replaces the previous provisional transcript.
type TranscriptionPartial struct {
	Text string // Complete provisional text.
}

func (TranscriptionPartial) isTranscriptionUpdate() {}

// TranscriptionDelta is permanent text to append exactly, including separators.
type TranscriptionDelta struct{ Text string }

func (TranscriptionDelta) isTranscriptionUpdate() {}

// TranscriptionFinal is emitted once, after validated completion and successful
// gRPC termination. No final result is emitted after an error or cancellation.
type TranscriptionFinal struct {
	Text     string // Complete confirmed text; empty for silence.
	Language string // Language confirmed by the service.
}

func (TranscriptionFinal) isTranscriptionUpdate() {}

// STTService opens independent streaming recognition operations.
type STTService struct{ client *Client }

type transcriptionLimits struct {
	connection, acceptance, completion, cleanup  time.Duration
	receiveBytes, transcriptBytes, queuedUpdates int
}

var defaultTranscriptionLimits = transcriptionLimits{
	connection: 10 * time.Second, acceptance: 10 * time.Second,
	completion: 120 * time.Second, cleanup: 2 * time.Second,
	receiveBytes: 1048576 + 65536, transcriptBytes: 1048576, queuedUpdates: 16,
}

// Stream validates options immediately. The first Recv starts network work and
// the overall timeout. Audio is not requested until the service accepts the
// configuration. Call Close if you stop reading before completion.
func (service *STTService) Stream(ctx context.Context, source AudioSource, options TranscriptionOptions) (*TranscriptionStream, error) {
	if source == nil {
		return nil, failure(ErrInput, "audio source must not be nil")
	}
	contract, err := transcriptionContract(options.OutputContract)
	if err != nil {
		return nil, err
	}
	mode, err := transcriptionMode(options.Mode)
	if err != nil {
		return nil, err
	}
	format, err := resolvePCMFormat(options.InputFormat)
	if err != nil {
		return nil, err
	}
	var budget time.Duration
	if options.Timeout != nil {
		budget = *options.Timeout
	}
	if budget < 0 {
		return nil, failure(ErrInput, "timeout must not be negative")
	}
	var disabled time.Duration
	operation, err := service.client.operation(ctx, &disabled, 0)
	if err != nil {
		return nil, err
	}
	stream := &TranscriptionStream{
		op: operation, source: source, format: format, timeout: budget,
		limits: service.client.sttLimits, queue: newTranscriptQueue(service.client.sttLimits.queuedUpdates),
		workersDone: make(chan struct{}), accepted: make(chan struct{}),
		config: &pb.StreamingConfig{Language: &options.Language, Mode: mode,
			ContextTerms:   append([]string(nil), options.ContextTerms...),
			OutputContract: contract},
	}
	stream.mu.Lock()
	stream.stopCancellation = context.AfterFunc(operation.ctx, func() { stream.fail(context.Cause(operation.ctx)) })
	stream.mu.Unlock()
	return stream, nil
}
