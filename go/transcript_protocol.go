package rime

import pb "github.com/rimelabs/rime-api/go"

type transcriptState struct {
	language, text string
	revision       uint64
	final          *TranscriptionFinal
	textLimit      int
}

func (state *transcriptState) accept(message *pb.StreamingTranscriptionResponse, inputDone bool) (*TranscriptionPartial, error) {
	if message.GetPayload() == nil {
		return nil, nil
	}
	if state.final != nil {
		return nil, failure(ErrStream, "received a message after transcription completion")
	}
	if accepted := message.GetAccepted(); accepted != nil {
		if state.language != "" {
			return nil, failure(ErrStream, "received duplicate transcription acceptance")
		}
		if accepted.GetOutputContract() != pb.StreamingOutputContract_STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES {
			return nil, failure(ErrStream, "the service accepted a different transcript contract")
		}
		language := accepted.GetLanguage()
		if language.GetTag() == "" || language.GetSource() == 0 {
			return nil, failure(ErrStream, "the service did not confirm a resolved language")
		}
		state.language = language.GetTag()
		return nil, nil
	}
	if state.language == "" {
		return nil, failure(ErrStream, "received a transcript before acceptance")
	}
	var text string
	switch payload := message.GetPayload().(type) {
	case *pb.StreamingTranscriptionResponse_Hypothesis:
		text = payload.Hypothesis.GetText()
	case *pb.StreamingTranscriptionResponse_Done:
		text = payload.Done.GetText()
	default:
		return nil, failure(ErrStream, "received an unexpected transcription response")
	}
	if len(text) > state.textLimit {
		return nil, failure(ErrResourceLimit, "the transcript exceeds the SDK text limit")
	}
	if hypothesis := message.GetHypothesis(); hypothesis != nil {
		if hypothesis.Revision <= state.revision || (state.revision == 0 && hypothesis.Revision != 1) {
			return nil, failure(ErrStream, "transcript revisions are not increasing from one")
		}
		state.revision, state.text = hypothesis.Revision, text
		return &TranscriptionPartial{Text: text}, nil
	}
	if !inputDone {
		return nil, failure(ErrStream, "the service completed before input finished")
	}
	done := message.GetDone()
	if done.GetRevision() != state.revision || text != state.text {
		return nil, failure(ErrStream, "final transcript does not match the last hypothesis")
	}
	if done.GetLanguage().GetTag() != state.language || done.GetLanguage().GetSource() == 0 {
		return nil, failure(ErrStream, "final language does not match the accepted language")
	}
	state.final = &TranscriptionFinal{Text: text, Language: state.language}
	return nil, nil
}

func (state *transcriptState) finish() (TranscriptionFinal, error) {
	if state.final == nil {
		return TranscriptionFinal{}, failure(ErrStream, "the service ended without a final transcript")
	}
	return *state.final, nil
}
