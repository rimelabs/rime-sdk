use super::{TranscriptionOutputContract, TranscriptionUpdate};
use crate::{Error, ErrorKind};
use rimelabs_api::{streaming_transcription_response::Payload, StreamingTranscriptionResponse};

#[derive(Default)]
pub(super) struct TranscriptState {
    pub language: Option<String>,
    output_contract: TranscriptionOutputContract,
    revision: u64,
    text: String,
    final_result: Option<TranscriptionUpdate>,
}
fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::Stream, message)
}
fn check_text(text: &str) -> Result<(), Error> {
    if text.len() > super::MAX_TRANSCRIPT_BYTES {
        return Err(Error::new(
            ErrorKind::ResourceLimit,
            "transcript exceeds the SDK text limit",
        ));
    }
    Ok(())
}
impl TranscriptState {
    pub fn new(output_contract: TranscriptionOutputContract) -> Self {
        Self {
            output_contract,
            ..Self::default()
        }
    }
    pub fn accept(
        &mut self,
        message: StreamingTranscriptionResponse,
        input_done: bool,
    ) -> Result<Option<TranscriptionUpdate>, Error> {
        let Some(payload) = message.payload else {
            return Ok(None);
        };
        if self.final_result.is_some() {
            return Err(invalid("message after transcription completion"));
        }
        match payload {
            Payload::Accepted(accepted) => {
                if self.language.is_some() {
                    return Err(invalid("duplicate transcription acceptance"));
                }
                if accepted.output_contract != self.output_contract.wire() {
                    return Err(invalid("service accepted a different transcript contract"));
                }
                let language = accepted
                    .language
                    .filter(|v| !v.tag.is_empty() && v.source != 0)
                    .ok_or_else(|| invalid("service did not confirm a resolved language"))?;
                self.language = Some(language.tag);
                Ok(None)
            }
            _ if self.language.is_none() => Err(invalid("transcript before acceptance")),
            Payload::Delta(value)
                if self.output_contract == TranscriptionOutputContract::AppendFragments =>
            {
                if self.text.len() + value.text.len() > super::MAX_TRANSCRIPT_BYTES {
                    return Err(Error::new(
                        ErrorKind::ResourceLimit,
                        "transcript exceeds the SDK text limit",
                    ));
                }
                self.text.push_str(&value.text);
                Ok(Some(TranscriptionUpdate::Delta { text: value.text }))
            }
            Payload::Hypothesis(value)
                if self.output_contract == TranscriptionOutputContract::RevisedHypotheses =>
            {
                check_text(&value.text)?;
                if value.revision <= self.revision || (self.revision == 0 && value.revision != 1) {
                    return Err(invalid("transcript revisions must increase from one"));
                }
                self.revision = value.revision;
                self.text = value.text.clone();
                Ok(Some(TranscriptionUpdate::Partial { text: value.text }))
            }
            Payload::Done(value) => {
                check_text(&value.text)?;
                if !input_done {
                    return Err(invalid("service completed before input finished"));
                }
                if value.revision != self.revision || value.text != self.text {
                    return Err(invalid("final transcript does not match prior updates"));
                }
                let language = value
                    .language
                    .filter(|v| Some(&v.tag) == self.language.as_ref() && v.source != 0)
                    .ok_or_else(|| invalid("final language does not match acceptance"))?;
                self.final_result = Some(TranscriptionUpdate::Final {
                    text: value.text,
                    language: language.tag,
                });
                Ok(None)
            }
            _ => Err(invalid("unexpected transcription response")),
        }
    }
    pub fn finish(self) -> Result<TranscriptionUpdate, Error> {
        self.final_result
            .ok_or_else(|| invalid("service ended without a final transcript"))
    }
}
