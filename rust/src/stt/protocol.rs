use super::TranscriptionUpdate;
use crate::{Error, ErrorKind};
use rimelabs_api::{
    streaming_transcription_response::Payload, StreamingOutputContract,
    StreamingTranscriptionResponse,
};

#[derive(Default)]
pub(super) struct TranscriptState {
    pub language: Option<String>,
    revision: u64,
    text: String,
    final_result: Option<TranscriptionUpdate>,
}
fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::Stream, message)
}
fn check_text(text: &str) -> Result<(), Error> {
    if text.len() > 65_536 {
        return Err(Error::new(
            ErrorKind::ResourceLimit,
            "transcript exceeds the SDK text limit",
        ));
    }
    Ok(())
}
impl TranscriptState {
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
                if accepted.output_contract != StreamingOutputContract::RevisedHypotheses as i32 {
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
            Payload::Hypothesis(value) => {
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
                    return Err(invalid(
                        "final transcript does not match the last hypothesis",
                    ));
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
