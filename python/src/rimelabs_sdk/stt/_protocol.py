"""Validate replacing hypotheses independently of consumer delivery."""

from .._errors import RimeResourceLimitError, RimeStreamError
from ._types import TranscriptionFinal, TranscriptionPartial


class TranscriptState:
    def __init__(self, proto, text_limit: int):
        self._proto = proto
        self._text_limit = text_limit
        self.language: str | None = None
        self._revision = 0
        self._text = ""
        self.final: TranscriptionFinal | None = None

    def accept(self, message, *, input_done: bool):
        kind = message.WhichOneof("payload")
        if kind is None:
            return None
        if self.final is not None:
            raise RimeStreamError("Received a message after transcription completion")
        if kind == "accepted":
            accepted = message.accepted
            if self.language is not None:
                raise RimeStreamError("Received duplicate transcription acceptance")
            if accepted.output_contract != self._proto.STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES:
                raise RimeStreamError("The service accepted a different transcript contract")
            if not accepted.language.tag or not accepted.language.source:
                raise RimeStreamError("The service did not confirm a resolved language")
            self.language = accepted.language.tag
            return None
        if self.language is None:
            raise RimeStreamError("Received a transcript before acceptance")
        if kind not in ("hypothesis", "done"):
            raise RimeStreamError("Received an unexpected transcription response")
        value = getattr(message, kind)
        if len(value.text.encode("utf-8")) > self._text_limit:
            raise RimeResourceLimitError("The transcript exceeds the SDK text limit")
        if kind == "hypothesis":
            if value.revision <= self._revision or (self._revision == 0 and value.revision != 1):
                raise RimeStreamError("Transcript revisions are not increasing from one")
            self._revision, self._text = value.revision, value.text
            return TranscriptionPartial(text=value.text)
        if not input_done:
            raise RimeStreamError("The service completed before input finished")
        if value.revision != self._revision or value.text != self._text:
            raise RimeStreamError("Final transcript does not match the last hypothesis")
        if value.language.tag != self.language or not value.language.source:
            raise RimeStreamError("Final language does not match the accepted language")
        self.final = TranscriptionFinal(text=value.text, language=value.language.tag)
        return None

    def finish(self) -> TranscriptionFinal:
        if self.final is None:
            raise RimeStreamError("The service ended without a final transcript")
        return self.final
