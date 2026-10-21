"""Validate the selected transcript contract independently of consumer delivery."""

from .._errors import RimeResourceLimitError, RimeStreamError
from ._types import (
    TranscriptionDelta,
    TranscriptionFinal,
    TranscriptionOutputContract,
    TranscriptionPartial,
)


class TranscriptState:
    def __init__(
        self, proto, text_limit: int, output_contract=TranscriptionOutputContract.REVISED_HYPOTHESES
    ):
        self._proto = proto
        self._output_contract = output_contract
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
            if accepted.output_contract != getattr(
                self._proto, "STREAMING_OUTPUT_CONTRACT_" + self._output_contract.value.upper()
            ):
                raise RimeStreamError("The service accepted a different transcript contract")
            if not accepted.language.tag or not accepted.language.source:
                raise RimeStreamError("The service did not confirm a resolved language")
            self.language = accepted.language.tag
            return None
        if self.language is None:
            raise RimeStreamError("Received a transcript before acceptance")
        append = self._output_contract == TranscriptionOutputContract.APPEND_FRAGMENTS
        if kind == "delta" and append:
            text = message.delta.text
            if len(self._text.encode("utf-8")) + len(text.encode("utf-8")) > self._text_limit:
                raise RimeResourceLimitError("The transcript exceeds the SDK text limit")
            self._text += text
            return TranscriptionDelta(text=text)
        if kind != "done" and (kind != "hypothesis" or append):
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
            raise RimeStreamError("Final transcript does not match prior updates")
        if value.language.tag != self.language or not value.language.source:
            raise RimeStreamError("Final language does not match the accepted language")
        self.final = TranscriptionFinal(text=value.text, language=value.language.tag)
        return None

    def finish(self) -> TranscriptionFinal:
        if self.final is None:
            raise RimeStreamError("The service ended without a final transcript")
        return self.final
