"""Final word timings, independent of audio synthesis success."""

from dataclasses import dataclass

from .._errors import RimeStreamError


@dataclass(frozen=True)
class WordTimestamp:
    """A spoken, normalized word with start/end seconds from synthesis start."""

    text: str
    start: float
    end: float


@dataclass(frozen=True)
class TimestampStatus:
    """Alignment's google.rpc.Code (0 means OK) and service explanation."""

    code: int
    message: str


@dataclass(frozen=True)
class TimestampResult:
    """Final alignment status and words. Non-OK statuses carry no spans."""

    status: TimestampStatus
    spans: tuple[WordTimestamp, ...]


class TimestampTrailer:
    def __init__(self):
        self.seen = False
        self.invalid = False
        self.value = None

    def accept(self, trailer):
        if self.seen:
            self.invalid = True
        else:
            self.seen = True
            if trailer.HasField("timestamps"):
                self.value = trailer.timestamps

    def result(self, request_id) -> TimestampResult:
        def malformed():
            return RimeStreamError("Invalid timestamp trailer", request_id=request_id)

        if self.invalid:
            raise malformed()
        value = self.value
        if value is None:
            raise RimeStreamError(
                "The service did not return requested timestamps", request_id=request_id
            )
        if not value.HasField("status") or (value.status.code != 0 and value.spans):
            raise malformed()

        def seconds(span, field):
            if not span.HasField(field):
                raise malformed()
            duration = getattr(span, field)
            if not 0 <= duration.seconds <= 315576000000 or not 0 <= duration.nanos < 1000000000:
                raise malformed()
            return duration.seconds + duration.nanos / 1e9

        spans = []
        for span in value.spans:
            start, end = seconds(span, "start"), seconds(span, "end")
            if not span.text.strip() or end < start:
                raise malformed()
            spans.append(WordTimestamp(span.text, start, end))
        return TimestampResult(
            TimestampStatus(value.status.code, value.status.message), tuple(spans)
        )
