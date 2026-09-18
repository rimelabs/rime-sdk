"""One detector, bounded incremental buffering, original text preservation."""

from livekit import blingfire

from ._errors import RimeResourceLimitError

# Punctuation from the pinned BlingFire sentence rules schedules scans only.
# BlingFire, not this set, decides whether a sentence ends here.
_SCAN_TRIGGERS = frozenset(
    "\r\n.!?\u01c3\u06d4\u061f\u2024\u2026\u203c\u203d\u2048\u2049"
    "\u2404\ufe52\uff0e\uff61\u3002\uff1f\uff01\u2028\u2029\u00bf\u00a1"
)
_LOOKAHEAD = 16
_SCAN_INTERVAL = 1024
_CONTEXT = 128


def sentence_ends(text):
    # Native offsets index the original Python string, including characters
    # removed from the normalized output, such as a leading BOM or U+200B.
    _, offsets = blingfire.text_to_sentences_with_offsets(text)
    return [end for _, end in offsets]


class SentenceBuffer:
    def __init__(self, limit):
        self.pending = ""
        self.limit = limit
        self._committed = 0
        self._parts: list[str] = []
        self._until_scan = _SCAN_INTERVAL
        self._since_punctuation = _LOOKAHEAD

    def feed(self, fragment, *, final=False):
        # Advance by Unicode code points, never by caller chunk boundaries.
        # Join at scan points to avoid repeatedly copying a long sentence.
        for char in fragment:
            self._parts.append(char)
            self._until_scan -= 1
            self._since_punctuation = min(self._since_punctuation + 1, _LOOKAHEAD)
            if char in _SCAN_TRIGGERS:
                self._since_punctuation = 0
                self._until_scan = min(self._until_scan, _LOOKAHEAD)
            if self._until_scan == 0:
                yield from self._scan(final=False)
        if final:
            yield from self._scan(final=True)

    def _scan(self, *, final):
        self.pending += "".join(self._parts)
        self._parts.clear()
        if not self.pending:
            return
        ends = sentence_ends(self.pending)
        if final and ends:
            ends[-1] = len(self.pending)
        # Candidate sentences retained for lookahead have separate byte limits.
        # Also check text after the last offset, including whitespace-only input.
        start = self._committed
        for end in [*ends, len(self.pending)]:
            if end <= start:
                continue
            if len(self.pending[start:end].encode()) > self.limit:
                raise RimeResourceLimitError("Sentence exceeds the supported byte limit")
            start = end
        committed_ends = (
            ends
            if final
            else [end for end in ends[:-1] if len(self.pending[end:].strip()) >= _LOOKAHEAD]
        )
        for end in committed_ends:
            if end <= self._committed:
                continue
            sentence = self.pending[self._committed : end]
            self._committed = end
            if sentence.strip():
                yield sentence
        if final:
            residual = self.pending[self._committed :]
            if residual.strip():
                yield residual
            self.pending = ""
            self._committed = 0
            self._until_scan = _SCAN_INTERVAL
            self._since_punctuation = _LOOKAHEAD
            return
        # Check again soon if a detected boundary still needs following text,
        # or a punctuation mark arrived shortly before this scan.
        self._until_scan = _LOOKAHEAD if self._since_punctuation < _LOOKAHEAD else _SCAN_INTERVAL
        for end in ends:
            if self._committed < end < len(self.pending):
                # Trailing spaces will become interior context when another
                # character arrives. Do not wait a full interval for it.
                needed = max(1, _LOOKAHEAD - len(self.pending[end:].lstrip()))
                self._until_scan = min(self._until_scan, needed)
                break
        # Keep preceding text for the detector, but never submit it twice.
        drop = max(0, self._committed - _CONTEXT)
        self.pending = self.pending[drop:]
        self._committed -= drop
