"""Stable failure types; transport details stay private."""


class RimeError(Exception):
    def __init__(self, message: str, *, request_id: str | None = None):
        super().__init__(message)
        self.request_id = request_id


class RimeAuthenticationError(RimeError):
    pass


class RimePermissionError(RimeError):
    pass


class RimeInputError(RimeError):
    pass


class RimeResourceLimitError(RimeError):
    pass


class RimeUnavailableError(RimeError):
    pass


class RimeTimeoutError(RimeError):
    pass


class RimeAudioFormatError(RimeError):
    pass


class RimeCancelledError(RimeError):
    pass


class RimeStreamError(RimeError):
    pass
