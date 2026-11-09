"""Stable failure types and optional connection diagnostics."""


class RimeError(Exception):
    """SDK failure with connection diagnostics when available.

    status_code is the refused HTTP upgrade status, such as 503. close_code is
    the received WebSocket close code, such as 1013. retry_after_s is the server's
    requested delay in seconds, including zero. None means unavailable. These
    fields do not cause retries or make replay of an accepted request safe.
    server_kind is the service's structured error kind, such as insufficient_credits.
    It is None when the service does not provide a structured reason.
    """

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        status_code: int | None = None,
        close_code: int | None = None,
        retry_after_s: float | None = None,
        server_kind: str | None = None,
    ):
        super().__init__(message)
        self.request_id = request_id
        self.status_code = status_code
        self.close_code = close_code
        self.retry_after_s = retry_after_s
        self.server_kind = server_kind


class RimeAuthenticationError(RimeError):
    pass


class RimeBillingError(RimeError):
    """The account has insufficient prepaid credits."""


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
