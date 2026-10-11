"""Shared gRPC status and request identity handling."""

import grpc

from ._errors import (
    RimeAuthenticationError,
    RimeCancelledError,
    RimeInputError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
    RimeUnavailableError,
)


def rpc_error(code, request_id=None, details=None):
    cls = {
        grpc.StatusCode.UNAUTHENTICATED: RimeAuthenticationError,
        grpc.StatusCode.PERMISSION_DENIED: RimePermissionError,
        grpc.StatusCode.INVALID_ARGUMENT: RimeInputError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: RimeResourceLimitError,
        grpc.StatusCode.UNAVAILABLE: RimeUnavailableError,
        grpc.StatusCode.DEADLINE_EXCEEDED: RimeTimeoutError,
        grpc.StatusCode.CANCELLED: RimeCancelledError,
    }.get(code, RimeStreamError)
    message = (
        details
        if isinstance(details, str) and details.strip()
        else "Rime operation failed: " + code.name
    )
    return cls(message, request_id=request_id)


def request_id(headers, trailers=(), previous=None):
    return (
        dict(headers or ()).get("x-request-id")
        or previous
        or dict(trailers or ()).get("x-request-id")
    )
