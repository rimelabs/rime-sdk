"""Shared gRPC status and request identity handling."""

from collections.abc import Mapping

import grpc

from ._errors import (
    RimeAuthenticationError,
    RimeBillingError,
    RimeCancelledError,
    RimeInputError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
    RimeUnavailableError,
)


def rpc_error(code, request_id=None, details=None, headers=(), trailers=()):
    server_kind = metadata_dict(headers).get("x-rime-error-kind") or metadata_dict(trailers).get(
        "x-rime-error-kind"
    )
    cls = {
        grpc.StatusCode.UNAUTHENTICATED: RimeAuthenticationError,
        grpc.StatusCode.PERMISSION_DENIED: RimePermissionError,
        grpc.StatusCode.INVALID_ARGUMENT: RimeInputError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: RimeResourceLimitError,
        grpc.StatusCode.UNAVAILABLE: RimeUnavailableError,
        grpc.StatusCode.DEADLINE_EXCEEDED: RimeTimeoutError,
        grpc.StatusCode.CANCELLED: RimeCancelledError,
    }.get(code, RimeStreamError)
    if code == grpc.StatusCode.PERMISSION_DENIED and server_kind == "insufficient_credits":
        cls = RimeBillingError
    message = (
        details
        if isinstance(details, str) and details.strip()
        else "Rime operation failed: " + code.name
    )
    return cls(message, request_id=request_id, server_kind=server_kind)


def metadata_dict(metadata):
    """Read mappings or metadata pairs without relying on gRPC's keys() view."""
    if isinstance(metadata, Mapping):
        return dict(metadata)
    return {key: value for key, value in metadata or ()}


def request_id(headers, trailers=(), previous=None):
    return (
        metadata_dict(headers).get("x-request-id")
        or previous
        or metadata_dict(trailers).get("x-request-id")
    )
