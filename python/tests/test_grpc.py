"""Request identity from gRPC metadata across supported runtime versions."""

import grpc
import pytest

from rimelabs_sdk._grpc import request_id


@pytest.mark.parametrize("container", [dict, tuple, grpc.aio.Metadata.from_tuple])
@pytest.mark.parametrize(
    "headers,trailers,previous,expected",
    [
        ((("x-request-id", "header"),), (("x-request-id", "trailer"),), "previous", "header"),
        ((), (("x-request-id", "trailer"),), "previous", "previous"),
        ((), (("x-request-id", "trailer"),), None, "trailer"),
        ((("other", "value"),), (), None, None),
        ((("x-request-id", ""),), (("x-request-id", "trailer"),), None, "trailer"),
        ((("x-request-id", "first"), ("x-request-id", "last")), (), None, "last"),
    ],
)
def test_request_id_with_legacy_metadata_keys(
    monkeypatch, container, headers, trailers, previous, expected
):
    # gRPC 1.78 exposes pairs through keys(), although __getitem__ takes a string.
    monkeypatch.setattr(grpc.aio.Metadata, "keys", lambda self: iter(self), raising=False)
    assert request_id(container(headers), container(trailers), previous) == expected


def test_request_id_without_metadata():
    assert request_id(None, None) is None
    assert request_id(None, None, "previous") == "previous"
