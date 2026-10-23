"""Opt-in checks for request IDs on a deployed Prism endpoint."""

import asyncio
import json
import os
import uuid

import pytest

from rimelabs_sdk import Rime

pytestmark = pytest.mark.skipif(
    os.environ.get("RIME_PRISM_LIVE") != "1", reason="set RIME_PRISM_LIVE=1 for live checks"
)


@pytest.mark.parametrize("attempt", range(4))
async def test_gateway_request_id(attempt):
    supplied = f"sdk-check-{uuid.uuid4()}"
    async with (
        asyncio.timeout(30),
        Rime() as client,
        client.realtime.connect(
            endpoint=os.environ.get("PRISM_URL"), request_id=supplied
        ) as session,
    ):
        used = session.info.request_id
        print(json.dumps({"attempt": attempt, "supplied": supplied, "used": used}))
        assert used, "Prism did not publish its request ID"
        if os.environ.get("RIME_PRISM_REQUIRE_REQUEST_ID") == "1":
            assert used == supplied, "The server or gateway replaced the supplied request ID"
