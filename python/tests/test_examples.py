"""Run the documented Python examples against local SDK test peers."""

import asyncio
import base64
import importlib.util
import json
from pathlib import Path

import pytest
from test_realtime import Peer

from rimelabs_sdk.realtime import _client

EXAMPLES = Path(__file__).parents[1] / "examples"


def load_example(path):
    spec = importlib.util.spec_from_file_location("sdk_example", EXAMPLES / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def prism_peer(monkeypatch, tmp_path):
    peer = Peer()
    peer.emit("prsm.typed_input.ready")

    async def connect(endpoint, **kwargs):
        assert endpoint == "ws://localhost/v1/realtime"
        assert kwargs["additional_headers"] == {"Authorization": "Bearer example-key"}
        return peer

    monkeypatch.setattr(_client, "connect", connect)
    monkeypatch.setenv("RIME_API_KEY", "example-key")
    monkeypatch.setenv("PRISM_URL", "ws://localhost/v1/realtime")
    monkeypatch.delenv("PRISM_VOICE", raising=False)
    monkeypatch.chdir(tmp_path)
    return peer


async def test_tts_example_saves_audio(setup, monkeypatch, tmp_path):
    service, _ = setup
    monkeypatch.setenv("RIME_API_KEY", "example-key")
    monkeypatch.chdir(tmp_path)
    await load_example("tts/stream.py").main()
    assert (tmp_path / "speech.pcm").read_bytes() == service.payload * 3


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_typed_example_saves_audio_and_checks_status(prism_peer, tmp_path, capsys, status):
    peer = prism_peer
    async with asyncio.timeout(2):
        task = asyncio.create_task(load_example("realtime/typed_turn.py").main())
        request = await peer.next("response.create")
        assert request["response"]["metadata"]["prsm_cause"] == "user_text"
        peer.accepted(request)
        peer.emit(
            "response.text.delta",
            response_id="reply-1",
            item_id="message",
            output_index=0,
            content_index=0,
            delta="Hello.",
        )
        audio = b"\x01\x00" * 20
        peer.emit(
            "response.audio.delta",
            response_id="reply-1",
            item_id="message",
            output_index=0,
            content_index=0,
            delta=base64.b64encode(audio).decode(),
        )
        peer.ended(status=status)
        if status == "completed":
            await task
            assert "Saved reply.pcm" in capsys.readouterr().out
        else:
            with pytest.raises(ExceptionGroup) as caught:
                await task
            assert any("Response failed" in str(error) for error in caught.value.exceptions)
            assert "Saved reply.pcm" not in capsys.readouterr().out
        assert (tmp_path / "reply.pcm").read_bytes() == audio
        assert peer.closed


@pytest.mark.parametrize("call_count", [0, 1, 2])
async def test_tool_example_waits_for_all_results_before_continuing(prism_peer, call_count):
    peer = prism_peer
    async with asyncio.timeout(2):
        task = asyncio.create_task(load_example("realtime/tools.py").main())
        request = await peer.next("response.create")
        assert peer.settings["tools"][0]["function"]["name"] == "lookup_order"
        peer.accepted(request)
        for index in range(call_count):
            # A duplicate call event must not execute the tool twice.
            for _ in range(2):
                peer.emit(
                    "response.function_call_arguments.done",
                    response_id="reply-1",
                    call_id=f"call-{index}",
                    item_id=f"tool-{index}",
                    name="lookup_order",
                    arguments=json.dumps({"order_id": "demo-123"}),
                )
        peer.ended()
        for index in range(call_count):
            result = await peer.next("conversation.item.create")
            assert result["item"]["call_id"] == f"call-{index}"
            assert json.loads(result["item"]["output"]) == {
                "order_id": "demo-123",
                "status": "shipped",
            }
            assert peer.sent.empty()
            peer.emit(
                "conversation.item.created",
                prsm_request_event_id=result["event_id"],
                item={"type": "function_call_output", "call_id": result["item"]["call_id"]},
            )
        if call_count:
            continuation = await peer.next("response.create")
            assert continuation["response"]["metadata"] == {
                "prsm_cause": "tool_continuation",
                "prsm_parent_response_id": "reply-1",
            }
            peer.accepted(continuation, "reply-2")
            peer.ended("reply-2")
        await task
        assert peer.sent.empty()
        assert peer.closed
