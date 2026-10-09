"""The local agent uses real SDK RPCs with controlled speech and LLM peers."""

import asyncio
import importlib
import json
import wave
from pathlib import Path

import grpc
import httpx
import pytest
import test_stt
from rime_api import text_to_speech_pb2 as proto

from rimelabs_sdk import Rime

stt_setup = test_stt.stt_setup


@pytest.fixture
def agent(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[2]))
    module = importlib.import_module("examples.python.agent.voice")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    return module


def completed(text="Hello world."):
    return {
        "status": "completed",
        "output": [
            {"type": "reasoning"},
            {"type": "message", "content": [{"type": "output_text", "text": text}]},
        ],
    }


@pytest.fixture
def llm():
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=completed(), headers={"x-request-id": "llm-request"})

    return requests, httpx.MockTransport(handle)


def options(agent, tmp_path, *args):
    settings = agent.arguments(["--no-playback", *args])
    settings.output_dir = str(tmp_path)
    return settings


async def test_two_cascaded_turns_use_final_stt_and_keep_history(
    agent, llm, setup, stt_setup, monkeypatch, tmp_path
):
    synthesis, _ = setup
    recognition, client = stt_setup
    closed = []

    async def capture(*_):
        try:
            yield b"\0\0" * 640
        finally:
            closed.append(True)

    monkeypatch.setattr(agent, "capture", capture)
    entries = tmp_path / "lexicon.json"
    # Use JSON serialization so the stress mark is preserved exactly.
    entries.write_text(json.dumps([{"spelling": "hello", "pronunciation": 'h @ . " l oU'}]))
    settings = options(agent, tmp_path, "--lexicon", str(entries))
    history = []
    requests, transport = llm
    async with httpx.AsyncClient(transport=transport) as http:
        for number in (1, 2):
            assert await agent.turn(client, http, settings, history, number)
    assert len(closed) == 2 and recognition.input_finished.is_set()
    assert requests[0]["input"] == [{"role": "user", "content": "Ice cream"}]
    assert requests[1]["input"] == history[:3]
    assert requests[0]["store"] is False
    for call in synthesis.calls:
        assert "".join(message.text_chunk for message in call[1:]) == "Hello world."
        assert call[0].header.custom_lexicon[0].pronunciation == 'h @ . " l oU'
    report = json.loads((tmp_path / "turn-002.json").read_text())
    assert report["status"] == "ok" and report["llm_request_id"] == "llm-request"
    assert report["stt_request_id"] and report["tts_request_id"]
    with wave.open(report["audio_file"]) as audio:
        assert (audio.getframerate(), audio.getnchannels(), audio.getsampwidth()) == (24000, 1, 2)
        assert audio.readframes(audio.getnframes()) == synthesis.payload


async def test_pronunciation_error_is_saved_and_next_turn_recovers(
    agent, setup, llm, tmp_path, monkeypatch
):
    service, client = setup
    monkeypatch.delenv("OPENAI_API_KEY")
    service.mode = "error_before_audio"
    service.rejection_status = grpc.StatusCode.INVALID_ARGUMENT
    service.rejection_message = 'custom-lexicon entry "hello": no-primary-stress'
    lexicon = tmp_path / "lexicon.json"
    lexicon.write_text(json.dumps([{"spelling": "hello", "pronunciation": "bad"}]))
    settings = options(agent, tmp_path, "--lexicon", str(lexicon))
    async with httpx.AsyncClient(transport=llm[1]) as http:
        assert not await agent.turn(client, http, settings, [], 1, say="Hello.")
        report = json.loads((tmp_path / "turn-001.json").read_text())
        assert report["error"] == {
            "type": "RimeInputError",
            "message": service.rejection_message,
            "request_id": "rejected-request",
        }
        assert not list(tmp_path.glob("*.wav"))
        service.mode = "normal"
        lexicon.write_text("[]")
        assert await agent.turn(client, http, settings, [], 2, say="Hello.")
    assert not service.calls[1][0].header.custom_lexicon
    assert not llm[0]


@pytest.mark.parametrize("code", [0, 12])
async def test_timestamps_report_matches_saved_audio(agent, setup, llm, tmp_path, code):
    service, _ = setup
    service.final_responses = [
        proto.SynthesisResponseStream(
            trailer={
                "timestamps": {
                    "status": {"code": code, "message": "unavailable" if code else ""},
                    "spans": []
                    if code
                    else [{"text": "Hello", "start": {}, "end": {"seconds": 1}}],
                }
            }
        )
    ]
    settings = options(agent, tmp_path, "--model", "mistv3", "--timestamps")
    async with (
        Rime(api_key="test-key", model="mistv3", endpoint=service.target) as client,
        httpx.AsyncClient(transport=llm[1]) as http,
    ):
        assert await agent.turn(client, http, settings, [], 1, say="Hello.")
    report = json.loads((tmp_path / "turn-001.json").read_text())
    assert report["timestamps"]["status"]["code"] == code
    assert Path(report["audio_file"]).exists()


async def test_missing_timestamps_preserve_completed_wav(agent, setup, llm, tmp_path):
    service, _ = setup
    settings = options(agent, tmp_path, "--model", "mistv3", "--timestamps")
    async with (
        Rime(api_key="test-key", model="mistv3", endpoint=service.target) as client,
        httpx.AsyncClient(transport=llm[1]) as http,
    ):
        assert not await agent.turn(client, http, settings, [], 1, say="Hello.")
    report = json.loads((tmp_path / "turn-001.json").read_text())
    assert report["stage"] == "timestamps" and Path(report["audio_file"]).exists()


@pytest.mark.parametrize("body", [{"status": "incomplete"}, {"status": "completed", "output": []}])
async def test_bad_llm_response_never_speaks(agent, setup, tmp_path, body):
    service, client = setup
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=body))
    async with httpx.AsyncClient(transport=transport) as http:
        assert not await agent.turn(client, http, options(agent, tmp_path), [], 1, ask="Hi")
    assert not service.calls


async def test_provider_errors_redact_key(agent):
    transport = httpx.MockTransport(
        lambda _: httpx.Response(401, json={"error": {"message": "Invalid test-openai-key"}})
    )
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(RuntimeError, match=r"\[redacted\]") as error:
            await agent.respond(http, "gpt-4.1-mini", agent.INSTRUCTIONS, [], "Hi")
    assert "test-openai-key" not in str(error.value)


async def test_cancel_during_tts_cleans_partial_file(agent, setup, llm, tmp_path):
    service, client = setup
    service.mode = "silence"
    async with httpx.AsyncClient(transport=llm[1]) as http:
        task = asyncio.create_task(
            agent.turn(client, http, options(agent, tmp_path), [], 1, say="Hello.")
        )
        await asyncio.wait_for(service.received.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not list(tmp_path.glob("*.wav"))
    assert json.loads((tmp_path / "turn-001.json").read_text())["status"] == "cancelled"
