"""TTS wire messages, metadata, and native gRPC completion."""

import asyncio

import grpc
from google.protobuf.message_factory import GetMessageClass
from rime_api import text_to_speech_pb2 as proto

from ._errors import (
    RimeAudioFormatError,
    RimeAuthenticationError,
    RimeCancelledError,
    RimeInputError,
    RimePermissionError,
    RimeResourceLimitError,
    RimeStreamError,
    RimeTimeoutError,
    RimeUnavailableError,
)


def make_channel(policy):
    return grpc.aio.secure_channel(
        policy.target,
        grpc.ssl_channel_credentials(),
        options=[
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", policy.receive_bytes),
            ("grpc.max_send_message_length", policy.sentence_bytes + 65536),
        ],
    )


def bind(channel, name):
    descriptor = proto.DESCRIPTOR.services_by_name["TextToSpeech"].methods_by_name[name]
    factory = channel.stream_stream if descriptor.client_streaming else channel.unary_unary
    return factory(
        "/rime.TextToSpeech/" + name,
        request_serializer=GetMessageClass(descriptor.input_type).SerializeToString,
        response_deserializer=GetMessageClass(descriptor.output_type).FromString,
    )


def header(voice, language):
    request = proto.SynthesisRequest(speaker=voice, language=language)
    request.audio_parameters.audio_format = "audio/pcm"
    request.audio_parameters.sampling_rate = 24000
    return proto.StreamingSynthesisRequest(header=request)


def rpc_error(code, request_id=None):
    cls = {
        grpc.StatusCode.UNAUTHENTICATED: RimeAuthenticationError,
        grpc.StatusCode.PERMISSION_DENIED: RimePermissionError,
        grpc.StatusCode.INVALID_ARGUMENT: RimeInputError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: RimeResourceLimitError,
        grpc.StatusCode.UNAVAILABLE: RimeUnavailableError,
        grpc.StatusCode.DEADLINE_EXCEEDED: RimeTimeoutError,
        grpc.StatusCode.CANCELLED: RimeCancelledError,
    }.get(code, RimeStreamError)
    return cls("Rime operation failed: " + code.name, request_id=request_id)


def _request_id(headers, trailers=(), previous=None):
    return (
        dict(headers or ()).get("x-request-id")
        or previous
        or dict(trailers or ()).get("x-request-id")
    )


async def discover(channel, metadata, kind, language, timeout):
    """Make one discovery attempt, retaining metadata on native deadline expiry."""
    if kind == "voices":
        name = "GetSupportedSpeakers"
        request = proto.GetSupportedSpeakersRequest()
        if language is not None:
            request.language = language
    else:
        name = "GetSupportedLanguages"
        request = proto.GetSupportedLanguagesRequest()
    # Let gRPC enforce the remaining deadline. Cancelling the await with an
    # asyncio timeout would discard headers batched with unary completion.
    try:
        response = await bind(channel, name)(request, metadata=metadata, timeout=timeout)
    except grpc.aio.AioRpcError as error:
        raise rpc_error(
            error.code(), _request_id(error.initial_metadata(), error.trailing_metadata())
        ) from None
    return list(response.speakers if kind == "voices" else response.languages)


class SynthesisCall:
    """One TTS RPC. Audio exhaustion means successful wire completion only."""

    def __init__(self, channel, metadata):
        self._call = bind(channel, "SynthesizeStreaming")(metadata=metadata)
        self.request_id = None

    def done(self):
        return self._call.done()

    def cancel(self):
        self._call.cancel()

    def _error(self, error):
        self.request_id = _request_id(
            error.initial_metadata(), error.trailing_metadata(), self.request_id
        )
        return rpc_error(error.code(), self.request_id)

    async def _check_status(self):
        code = await self._call.code()
        self.request_id = _request_id(
            await self._call.initial_metadata(),
            await self._call.trailing_metadata(),
            self.request_id,
        )
        if code != grpc.StatusCode.OK:
            raise rpc_error(code, self.request_id)

    async def _write(self, message):
        try:
            await self._call.write(message)
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None
        except asyncio.InvalidStateError:
            # A write after server rejection must report the final status,
            # even when no reader has observed that status yet.
            if not self.done():
                raise
            await self._check_status()
            raise RimeStreamError(
                "The service completed before input finished", request_id=self.request_id
            ) from None

    async def start(self, voice, language):
        # Do not wait for response metadata before allowing text writes.
        await self._write(header(voice, language))

    async def write(self, sentence):
        await self._write(proto.StreamingSynthesisRequest(text_chunk=sentence))

    async def finish_input(self):
        try:
            await self._call.done_writing()
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None

    async def audio(self):
        try:
            metadata = dict(await self._call.initial_metadata())
            self.request_id = _request_id(metadata) or self.request_id
            content_type = metadata.get("x-rime-audio-content-type")
            while True:
                message = await self._call.read()
                if message is grpc.aio.EOF:
                    break
                if message.audio:
                    self._check_format(content_type)
                yield message.audio
            await self._check_status()
            # Empty rejections must keep their status, even without format metadata.
            self._check_format(content_type)
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None

    def _check_format(self, content_type):
        if content_type != "audio/pcm":
            raise RimeAudioFormatError(
                "Expected raw audio/pcm from the service", request_id=self.request_id
            )
