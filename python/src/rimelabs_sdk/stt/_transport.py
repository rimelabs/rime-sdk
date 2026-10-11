"""One bidirectional STT RPC using the canonical generated messages."""

import asyncio

import grpc
from google.protobuf.message_factory import GetMessageClass

from .._errors import RimeInputError, RimeStreamError
from .._grpc import request_id, rpc_error
from ._types import TranscriptionMode


def schema():
    try:
        from rime_api import speech_to_text_pb2
    except ImportError:
        raise RimeInputError("STT requires rime-api with SpeechToText definitions") from None
    return speech_to_text_pb2


def make_channel(policy):
    return grpc.aio.secure_channel(
        policy.target,
        grpc.ssl_channel_credentials(),
        options=[
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", policy.receive_bytes),
            ("grpc.max_send_message_length", 131072),
        ],
    )


class TranscriptionCall:
    def __init__(self, channel, metadata):
        self.proto = schema()
        service = self.proto.DESCRIPTOR.services_by_name["SpeechToText"]
        method = service.methods_by_name["TranscribeStreaming"]
        self._call = channel.stream_stream(
            f"/{service.full_name}/{method.name}",
            request_serializer=GetMessageClass(method.input_type).SerializeToString,
            response_deserializer=GetMessageClass(method.output_type).FromString,
        )(metadata=metadata)
        self.request_id = None

    def cancel(self):
        self._call.cancel()

    def _error(self, error):
        self.request_id = request_id(
            error.initial_metadata(), error.trailing_metadata(), self.request_id
        )
        return rpc_error(error.code(), self.request_id, error.details())

    async def _status(self):
        code = await self._call.code()
        self.request_id = request_id(
            await self._call.initial_metadata(),
            await self._call.trailing_metadata(),
            self.request_id,
        )
        if code != grpc.StatusCode.OK:
            raise rpc_error(code, self.request_id, await self._call.details())

    async def _write(self, message):
        try:
            await self._call.write(message)
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None
        except asyncio.InvalidStateError:
            if not self._call.done():
                raise
            await self._status()
            raise RimeStreamError("The service completed before input finished") from None

    async def start(self, language, mode, context_terms):
        proto = self.proto
        await self._write(
            proto.StreamingTranscriptionRequest(
                config=proto.StreamingConfig(
                    language=language,
                    mode=(
                        proto.TRANSCRIPTION_MODE_VERBATIM
                        if mode is TranscriptionMode.VERBATIM
                        else proto.TRANSCRIPTION_MODE_WRITTEN
                    ),
                    context_terms=context_terms,
                    output_contract=proto.STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES,
                )
            )
        )

    async def write(self, audio):
        await self._write(self.proto.StreamingTranscriptionRequest(audio=audio))

    async def finish_input(self):
        try:
            await self._call.done_writing()
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None

    async def responses(self):
        try:
            self.request_id = request_id(await self._call.initial_metadata())
            while True:
                message = await self._call.read()
                if message is grpc.aio.EOF:
                    break
                yield message
            await self._status()
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None
