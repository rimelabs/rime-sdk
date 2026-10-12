"""TTS wire messages, metadata, and native gRPC completion."""

import asyncio

import grpc
from google.protobuf.message_factory import GetMessageClass
from rime_api import text_to_speech_pb2 as proto

from .._errors import RimeAudioFormatError, RimeResourceLimitError, RimeStreamError
from .._grpc import request_id as _request_id
from .._grpc import rpc_error
from ._timestamps import TimestampTrailer

_MAX_SEND_BYTES = 131072


def make_channel(policy):
    return grpc.aio.secure_channel(
        policy.target,
        grpc.ssl_channel_credentials(),
        options=[
            ("grpc.enable_retries", 0),
            ("grpc.max_receive_message_length", policy.receive_bytes),
            ("grpc.max_send_message_length", _MAX_SEND_BYTES),
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


def header(
    voice,
    language,
    timestamps=False,
    custom_lexicon=(),
    audio_format="audio/pcm",
    sample_rate=24000,
    time_scale_factor=None,
):
    request = proto.SynthesisRequest(
        speaker=voice, language=language, split_strategy=proto.SPLIT_STRATEGY_ACCUMULATE_SENTENCE
    )
    request.audio_parameters.audio_format = audio_format
    request.audio_parameters.sampling_rate = sample_rate
    if time_scale_factor is not None:
        request.audio_parameters.time_scale_factor = time_scale_factor
    if timestamps:
        request.timestamps.enable = True
    request.custom_lexicon.extend(
        proto.PronunciationEntry(spelling=entry.spelling, pronunciation=entry.pronunciation)
        for entry in custom_lexicon
    )
    return proto.StreamingSynthesisRequest(header=request)


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
            error.code(),
            _request_id(error.initial_metadata(), error.trailing_metadata()),
            error.details(),
        ) from None
    return list(response.speakers if kind == "voices" else response.languages)


class SynthesisCall:
    """One TTS RPC. Audio exhaustion means successful wire completion only."""

    def __init__(self, channel, metadata):
        self._call = bind(channel, "SynthesizeStreaming")(metadata=metadata)
        self._write_task: asyncio.Task[None] | None = None
        self._call.add_done_callback(self._cancel_pending_write)
        self.request_id = None
        self._timestamps_requested = False
        self._timestamps = TimestampTrailer()
        self._audio_format = "audio/pcm"

    def done(self):
        return self._call.done()

    def cancel(self):
        self._call.cancel()

    def _cancel_pending_write(self, _call):
        # grpc.aio can overwrite a server rejection with INTERNAL when an
        # in-flight send resumes after the RPC ends. Cancel it synchronously
        # in the completion callback, before it can discard the server status.
        if self._write_task is not None:
            self._write_task.cancel()

    def _error(self, error):
        self.request_id = _request_id(
            error.initial_metadata(), error.trailing_metadata(), self.request_id
        )
        return rpc_error(error.code(), self.request_id, error.details())

    async def _check_status(self):
        code = await self._call.code()
        self.request_id = _request_id(
            await self._call.initial_metadata(),
            await self._call.trailing_metadata(),
            self.request_id,
        )
        if code != grpc.StatusCode.OK:
            raise rpc_error(code, self.request_id, await self._call.details())

    async def _write(self, message):
        # grpc.aio can report an oversized streaming write as INTERNAL instead
        # of RESOURCE_EXHAUSTED. Keep the same limit and classify it locally.
        if message.ByteSize() > _MAX_SEND_BYTES:
            raise RimeResourceLimitError(
                "Synthesis request exceeds the supported byte limit", request_id=self.request_id
            )
        self._write_task = asyncio.create_task(self._call.write(message))
        try:
            await self._write_task
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if (current is not None and current.cancelling()) or not self.done():
                raise
        except asyncio.InvalidStateError:
            # A write after server rejection must report the final status,
            # even when no reader has observed that status yet.
            if not self.done():
                raise
        else:
            return
        finally:
            self._write_task = None
        await self._check_status()
        raise RimeStreamError(
            "The service completed before input finished", request_id=self.request_id
        ) from None

    async def start(
        self,
        voice,
        language,
        timestamps=False,
        custom_lexicon=(),
        audio_format="audio/pcm",
        sample_rate=24000,
        time_scale_factor=None,
    ):
        # Do not wait for response metadata before allowing text writes.
        self._timestamps_requested = timestamps
        self._audio_format = audio_format
        await self._write(
            header(
                voice,
                language,
                timestamps,
                custom_lexicon,
                audio_format,
                sample_rate,
                time_scale_factor,
            )
        )

    def timestamp_result(self):
        return self._timestamps.result(self.request_id)

    async def write(self, text):
        await self._write(proto.StreamingSynthesisRequest(text_chunk=text))

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
                payload = message.WhichOneof("payload")
                if payload == "trailer" and self._timestamps_requested:
                    self._timestamps.accept(message.trailer)
                if payload != "audio":
                    continue
                if self._timestamps.seen:
                    self._timestamps.invalid = True
                if message.audio:
                    self._check_format(content_type)
                yield message.audio
            await self._check_status()
            # Empty rejections must keep their status, even without format metadata.
            self._check_format(content_type)
        except grpc.aio.AioRpcError as error:
            raise self._error(error) from None

    def _check_format(self, content_type):
        if content_type != self._audio_format:
            raise RimeAudioFormatError(
                f"Expected {self._audio_format} from the service", request_id=self.request_id
            )
