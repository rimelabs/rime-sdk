"""Canonical schemas and private gRPC runtime bindings."""

import grpc
from google.protobuf.message_factory import GetMessageClass
from rime_api import text_to_speech_pb2 as proto

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
