"""Controllable native gRPC service, shared by SDK and adapter acceptance tests."""

import asyncio

import grpc
from rime_api import text_to_speech_pb2 as proto


class FakeService:
    def __init__(self):
        self.mode = "normal"
        self.rejection_status = grpc.StatusCode.UNAVAILABLE
        self.response_metadata = (
            ("x-rime-audio-content-type", "audio/pcm"),
            ("x-request-id", "test-request"),
        )
        self.trailing_metadata = ()
        self.headers_sent = asyncio.Event()
        self.calls = []
        self.metadata = []
        self.active = 0
        self.release = asyncio.Event()
        self.received = asyncio.Event()
        self.discovery_calls = 0
        self.discovery_failures = 0
        self.payload = b"\x01\x00" * 2400

    async def streaming(self, requests, context):
        messages = []
        self.calls.append(messages)
        self.metadata.append(dict(context.invocation_metadata()))
        self.active += 1
        try:
            first = await anext(requests)
            messages.append(first)
            assert first.WhichOneof("payload") == "header"
            if self.mode == "empty_no_headers":
                return
            if self.mode == "error_before_audio":
                context.set_trailing_metadata((("x-request-id", "rejected-request"),))
                await context.abort(self.rejection_status, "test admission failure")
            metadata = dict(self.response_metadata)
            if self.mode == "wrong_format":
                metadata["x-rime-audio-content-type"] = "audio/wav"
            await context.send_initial_metadata(tuple(metadata.items()))
            context.set_trailing_metadata(self.trailing_metadata)
            self.headers_sent.set()
            if self.mode == "no_audio_error":
                await self.release.wait()
                await context.abort(self.rejection_status, "test rejection after headers")
            async for message in requests:
                messages.append(message)
                self.received.set()
                assert message.WhichOneof("payload") == "text_chunk"
                if self.mode == "empty_audio":
                    continue
                if self.mode == "silence":
                    await self.release.wait()
                if self.mode == "odd_chunks":
                    yield proto.SynthesisResponseStream(audio=self.payload[:1])
                    yield proto.SynthesisResponseStream(audio=self.payload[1:])
                else:
                    for _ in range(2000 if self.mode == "burst" else 1):
                        yield proto.SynthesisResponseStream(audio=self.payload)
                if self.mode == "partial_error":
                    await self.release.wait()
                    await context.abort(grpc.StatusCode.UNAVAILABLE, "test disconnect")
            if self.mode == "hang_after_input":
                await self.release.wait()
        finally:
            self.active -= 1

    async def synthesize(self, message, context):
        self.calls.append([message])
        await context.send_initial_metadata((("x-rime-audio-content-type", "audio/pcm"),))
        yield proto.SynthesisResponseStream(audio=self.payload)

    async def languages(self, message, context):
        self.discovery_calls += 1
        if self.mode == "discovery_timeout":
            await self.discovery_timeout(context)
        if self.mode == "discovery_error":
            await self.discovery_error(context)
        if self.discovery_calls <= self.discovery_failures:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "test retryable discovery")
        return proto.GetSupportedLanguagesResponse(languages=["en", "de"])

    async def voices(self, message, context):
        if self.mode == "discovery_timeout":
            await self.discovery_timeout(context)
        if self.mode == "discovery_error":
            await self.discovery_error(context)
        return proto.GetSupportedSpeakersResponse(speakers=["test-speaker"])

    async def discovery_timeout(self, context):
        await context.send_initial_metadata(self.response_metadata)
        self.headers_sent.set()
        await self.release.wait()

    async def discovery_error(self, context):
        await context.send_initial_metadata(self.response_metadata)
        context.set_trailing_metadata(self.trailing_metadata)
        await context.abort(self.rejection_status, "test discovery rejection")

    async def __aenter__(self):
        self.server = grpc.aio.server()
        from google.protobuf.message_factory import GetMessageClass

        handlers = {}
        methods = proto.DESCRIPTOR.services_by_name["TextToSpeech"].methods_by_name
        for name, implementation in {
            "SynthesizeStreaming": self.streaming,
            "Synthesize": self.synthesize,
            "GetSupportedLanguages": self.languages,
            "GetSupportedSpeakers": self.voices,
        }.items():
            descriptor = methods[name]
            factory = (
                grpc.stream_stream_rpc_method_handler
                if descriptor.client_streaming
                else (
                    grpc.unary_stream_rpc_method_handler
                    if descriptor.server_streaming
                    else grpc.unary_unary_rpc_method_handler
                )
            )
            handlers[name] = factory(
                implementation,
                request_deserializer=GetMessageClass(descriptor.input_type).FromString,
                response_serializer=GetMessageClass(descriptor.output_type).SerializeToString,
            )
        self.server.add_generic_rpc_handlers(
            (grpc.method_handlers_generic_handler("rime.TextToSpeech", handlers),)
        )
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.target = f"127.0.0.1:{port}"
        await self.server.start()
        return self

    async def __aexit__(self, *_):
        self.release.set()
        await self.server.stop(0)
