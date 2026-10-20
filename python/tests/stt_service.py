"""Controlled gRPC recognition peer for public SDK tests."""

import asyncio

import grpc
from google.protobuf.message_factory import GetMessageClass
from rime_api import speech_to_text_pb2 as proto


class RecognitionService:
    def __init__(self):
        self.mode = "normal"
        self.rejection = grpc.StatusCode.UNAVAILABLE
        self.calls = []
        self.unary_calls = []
        self.metadata = []
        self.config_seen = asyncio.Event()
        self.input_finished = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.completed = asyncio.Event()
        self.active = 0

    async def transcribe(self, request, context):
        self.unary_calls.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        self.config_seen.set()
        if self.mode == "reject":
            context.set_trailing_metadata((("x-request-id", "stt-rejected"),))
            await context.abort(self.rejection, "deliberate rejection")
        await context.send_initial_metadata((("x-request-id", "stt-request"),))
        if self.mode == "no_completion":
            try:
                await self.release.wait()
            finally:
                self.cancelled.set()
        return proto.TranscriptionResponse(
            text="" if self.mode == "silence" else "Ice cream",
            language=None
            if self.mode == "missing_language"
            else proto.ResolvedLanguage(tag="en", source=proto.LANGUAGE_SOURCE_SELECTED),
        )

    async def streaming(self, requests, context):
        handler = asyncio.current_task()

        def finished(context):
            # Peer cancellation can cancel the handler without changing the context status.
            if context.cancelled() or handler.cancelling():
                self.cancelled.set()
            self.completed.set()

        context.add_done_callback(finished)
        messages = []
        self.calls.append(messages)
        self.metadata.append(dict(context.invocation_metadata()))
        self.active += 1
        try:
            config = await anext(requests)
            messages.append(config)
            assert config.WhichOneof("payload") == "config"
            self.config_seen.set()
            if self.mode == "reject":
                context.set_trailing_metadata((("x-request-id", "stt-rejected"),))
                await context.abort(self.rejection, "deliberate rejection")
            await context.send_initial_metadata((("x-request-id", "stt-request"),))
            if self.mode == "no_acceptance":
                await self.release.wait()
                return
            language = proto.ResolvedLanguage(tag="en", source=proto.LANGUAGE_SOURCE_SELECTED)
            yield proto.StreamingTranscriptionResponse(
                accepted=proto.StreamingAccepted(
                    output_contract=config.config.output_contract,
                    language=language,
                )
            )
            if self.mode == "early_final":
                yield proto.StreamingTranscriptionResponse(
                    done=proto.TranscriptionDone(language=language)
                )
                return
            revision, text = 0, ""
            async for message in requests:
                assert message.WhichOneof("payload") == "audio"
                assert len(message.audio) % 2 == 0 and len(message.audio) <= 65536
                messages.append(message)
                if self.mode == "silence":
                    continue
                for index in range(100 if self.mode == "burst" else 2):
                    if (
                        config.config.output_contract
                        == proto.STREAMING_OUTPUT_CONTRACT_APPEND_FRAGMENTS
                    ):
                        fragment = ("\n" if text else "") + (
                            "é" * 200000 if self.mode == "large" else f"fragment {index}"
                        )
                        text += fragment
                        yield proto.StreamingTranscriptionResponse(
                            delta=proto.TranscriptionDelta(text=fragment)
                        )
                        continue
                    revision += 1
                    text = "I scream" if index == 0 else "Ice cream"
                    yield proto.StreamingTranscriptionResponse(
                        hypothesis=proto.TranscriptionHypothesis(
                            text=text,
                            revision=revision,
                        )
                    )
                if self.mode == "partial_error":
                    await self.release.wait()
                    await context.abort(self.rejection, "deliberate error after partials")
            self.input_finished.set()
            if self.mode == "no_completion":
                await self.release.wait()
                return
            if self.mode == "missing_final":
                return
            yield proto.StreamingTranscriptionResponse(
                done=proto.TranscriptionDone(
                    text=text,
                    revision=revision,
                    language=language,
                )
            )
            if self.mode == "done_then_error":
                await context.abort(self.rejection, "deliberate status failure after done")
            if self.mode == "duplicate_final":
                yield proto.StreamingTranscriptionResponse(
                    done=proto.TranscriptionDone(
                        text=text,
                        revision=revision,
                        language=language,
                    )
                )
        finally:
            self.active -= 1

    async def __aenter__(self):
        service = proto.DESCRIPTOR.services_by_name["SpeechToText"]
        method = service.methods_by_name["TranscribeStreaming"]
        unary = service.methods_by_name["Transcribe"]
        self.server = grpc.aio.server(
            options=[("grpc.max_receive_message_length", 17 * 1024 * 1024)]
        )
        self.server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    service.full_name,
                    {
                        unary.name: grpc.unary_unary_rpc_method_handler(
                            self.transcribe,
                            request_deserializer=GetMessageClass(unary.input_type).FromString,
                            response_serializer=GetMessageClass(
                                unary.output_type
                            ).SerializeToString,
                        ),
                        method.name: grpc.stream_stream_rpc_method_handler(
                            self.streaming,
                            request_deserializer=GetMessageClass(method.input_type).FromString,
                            response_serializer=GetMessageClass(
                                method.output_type
                            ).SerializeToString,
                        ),
                    },
                ),
            )
        )
        self.target = f"127.0.0.1:{self.server.add_insecure_port('127.0.0.1:0')}"
        await self.server.start()
        return self

    async def __aexit__(self, *_):
        self.release.set()
        await self.server.stop(0)
