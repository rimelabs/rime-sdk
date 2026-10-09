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
        self.metadata = []
        self.config_seen = asyncio.Event()
        self.input_finished = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.active = 0

    async def streaming(self, requests, context):
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
                    output_contract=proto.STREAMING_OUTPUT_CONTRACT_REVISED_HYPOTHESES,
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
            self.cancelled.set()

    async def __aenter__(self):
        service = proto.DESCRIPTOR.services_by_name["SpeechToText"]
        method = service.methods_by_name["TranscribeStreaming"]
        self.server = grpc.aio.server()
        self.server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    service.full_name,
                    {
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
