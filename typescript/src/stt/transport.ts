import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { BidiCall, serializer, deserializer } from "../grpc.js";
import { RimeInputError } from "../errors.js";
import { policy } from "./policy.js";
import type { TranscriptionMode } from "./types.js";

export function requireSchema() {
  if (!schema.SpeechToText)
    throw new RimeInputError(
      "STT requires @rimelabs/api with SpeechToText definitions",
    );
}

export const transport = {
  makeClient: (target: string) =>
    new grpc.Client(target, grpc.credentials.createSsl(), {
      "grpc.enable_retries": 0,
      "grpc.max_receive_message_length": policy.receiveBytes,
      "grpc.max_send_message_length": 131072,
    }),
};

export class TranscriptionCall extends BidiCall<
  schema.StreamingTranscriptionRequest,
  schema.StreamingTranscriptionResponse
> {
  constructor(client: grpc.Client, auth: grpc.Metadata, signal: AbortSignal) {
    requireSchema();
    const service = schema.SpeechToText;
    const method = service.method.transcribeStreaming;
    super(
      client.makeBidiStreamRequest(
        `/${service.typeName}/${method.name}`,
        serializer(method.input),
        deserializer(method.output),
        auth,
      ),
      signal,
    );
  }
  start(
    language: string,
    mode: TranscriptionMode,
    contextTerms: readonly string[],
  ) {
    return this.write(
      create(schema.StreamingTranscriptionRequestSchema, {
        payload: {
          case: "config",
          value: {
            language,
            mode:
              mode === "verbatim"
                ? schema.TranscriptionMode.VERBATIM
                : schema.TranscriptionMode.WRITTEN,
            contextTerms: [...contextTerms],
            outputContract: schema.StreamingOutputContract.REVISED_HYPOTHESES,
          },
        },
      }),
    );
  }
  writeAudio(audio: Uint8Array) {
    return this.write(
      create(schema.StreamingTranscriptionRequestSchema, {
        payload: { case: "audio", value: audio },
      }),
    );
  }
}
