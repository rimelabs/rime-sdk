import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import {
  BidiCall,
  serializer,
  deserializer,
  requestId,
  nativeError,
} from "../grpc.js";
import { abortable, scope } from "../cancellation.js";
import {
  RimeInputError,
  RimeStreamError,
  RimeResourceLimitError,
  RimeTimeoutError,
  RimeCancelledError,
} from "../errors.js";
import { policy } from "./policy.js";
import type {
  TranscriptionMode,
  TranscribeOptions,
  Transcription,
} from "./types.js";

export const MAX_WAV_BYTES = 16 * 1024 * 1024;

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
      "grpc.max_send_message_length": MAX_WAV_BYTES + 65536,
    }),
};

export async function transcribe(
  client: grpc.Client,
  auth: grpc.Metadata,
  audio: Uint8Array,
  options: TranscribeOptions,
  signal: AbortSignal,
): Promise<Transcription> {
  const method = schema.SpeechToText.method.transcribe;
  const deadline = scope(
    [signal],
    policy.completionTimeout,
    new RimeTimeoutError("Transcription request timed out"),
  );
  let call: grpc.ClientUnaryCall | undefined;
  let id: string | null = null;
  try {
    deadline.signal.throwIfAborted();
    const result = await abortable(
      new Promise<schema.TranscriptionResponse>((resolve, reject) => {
        call = client.makeUnaryRequest(
          `/${schema.SpeechToText.typeName}/${method.name}`,
          serializer(method.input),
          deserializer(method.output),
          create(method.input, {
            audio,
            options: {
              language: options.language,
              mode:
                options.mode === "verbatim"
                  ? schema.TranscriptionMode.VERBATIM
                  : schema.TranscriptionMode.WRITTEN,
              contextTerms: [...(options.contextTerms ?? [])],
            },
          }),
          auth,
          (error, response) => (error ? reject(error) : resolve(response!)),
        );
        call.on("metadata", (headers: grpc.Metadata) => {
          id = requestId(headers);
        });
        call.on("status", (status: grpc.StatusObject) => {
          id ??= requestId(status.metadata);
        });
      }),
      deadline.signal,
    );
    if (!result.language?.tag || !result.language.source)
      throw new RimeStreamError(
        "The service did not confirm a resolved language",
        id,
      );
    if (Buffer.byteLength(result.text) > policy.transcriptBytes)
      throw new RimeResourceLimitError(
        "The transcript exceeds the SDK text limit",
        id,
      );
    return { text: result.text, language: result.language.tag, requestId: id };
  } catch (error) {
    if (deadline.signal.aborted) {
      const reason = deadline.signal.reason;
      if (reason instanceof RimeTimeoutError)
        throw new RimeTimeoutError(reason.message, id);
      throw new RimeCancelledError("Transcription cancelled", id);
    }
    throw nativeError(error, id);
  } finally {
    call?.cancel();
    deadline.dispose();
  }
}

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
