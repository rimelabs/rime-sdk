import * as grpc from "@grpc/grpc-js";
import {
  create,
  fromBinary,
  toBinary,
  type DescMessage,
} from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { policy, abortable } from "./policy.js";
import * as errors from "./errors.js";
export { grpc, create, schema };
export function rpcError(
  code: number,
  requestId: string | null = null,
): errors.RimeError {
  const classes: Record<number, typeof errors.RimeError> = {
    [grpc.status.UNAUTHENTICATED]: errors.RimeAuthenticationError,
    [grpc.status.PERMISSION_DENIED]: errors.RimePermissionError,
    [grpc.status.INVALID_ARGUMENT]: errors.RimeInputError,
    [grpc.status.RESOURCE_EXHAUSTED]: errors.RimeResourceLimitError,
    [grpc.status.UNAVAILABLE]: errors.RimeUnavailableError,
    [grpc.status.DEADLINE_EXCEEDED]: errors.RimeTimeoutError,
    [grpc.status.CANCELLED]: errors.RimeCancelledError,
  };
  const ErrorType = classes[code] ?? errors.RimeStreamError;
  return new ErrorType(
    `Rime operation failed: ${grpc.status[code] ?? "UNKNOWN"}`,
    requestId,
  );
}
export const transport = {
  makeClient: () =>
    new grpc.Client(policy.target, grpc.credentials.createSsl(), {
      "grpc.enable_retries": 0,
      "grpc.max_receive_message_length": policy.receiveBytes,
      "grpc.max_send_message_length": policy.sentenceBytes + 65536,
    }),
};
export function serializer(desc: DescMessage) {
  return (message: any) => Buffer.from(toBinary(desc, message));
}
export function deserializer(desc: DescMessage) {
  return (data: Buffer) => fromBinary(desc, data);
}
export async function connect(client: grpc.Client, signal: AbortSignal) {
  await abortable(
    new Promise<void>((resolve, reject) =>
      client.waitForReady(
        Date.now() + policy.connectionTimeout * 1000,
        (error) =>
          error
            ? reject(
                new errors.RimeTimeoutError(
                  "Connection establishment timed out",
                ),
              )
            : resolve(),
      ),
    ),
    signal,
  );
}
export function metadata(value: string) {
  const result = new grpc.Metadata();
  result.set("authorization", value);
  return result;
}
export function openStream(client: grpc.Client, auth: grpc.Metadata) {
  return client.makeBidiStreamRequest(
    "/rime.TextToSpeech/SynthesizeStreaming",
    serializer(schema.StreamingSynthesisRequestSchema),
    deserializer(schema.SynthesisResponseStreamSchema),
    auth,
  );
}
export function header(voice: string, language: string) {
  return create(schema.StreamingSynthesisRequestSchema, {
    payload: {
      case: "header",
      value: create(schema.SynthesisRequestSchema, {
        speaker: voice,
        language,
        audioParameters: { audioFormat: "audio/pcm", samplingRate: 24000 },
      }),
    },
  });
}
export function textMessage(text: string) {
  return create(schema.StreamingSynthesisRequestSchema, {
    payload: { case: "textChunk", value: text },
  });
}
