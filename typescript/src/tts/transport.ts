import { connect as connectGrpc, metadata } from "../grpc.js";
export { metadata } from "../grpc.js";
import {
  BidiCall,
  rpcError,
  serializer,
  deserializer,
  requestId,
  nativeError,
} from "../grpc.js";
export { rpcError } from "../grpc.js";
import * as grpc from "@grpc/grpc-js";
import {
  create,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
import { policy } from "./policy.js";
import { abortable } from "../cancellation.js";
import * as errors from "../errors.js";
export type Connection = grpc.Client;
export interface PreparedConnection {
  client: Connection;
  metadata: grpc.Metadata;
}
export type DiscoveryKind = "voices" | "languages";
export const transport = {
  makeClient: (target: string) =>
    new grpc.Client(target, grpc.credentials.createSsl(), {
      "grpc.enable_retries": 0,
      "grpc.max_receive_message_length": policy.receiveBytes,
      "grpc.max_send_message_length": policy.sentenceBytes + 65536,
    }),
};
export function connect(client: grpc.Client, signal: AbortSignal) {
  return connectGrpc(client, signal, policy.connectionTimeout);
}
function openStream(client: grpc.Client, auth: grpc.Metadata) {
  return client.makeBidiStreamRequest<
    schema.StreamingSynthesisRequest,
    schema.SynthesisResponseStream
  >(
    "/rime.TextToSpeech/SynthesizeStreaming",
    serializer(schema.StreamingSynthesisRequestSchema),
    deserializer(schema.SynthesisResponseStreamSchema),
    auth,
  );
}
function header(voice: string, language: string) {
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
function textMessage(text: string) {
  return create(schema.StreamingSynthesisRequestSchema, {
    payload: { case: "textChunk", value: text },
  });
}

async function unary<I extends DescMessage, O extends DescMessage>(
  prepared: PreparedConnection,
  name: string,
  input: I,
  output: O,
  request: MessageShape<I>,
  signal: AbortSignal,
): Promise<MessageShape<O>> {
  let call: grpc.ClientUnaryCall | undefined;
  let id: string | null = null;
  try {
    signal.throwIfAborted();
    const operation = new Promise<MessageShape<O>>((resolve, reject) => {
      call = prepared.client.makeUnaryRequest(
        "/rime.TextToSpeech/" + name,
        serializer(input),
        deserializer(output),
        request,
        prepared.metadata,
        (error, result) => (error ? reject(error) : resolve(result!)),
      );
      call.once("metadata", (metadata: grpc.Metadata) => {
        id = requestId(metadata);
      });
    });
    return await abortable(operation, signal);
  } catch (error) {
    call?.cancel();
    if (signal.aborted) {
      if (signal.reason instanceof errors.RimeTimeoutError)
        throw new errors.RimeTimeoutError(signal.reason.message, id, {
          cause: signal.reason.cause,
        });
      throw signal.reason;
    }
    throw nativeError(error, id);
  }
}

export async function discover(
  prepared: PreparedConnection,
  kind: DiscoveryKind,
  language: string | null | undefined,
  signal: AbortSignal,
): Promise<string[]> {
  // Exactly one attempt. The client owns retries and the shared deadline.
  if (kind === "voices") {
    const method = schema.TextToSpeech.method.getSupportedSpeakers;
    const response = await unary(
      prepared,
      method.name,
      method.input,
      method.output,
      create(method.input, { language: language ?? undefined }),
      signal,
    );
    return response.speakers;
  }
  const method = schema.TextToSpeech.method.getSupportedLanguages;
  const response = await unary(
    prepared,
    method.name,
    method.input,
    method.output,
    create(method.input),
    signal,
  );
  return response.languages;
}

export class SynthesisCall {
  private readonly rpc: BidiCall<
    schema.StreamingSynthesisRequest,
    schema.SynthesisResponseStream
  >;
  constructor(prepared: PreparedConnection, signal: AbortSignal) {
    this.rpc = new BidiCall(
      openStream(prepared.client, prepared.metadata),
      signal,
    );
  }
  get requestId() {
    return this.rpc.requestId;
  }
  get done() {
    return this.rpc.done;
  }
  cancel() {
    this.rpc.cancel();
  }
  start(voice: string, language: string) {
    return this.rpc.write(header(voice, language));
  }
  write(sentence: string) {
    return this.rpc.write(textMessage(sentence));
  }
  finishInput() {
    this.rpc.finishInput();
  }
  private checkFormat(contentType: unknown) {
    if (contentType !== "audio/pcm")
      throw new errors.RimeAudioFormatError(
        "Expected raw audio/pcm from the service",
        this.requestId,
      );
  }
  async *audio(): AsyncGenerator<Uint8Array> {
    const headers = await this.rpc.headers();
    const contentType = headers.get("x-rime-audio-content-type")[0];
    for await (const response of this.rpc.responses()) {
      if (response.payload.case !== "audio") continue;
      if (response.payload.value.length) this.checkFormat(contentType);
      yield response.payload.value;
    }
    this.checkFormat(contentType);
  }
}
