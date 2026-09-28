import * as grpc from "@grpc/grpc-js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";
export class FakeService {
  mode = "normal";
  rejectionStatus = grpc.status.UNAVAILABLE;
  responseMetadata = {
    "x-rime-audio-content-type": "audio/pcm",
    "x-request-id": "test-request",
  };
  trailingMetadata = {};
  calls = [];
  metadata = [];
  discoveryCalls = 0;
  discoveryFailures = 0;
  supportedSpeakers = ["test-speaker"];
  payload = Buffer.from(Array(2400).fill([1, 0]).flat());
  release = () => {};
  constructor() {
    this.server = new grpc.Server();
    this.headersSent = new Promise((resolve) => {
      this.markHeadersSent = resolve;
    });
  }
  async start() {
    const methods = {};
    const handlers = {};
    for (const [name, descriptor] of Object.entries(
      schema.TextToSpeech.method,
    )) {
      if (
        ![
          "synthesizeStreaming",
          "getSupportedSpeakers",
          "getSupportedLanguages",
        ].includes(name)
      )
        continue;
      methods[name] = {
        path: "/rime.TextToSpeech/" + descriptor.name,
        requestStream: descriptor.methodKind === "bidi_streaming",
        responseStream: descriptor.methodKind === "bidi_streaming",
        requestSerialize: (m) => Buffer.from(toBinary(descriptor.input, m)),
        requestDeserialize: (b) => fromBinary(descriptor.input, b),
        responseSerialize: (m) => Buffer.from(toBinary(descriptor.output, m)),
        responseDeserialize: (b) => fromBinary(descriptor.output, b),
      };
    }
    handlers.synthesizeStreaming = (call) => {
      this.calls.push([]);
      const messages = this.calls.at(-1);
      this.metadata.push(call.metadata);
      call.on("data", (message) => {
        messages.push(message);
        if (message.payload.case === "header") {
          if (this.mode === "empty_no_headers") {
            call.end();
            return;
          }
          if (this.mode === "error_before_audio") {
            call.emit("error", this.rejection());
            return;
          }
          if (this.mode === "headers_after_text") return;
          const metadata = this.makeMetadata(this.responseMetadata);
          if (this.mode === "wrong_format")
            metadata.set("x-rime-audio-content-type", "audio/wav");
          call.sendMetadata(metadata);
          this.markHeadersSent();
          if (this.mode === "no_audio_error")
            this.release = () => call.emit("error", this.rejection());
          return;
        }
        if (this.mode === "headers_after_text" && messages.length === 2) {
          call.sendMetadata(this.makeMetadata(this.responseMetadata));
          this.markHeadersSent();
        }
        if (["no_audio_error", "empty_audio"].includes(this.mode)) return;
        const send = () => {
          if (this.mode === "odd_chunks") {
            call.write(
              create(schema.SynthesisResponseStreamSchema, {
                audio: this.payload.subarray(0, 1),
              }),
            );
            call.write(
              create(schema.SynthesisResponseStreamSchema, {
                audio: this.payload.subarray(1),
              }),
            );
          } else
            for (let i = 0; i < (this.mode === "burst" ? 100 : 1); i++)
              call.write(
                create(schema.SynthesisResponseStreamSchema, {
                  audio: this.payload,
                }),
              );
        };
        if (this.mode === "silence") {
          this.release = send;
          return;
        }
        send();
        if (this.mode === "partial_error")
          this.release = () =>
            call.emit(
              "error",
              Object.assign(new Error("service failure"), {
                code: grpc.status.UNAVAILABLE,
              }),
            );
      });
      call.on("end", () => {
        if (!["partial_error", "silence", "no_audio_error"].includes(this.mode))
          call.end(this.makeMetadata(this.trailingMetadata));
      });
      call.on("error", () => {});
    };
    handlers.getSupportedLanguages = (call, callback) => {
      this.discoveryCalls++;
      if (this.mode === "discovery_timeout") {
        call.sendMetadata(this.makeMetadata(this.responseMetadata));
        this.markHeadersSent();
        return;
      }
      if (this.mode === "discovery_error") {
        call.sendMetadata(this.makeMetadata(this.responseMetadata));
        callback(this.rejection());
        return;
      }
      if (this.discoveryCalls <= this.discoveryFailures)
        callback(
          Object.assign(new Error("retry"), { code: grpc.status.UNAVAILABLE }),
        );
      else
        callback(
          null,
          create(schema.GetSupportedLanguagesResponseSchema, {
            languages: ["en", "de"],
          }),
        );
    };
    handlers.getSupportedSpeakers = (call, callback) => {
      if (this.mode === "discovery_timeout") {
        call.sendMetadata(this.makeMetadata(this.responseMetadata));
        this.markHeadersSent();
        return;
      }
      if (this.mode === "discovery_error") {
        call.sendMetadata(this.makeMetadata(this.responseMetadata));
        callback(this.rejection());
        return;
      }
      callback(
        null,
        create(schema.GetSupportedSpeakersResponseSchema, {
          speakers: this.supportedSpeakers,
        }),
      );
    };
    this.server.addService(methods, handlers);
    const port = await new Promise((resolve, reject) =>
      this.server.bindAsync(
        "127.0.0.1:0",
        grpc.ServerCredentials.createInsecure(),
        (error, port) => (error ? reject(error) : resolve(port)),
      ),
    );
    this.target = "127.0.0.1:" + port;
    return this;
  }
  close() {
    this.server.forceShutdown();
  }
  makeMetadata(values) {
    const metadata = new grpc.Metadata();
    for (const [name, value] of Object.entries(values))
      metadata.set(name, value);
    return metadata;
  }
  rejection() {
    return Object.assign(new Error("test service rejection"), {
      code: this.rejectionStatus,
      metadata: this.makeMetadata(this.trailingMetadata),
    });
  }
}
