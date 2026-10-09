import * as grpc from "@grpc/grpc-js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as schema from "@rimelabs/api";

export class RecognitionService {
  mode = "normal";
  rejection = grpc.status.UNAVAILABLE;
  calls = [];
  metadata = [];
  inputFinished = false;
  configSeen = new Promise((resolve) => {
    this.markConfig = resolve;
  });
  cancelled = new Promise((resolve) => {
    this.markCancelled = resolve;
  });
  release = () => {};
  async start() {
    this.server = new grpc.Server();
    const method = schema.SpeechToText.method.transcribeStreaming;
    this.server.addService(
      {
        streaming: {
          path: `/${schema.SpeechToText.typeName}/${method.name}`,
          requestStream: true,
          responseStream: true,
          requestSerialize: (m) => Buffer.from(toBinary(method.input, m)),
          requestDeserialize: (b) => fromBinary(method.input, b),
          responseSerialize: (m) => Buffer.from(toBinary(method.output, m)),
          responseDeserialize: (b) => fromBinary(method.output, b),
        },
      },
      {
        streaming: (call) => {
          const messages = [];
          this.calls.push(messages);
          this.metadata.push(call.metadata);
          let revision = 0n,
            text = "";
          const language = {
            tag: "en",
            source: schema.LanguageSource.SELECTED,
          };
          const send = (name, value) =>
            call.write(
              create(method.output, { payload: { case: name, value } }),
            );
          const fail = () => {
            const metadata = new grpc.Metadata();
            metadata.set("x-request-id", "stt-rejected");
            call.emit(
              "error",
              Object.assign(new Error("deliberate rejection"), {
                code: this.rejection,
                metadata,
              }),
            );
          };
          call.on("cancelled", () => this.markCancelled());
          call.on("error", () => {});
          call.on("data", (message) => {
            messages.push(message);
            if (message.payload.case === "config") {
              this.markConfig();
              if (this.mode === "reject") {
                fail();
                return;
              }
              const headers = new grpc.Metadata();
              headers.set("x-request-id", "stt-request");
              call.sendMetadata(headers);
              if (this.mode === "no_acceptance") return;
              send("accepted", {
                outputContract:
                  schema.StreamingOutputContract.REVISED_HYPOTHESES,
                language,
              });
              if (this.mode === "early_final") {
                send("done", { text, revision, language });
                call.end();
              }
              return;
            }
            if (this.mode === "silence") return;
            for (
              let index = 0;
              index < (this.mode === "burst" ? 100 : 2);
              index++
            ) {
              revision++;
              text = index === 0 ? "I scream" : "Ice cream";
              send("hypothesis", { text, revision });
            }
            if (this.mode === "partial_error") this.release = fail;
          });
          call.on("end", () => {
            this.inputFinished = true;
            if (
              [
                "no_completion",
                "partial_error",
                "reject",
                "early_final",
                "no_acceptance",
              ].includes(this.mode)
            )
              return;
            if (this.mode !== "missing_final")
              send("done", { text, revision, language });
            if (this.mode === "done_then_error") {
              fail();
              return;
            }
            if (this.mode === "duplicate_final")
              send("done", { text, revision, language });
            call.end();
          });
        },
      },
    );
    const port = await new Promise((resolve, reject) =>
      this.server.bindAsync(
        "127.0.0.1:0",
        grpc.ServerCredentials.createInsecure(),
        (error, port) => (error ? reject(error) : resolve(port)),
      ),
    );
    this.target = `127.0.0.1:${port}`;
    return this;
  }
  close() {
    this.server.forceShutdown();
  }
}
