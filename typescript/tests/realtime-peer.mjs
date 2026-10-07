import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import Ajv from "ajv";
import { parse } from "yaml";

const contract = parse(
  readFileSync(
    new URL(
      "../../conformance/prism/speech_to_speech.asyncapi.yaml",
      import.meta.url,
    ),
    "utf8",
  ),
);
const ajv = new Ajv({ strict: false, validateFormats: false });
const validators = new Map(
  Object.values(contract.components.messages).map((message) => [
    message.name,
    ajv.compile({ ...message.payload, components: contract.components }),
  ]),
);

export class Peer {
  queue = [];
  waiters = [];
  requests = [];
  sockets = [];
  invalid = [];
  settings = null;
  autoInitialize = true;
  autoCreate = true;
  onRequest = null;
  async start() {
    this.server = new WebSocketServer({
      host: "127.0.0.1",
      port: 0,
      path: "/v1/realtime",
    });
    this.server.on("connection", (socket, request) => {
      this.socket = socket;
      this.sockets.push(socket);
      this.authorization = request.headers.authorization;
      socket.on("message", (raw) => {
        const event = JSON.parse(raw.toString());
        const validate = validators.get(event.type);
        if (!validate?.(event))
          this.invalid.push({ event, errors: validate?.errors });
        this.requests.push(event);
        if (event.type === "session.update" && this.autoInitialize) {
          this.settings = event.session;
          this.emit(
            "session.updated",
            {
              prsm_request_event_id: event.event_id,
              session: {
                id: "session-1",
                voice: "test",
                prsm_effective_interrupt_response: false,
                prsm_tool_waits: {
                  result_timeout_s: 20,
                  continuation_timeout_s: 5,
                },
              },
            },
            socket,
          );
        } else if (this.onRequest) this.onRequest(event, socket);
        else if (this.waiters.length) this.waiters.shift()(event);
        else this.queue.push(event);
      });
      if (this.autoCreate)
        this.emit("session.created", { session: { id: "session-1" } }, socket);
    });
    await once(this.server, "listening");
    this.endpoint = `ws://127.0.0.1:${this.server.address().port}/v1/realtime`;
    return this;
  }
  emit(type, body = {}, socket = this.socket) {
    socket.send(JSON.stringify({ type, event_id: "server-event", ...body }));
  }
  async next(kind) {
    let timer;
    const event =
      this.queue.shift() ??
      (await Promise.race([
        new Promise((resolve) => this.waiters.push(resolve)),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`No ${kind} request`)),
            1500,
          );
        }),
      ]).finally(() => clearTimeout(timer)));
    assert.equal(event.type, kind);
    return event;
  }
  accepted(request, id = "reply-1", metadata = {}) {
    this.emit("response.created", {
      prsm_request_event_id: request.event_id,
      response: {
        id,
        metadata: { ...request.response?.metadata, ...metadata },
      },
    });
  }
  ended(id = "reply-1", status = "completed") {
    this.emit("response.done", {
      response: { id, status, metadata: { prsm_drain_token: "drain-1" } },
    });
  }
  fault(request, code, scope = "event", owner = {}) {
    this.emit("error", {
      error: {
        code,
        message: code,
        scope,
        owner: { event_id: request?.event_id, ...owner },
      },
    });
  }
  tool(id = "reply-1", call = "call-1", args = '{"order_id":"demo-123"}') {
    this.emit("response.function_call_arguments.done", {
      response_id: id,
      item_id: `item-${call}`,
      call_id: call,
      name: "lookup_order",
      arguments: args,
    });
  }
  message(id = "reply-1", item = "message-1", index = 0) {
    this.emit("response.output_item.added", {
      response_id: id,
      output_index: index,
      item: { type: "message", id: item },
    });
  }
  ack(request) {
    this.emit("conversation.item.created", {
      prsm_request_event_id: request.event_id,
      item:
        request.item?.type === "function_call_output"
          ? { type: "function_call_output", call_id: request.item.call_id }
          : {
              id: "history-1",
              type: "message",
              role: request.item?.role ?? "user",
            },
    });
  }
  async close() {
    for (const socket of this.sockets) socket.terminate();
    await new Promise((resolve) => this.server.close(resolve));
    assert.deepEqual(
      this.invalid,
      [],
      "Outgoing messages must match the pinned Prism schema",
    );
  }
}
