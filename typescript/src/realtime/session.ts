// SPDX-License-Identifier: Apache-2.0
// Protocol rules adapted from rimelabs/sglang-omni clients/prism_realtime at
// bc8a500249536f369d20bc094baa9169e5986758, as in the Python SDK.
import { createHash, randomUUID } from "node:crypto";
import { setMaxListeners } from "node:events";
import { abortable } from "../cancellation.js";
import {
  RimeError,
  RimeInputError,
  RimeResourceLimitError,
  RimeStreamError,
  RimeTimeoutError,
} from "../errors.js";
import { Deferred, Flag, Mutex, cancellation, scope } from "./async.js";
import { formatOf, InputConverter } from "./audio.js";
import { RealtimeConnection } from "./transport.js";
import * as t from "./types.js";
import * as p from "./protocol.js";

const NOT_READY = new Set([
  "typed_turn_busy",
  "instructions_busy",
  "proactive_unavailable",
]);
const LIMIT = 128;
interface Pending {
  request: p.Request<p.Acknowledgment>;
  result: Deferred<p.Acknowledgment>;
  target?: string;
  itemId?: string;
}
class ResponseState {
  readonly done = new Flag();
  readonly changed = new Flag();
  readonly calls = new Map<string, string | null>();
  readonly outputs = new Set<string>();
  readonly cancelLock = new Mutex();
  token: string | null = null;
  continued = false;
  abandoned = false;
  constructor(readonly ref: t.ResponseRef) {}
}
function outputKey(output: t.OutputRef) {
  return JSON.stringify([
    output.itemId,
    output.outputIndex,
    output.contentIndex,
  ]);
}
function text(value: string) {
  if (typeof value !== "string" || !value.trim() || [...value].length > 4000)
    throw new RimeInputError(
      "Text must be nonblank and at most 4000 characters",
    );
}

/** One conversation. Consume events concurrently with session operations. */
export class RealtimeSession {
  private readonly created = new Deferred<p.SessionView>();
  private sessionId: string | null = null;
  private sessionInfo: t.SessionInfo | null = null;
  private readonly lifetime = new AbortController();
  private failure: RimeError | null = null;
  private closed = false;
  private closing: Promise<void> | null = null;
  private consuming = false;
  private readonly queue: t.SessionEvent[] = [];
  private readonly queued = new Flag();
  private readonly ready = new Flag();
  private readyEpoch = 0;
  private readonly admission = new Mutex();
  private readonly writeLock = new Mutex();
  private readonly audioLock = new Mutex();
  private readonly truncateLock = new Mutex();
  private readonly toolLock = new Mutex();
  private readonly pending = new Map<string, Pending>();
  private readonly responses = new Map<string, ResponseState>();
  private latest: string | null = null;
  private readonly tasks = new Set<Promise<unknown>>();
  private converter: InputConverter | null = null;

  /** @internal Use client.realtime.connect(). */
  constructor(
    private readonly socket: RealtimeConnection,
    private readonly timeouts: Required<t.RealtimeTimeouts>,
    private readonly forget: () => void,
  ) {
    setMaxListeners(0, this.lifetime.signal);
    socket.onFailure = (error) => this.fail(error);
    socket.onMessage = (raw) => {
      if (this.closed) return;
      try {
        const event = p.decode(raw, this.sessionId);
        if (event) this.dispatch(event);
      } catch (error) {
        this.fail(
          error instanceof RimeError
            ? error
            : new RimeStreamError("Invalid realtime event"),
        );
      }
    };
  }

  /** @internal */
  async initialize(
    settings: p.SessionSettings,
    signal: AbortSignal,
  ): Promise<void> {
    const deadline = scope(
      [signal, this.lifetime.signal],
      this.timeouts.connectS,
      new RimeTimeoutError("Realtime connection or session.created timed out"),
    );
    try {
      await abortable(this.socket.ready, deadline.signal);
      await abortable(this.created.promise, deadline.signal);
    } finally {
      deadline.dispose();
    }
    const event = await this.request(
      p.requests.update,
      { session: settings },
      signal,
    );
    const view = event.session;
    this.sessionInfo = Object.freeze({
      sessionId: this.id(),
      model: "prism",
      voice: view.voice ?? "",
      interruptOnSpeech:
        view.interruptOnSpeech ?? settings.turn_detection.interrupt_response,
      toolResultTimeoutS: view.toolResultTimeoutS ?? null,
      toolContinuationTimeoutS: view.toolContinuationTimeoutS ?? null,
    });
  }

  get info(): t.SessionInfo {
    if (!this.sessionInfo)
      throw new RimeInputError("The session has not initialized");
    return this.sessionInfo;
  }
  get events(): AsyncIterableIterator<t.SessionEvent> {
    return this.readEvents();
  }
  private async *readEvents(): AsyncIterableIterator<t.SessionEvent> {
    if (this.consuming)
      throw new RimeInputError("Only one consumer may read session events");
    this.consuming = true;
    try {
      for (;;) {
        if (this.failure) throw this.failure;
        const event = this.queue.shift();
        if (event) {
          yield event;
          continue;
        }
        if (this.closed) return;
        this.queued.clear();
        // Close wakes this flag; use no cancellation so explicit close ends normally.
        await this.queued.wait(new AbortController().signal);
      }
    } finally {
      this.consuming = false;
    }
  }
  private id(): string {
    if (!this.sessionId)
      throw new RimeStreamError("Event arrived before session.created");
    return this.sessionId;
  }
  private check() {
    if (this.failure) throw this.failure;
    if (this.closed) throw new RimeInputError("The realtime session is closed");
  }
  private response(ref: t.ResponseRef | t.ToolCallRef): ResponseState {
    this.check();
    if (ref.sessionId !== this.info.sessionId)
      throw new RimeInputError("Reference belongs to another session");
    const state = this.responses.get(ref.responseId);
    if (!state)
      throw new RimeInputError("Unknown or expired response reference");
    if ("callId" in ref && !state.calls.has(ref.callId))
      throw new RimeInputError("Unknown tool call reference");
    return state;
  }
  private track<T>(task: Promise<T>): Promise<T> {
    this.tasks.add(task);
    task.then(
      () => this.tasks.delete(task),
      () => this.tasks.delete(task),
    );
    return task;
  }
  private capacity() {
    this.check();
    if (this.tasks.size >= LIMIT)
      throw new RimeResourceLimitError("Too many pending realtime operations");
  }
  private async operation<T>(
    options: t.RealtimeOperationOptions,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    this.check();
    const caller = cancellation(options.signal);
    const lifetime = scope([caller.signal, this.lifetime.signal]);
    try {
      lifetime.signal.throwIfAborted();
      return await work(lifetime.signal);
    } finally {
      lifetime.dispose();
      caller.dispose();
    }
  }
  private async locked<T>(
    mutex: Mutex,
    signal: AbortSignal,
    work: () => Promise<T>,
  ): Promise<T> {
    const release = await mutex.acquire(signal);
    try {
      signal.throwIfAborted();
      this.check();
      return await work();
    } finally {
      release();
    }
  }
  private async send(
    event: Record<string, unknown>,
    signal: AbortSignal,
    markSubmitted: () => void = () => {},
  ): Promise<void> {
    const deadline = scope(
      [signal, this.lifetime.signal],
      this.timeouts.requestS,
      new RimeTimeoutError("Realtime write timed out; outcome unknown"),
    );
    try {
      await this.locked(this.writeLock, deadline.signal, async () => {
        const encoded = JSON.stringify(event);
        markSubmitted();
        await abortable(this.socket.send(encoded), deadline.signal);
      });
    } catch (error) {
      if (error instanceof RimeTimeoutError || error instanceof RimeStreamError)
        this.fail(error);
      throw error;
    } finally {
      deadline.dispose();
    }
  }
  private async request<T extends p.Acknowledgment>(
    request: p.Request<T>,
    body: Record<string, unknown>,
    signal: AbortSignal,
    target?: string,
    continuation?: ResponseState,
  ): Promise<T> {
    const kind = request.kind;
    signal.throwIfAborted();
    this.capacity();
    if (kind === "response.create") {
      if (continuation) continuation.continued = true;
      else this.ready.clear();
    }
    let accepted: t.ResponseRef | undefined;
    const task = this.track(
      this.runRequest(
        request,
        body,
        signal,
        this.readyEpoch,
        target,
        continuation,
      ).then((result) => {
        if (result.kind === "response.started") {
          accepted = result.response;
          if (signal.aborted) this.abandon(result.response);
        }
        return result;
      }),
    );
    try {
      const result = await abortable(task, signal);
      signal.throwIfAborted();
      return request.result(result);
    } catch (error) {
      if (signal.aborted) {
        if (kind !== "response.create") await task.catch(() => {});
        else if (accepted) this.abandon(accepted);
      }
      throw error;
    }
  }
  private async runRequest(
    request: p.Request<p.Acknowledgment>,
    body: Record<string, unknown>,
    caller: AbortSignal,
    epoch: number,
    target?: string,
    continuation?: ResponseState,
  ): Promise<p.Acknowledgment> {
    const kind = request.kind;
    const eventId = `evt_${randomUUID().replaceAll("-", "")}`;
    const result = new Deferred<p.Acknowledgment>();
    this.pending.set(eventId, { request, result, target });
    const deadline = scope(
      [this.lifetime.signal],
      this.timeouts.requestS,
      new RimeTimeoutError(
        `${kind}: no acknowledgment; outcome unknown`,
        eventId,
      ),
    );
    let submitted = false;
    // Cancellation can prevent a write. After submission, keep matching its outcome.
    const beforeSend = scope([caller, deadline.signal]);
    try {
      const release = await this.writeLock.acquire(beforeSend.signal);
      try {
        beforeSend.signal.throwIfAborted();
        this.check();
        const encoded = JSON.stringify({
          type: kind,
          event_id: eventId,
          ...body,
        });
        submitted = true;
        await abortable(this.socket.send(encoded), deadline.signal);
      } finally {
        release();
        beforeSend.dispose();
      }
      return await abortable(result.promise, deadline.signal);
    } catch (error) {
      if (
        continuation &&
        error instanceof t.RimeRealtimeError &&
        error.fault.scope === "event" &&
        error.fault.code === "tool_continuation_not_ready"
      )
        continuation.continued = false;
      if (error instanceof RimeTimeoutError || error instanceof RimeStreamError)
        this.fail(error);
      throw error;
    } finally {
      if (kind === "response.create" && !submitted) {
        if (continuation) continuation.continued = false;
        else if (epoch === this.readyEpoch) this.ready.set();
      }
      this.pending.delete(eventId);
      beforeSend.dispose();
      deadline.dispose();
    }
  }
  private abandon(ref: t.ResponseRef) {
    const state = this.responses.get(ref.responseId);
    if (!state || state.abandoned || this.closed) return;
    state.abandoned = true;
    this.publish({ kind: "response.abandoned", response: ref });
    this.track(this.cancel(ref));
  }
  private async create(
    body: Record<string, unknown>,
    signal: AbortSignal,
    request: p.Request<t.ResponseStarted> = p.requests.create,
  ): Promise<t.ResponseRef> {
    const deadline = performance.now() + this.timeouts.readyS * 1000;
    const admission = scope(
      [signal],
      this.timeouts.readyS,
      new t.RealtimeAdmissionTimeout("Prism admission exceeded readyS"),
    );
    let release: (() => void) | undefined;
    try {
      release = await this.admission.acquire(admission.signal);
    } finally {
      admission.dispose();
    }
    try {
      for (;;) {
        this.check();
        const ready = scope(
          [signal],
          (deadline - performance.now()) / 1000,
          new t.RealtimeAdmissionTimeout(
            "Prism did not admit the turn before readyS expired",
          ),
        );
        try {
          await this.ready.wait(ready.signal);
          ready.signal.throwIfAborted();
        } finally {
          ready.dispose();
        }
        try {
          return (await this.request(request, { response: body }, signal))
            .response;
        } catch (error) {
          if (
            !(error instanceof t.RimeRealtimeError) ||
            error.fault.scope !== "event" ||
            !NOT_READY.has(error.fault.code)
          )
            throw error;
        }
      }
    } finally {
      release();
    }
  }
  async sendText(
    value: string,
    options: t.RealtimeOperationOptions = {},
  ): Promise<t.ResponseRef> {
    text(value);
    return this.operation(options, (signal) =>
      this.create(
        { prsm_input_text: value, metadata: { prsm_cause: "user_text" } },
        signal,
        p.requests.text,
      ),
    );
  }
  async requestReply(options: t.ReplyOptions = {}): Promise<t.ResponseRef> {
    const body: Record<string, unknown> = {
      metadata: { prsm_cause: "proactive" },
    };
    if (options.instruction != null) {
      text(options.instruction);
      body.prsm_instruction = options.instruction;
    }
    if (options.toolCall != null) {
      const state = this.response(options.toolCall);
      if (state.calls.get(options.toolCall.callId) === null)
        throw new RimeInputError(
          "Record the tool result before requesting its report",
        );
      body.prsm_call_id = options.toolCall.callId;
    }
    return this.operation(options, (signal) => this.create(body, signal));
  }
  async addMessage(
    role: "user" | "assistant",
    value: string,
    options: t.RealtimeOperationOptions = {},
  ): Promise<t.ItemRef> {
    if (role !== "user" && role !== "assistant")
      throw new RimeInputError("History role must be user or assistant");
    text(value);
    return this.operation(options, async (signal) => {
      const event = await this.request(
        p.requests.item,
        { item: { type: "message", role, text: value } },
        signal,
      );
      return Object.freeze({
        sessionId: this.info.sessionId,
        itemId: event.itemId,
      });
    });
  }
  async submitToolResult(
    call: t.ToolCallRef,
    output: string,
    options: t.RealtimeOperationOptions = {},
  ): Promise<void> {
    const ref = { ...call };
    this.response(ref);
    if (typeof output !== "string")
      throw new RimeInputError("Tool output must be a string");
    return this.operation(options, async (signal) => {
      this.capacity();
      const task = this.track(
        this.locked(this.toolLock, this.lifetime.signal, async () => {
          const state = this.response(ref);
          const digest = createHash("sha256").update(output).digest("hex");
          const recorded = state.calls.get(ref.callId);
          if (recorded !== null) {
            if (recorded !== digest)
              throw new RimeInputError("A recorded tool result cannot change");
            return;
          }
          await this.request(
            p.requests.toolResult,
            {
              item: {
                type: "function_call_output",
                call_id: ref.callId,
                output,
              },
            },
            this.lifetime.signal,
            ref.callId,
          );
          state.calls.set(ref.callId, digest);
          state.changed.set();
        }),
      );
      await abortable(task, signal);
    });
  }
  async continueReply(
    parent: t.ResponseRef,
    options: t.RealtimeOperationOptions = {},
  ): Promise<t.ResponseRef> {
    const ref = { ...parent };
    const state = this.response(ref);
    return this.operation(options, async (signal) => {
      const ready = scope(
        [signal],
        this.timeouts.readyS,
        new t.RealtimeAdmissionTimeout(
          "Tool results or parent completion did not arrive before readyS expired",
        ),
      );
      try {
        for (;;) {
          this.check();
          ready.signal.throwIfAborted();
          if (ref.responseId !== this.latest || state.continued)
            throw new RimeInputError(
              "The tool round is superseded or already continued",
            );
          if (
            state.done.isSet &&
            state.calls.size &&
            [...state.calls.values()].every((value) => value !== null)
          )
            break;
          if (state.done.isSet && !state.calls.size)
            throw new RimeInputError("The response has no tool calls");
          state.changed.clear();
          await state.changed.wait(ready.signal);
        }
      } finally {
        ready.dispose();
      }
      return (
        await this.request(
          p.requests.create,
          {
            response: {
              metadata: {
                prsm_cause: "tool_continuation",
                prsm_parent_response_id: ref.responseId,
              },
            },
          },
          signal,
          undefined,
          state,
        )
      ).response;
    });
  }
  async cancel(
    response: t.ResponseRef,
    options: t.RealtimeOperationOptions = {},
  ): Promise<void> {
    const state = this.response(response);
    return this.operation(options, (signal) =>
      this.locked(state.cancelLock, signal, async () => {
        if (!state.done.isSet)
          await this.request(
            p.requests.cancel,
            { response_id: state.ref.responseId },
            signal,
            state.ref.responseId,
          );
      }),
    );
  }
  async clearAudio(options: t.RealtimeOperationOptions = {}): Promise<void> {
    return this.operation(options, (signal) =>
      this.locked(this.audioLock, signal, async () => {
        await this.request(p.requests.clear, {}, signal);
      }),
    );
  }
  async sendAudio(
    chunk: t.AudioChunk,
    options: t.RealtimeOperationOptions = {},
  ): Promise<void> {
    const format = formatOf(chunk);
    // Snapshot one bounded chunk before the first await.
    const data = Buffer.from(chunk.data);
    return this.operation(options, (signal) =>
      this.locked(this.audioLock, signal, async () => {
        const previous = this.converter;
        const same =
          previous &&
          previous.format.sampleRate === format.sampleRate &&
          previous.format.channels === format.channels;
        this.converter = same ? previous.clone() : new InputConverter(format);
        const converted = this.converter.process(data);
        let submitted = false;
        try {
          for (let offset = 0; offset < converted.length; offset += 1280)
            await this.send(
              {
                type: "input_audio_buffer.append",
                audio: converted
                  .subarray(offset, offset + 1280)
                  .toString("base64"),
              },
              signal,
              () => {
                submitted = true;
              },
            );
        } catch (error) {
          if (signal.aborted) {
            if (submitted)
              this.fail(
                new RimeStreamError(
                  "Realtime audio submission cancelled; outcome unknown",
                ),
              );
            else this.converter = previous;
          }
          throw error;
        }
      }),
    );
  }
  async reportPlayback(
    report: t.PlaybackReport,
    options: t.RealtimeOperationOptions = {},
  ): Promise<void> {
    if (report.kind === "interrupted") {
      const state = this.response(report.output.response);
      if (!state.outputs.has(outputKey(report.output)))
        throw new RimeInputError("Unknown output reference");
      if (
        !Number.isInteger(report.audioEndMs) ||
        report.audioEndMs < 0 ||
        report.audioEndMs > 4294967295
      )
        throw new RimeInputError(
          "audioEndMs must be an unsigned 32-bit integer",
        );
      const body = {
        item_id: report.output.itemId,
        content_index: report.output.contentIndex,
        audio_end_ms: report.audioEndMs,
      };
      return this.operation(options, (signal) =>
        this.locked(this.truncateLock, signal, async () => {
          await this.request(p.requests.truncate, body, signal, body.item_id);
        }),
      );
    }
    if (report.kind !== "finished")
      throw new RimeInputError("Unknown playback report kind");
    const state = this.response(report.response);
    const played = report.playedMs ?? null,
      tail = report.audibleTailMs ?? null;
    for (const value of [played, tail])
      if (
        value !== null &&
        (typeof value !== "number" || !Number.isFinite(value) || value < 0)
      )
        throw new RimeInputError(
          "Playback times must be finite and nonnegative",
        );
    return this.operation(options, async (signal) => {
      const ready = scope(
        [signal],
        this.timeouts.readyS,
        new RimeTimeoutError(
          "Response did not finish before playback report deadline",
        ),
      );
      try {
        await state.done.wait(ready.signal);
      } finally {
        ready.dispose();
      }
      this.check();
      if (state.token)
        await this.send(
          {
            type: "prsm.playback.drained",
            response_id: state.ref.responseId,
            drain_token: state.token,
            played_ms: played,
            audible_tail_ms: tail,
          },
          signal,
        );
    });
  }
  private settle(key: string | undefined | null, result: p.Acknowledgment) {
    const pending = this.pending.get(key ?? "");
    if (pending) {
      pending.request.check(result);
      pending.result.resolve(result);
    }
  }
  private reject(key: string | null, error: RimeError) {
    this.pending.get(key ?? "")?.result.reject(error);
  }
  private fail(error: RimeError) {
    if (this.closed) return;
    this.failure = error;
    this.closed = true;
    this.lifetime.abort(error);
    this.created.reject(error);
    for (const pending of this.pending.values()) pending.result.reject(error);
    this.queued.set();
    void this.socket.close().then(() => this.forget());
  }
  private dispatch(event: p.ServerEvent) {
    const { payload, requestId: echo } = event;
    // Check echoed acknowledgments before changing response or item state.
    if (
      payload.kind === "session.updated" ||
      payload.kind === "item.created" ||
      payload.kind === "tool.result.created" ||
      payload.kind === "response.started"
    ) {
      const pending = this.pending.get(echo ?? "");
      if (
        pending &&
        !(payload.kind === "item.created" && pending.request.allowsItemCreated)
      )
        pending.request.check(payload);
      if (
        pending &&
        payload.kind === "tool.result.created" &&
        pending.target !== payload.callId
      )
        throw new RimeStreamError(
          "Tool result acknowledgment names another call",
        );
    }
    switch (payload.kind) {
      case "session.created":
        if (this.sessionId === null) {
          this.sessionId = payload.session.id;
          this.created.resolve(payload.session);
        }
        return;
      case "session.updated":
      case "tool.result.created":
        this.settle(echo, payload);
        return;
      case "item.created": {
        const pending = this.pending.get(echo ?? "");
        if (pending?.request.allowsItemCreated) pending.itemId = payload.itemId;
        else this.settle(echo, payload);
        return;
      }
      case "audio.cleared":
        for (const [key, pending] of this.pending)
          if (pending.request.kind === "input_audio_buffer.clear") {
            this.converter = null;
            this.settle(key, payload);
            break;
          }
        return;
      case "item.truncated":
        for (const [key, pending] of this.pending)
          if (
            pending.request.kind === "conversation.item.truncate" &&
            pending.target === payload.itemId &&
            payload.contentIndex === 0
          )
            this.settle(key, payload);
        return;
      case "input.ready":
        this.readyEpoch++;
        this.ready.set();
        break;
      case "speech.started":
        this.readyEpoch++;
        this.ready.clear();
        this.latest = null;
        for (const state of this.responses.values()) state.changed.set();
        break;
      case "response.started": {
        const ref = payload.response;
        if (this.responses.has(ref.responseId))
          throw new RimeStreamError("Duplicate response.created");
        this.responses.set(ref.responseId, new ResponseState(ref));
        for (const state of this.responses.values()) state.changed.set();
        this.latest = ref.responseId;
        this.readyEpoch++;
        this.ready.clear();
        while (this.responses.size > LIMIT) {
          const oldest = this.responses.values().next().value!;
          if (!oldest.done.isSet)
            throw new RimeResourceLimitError(
              "Too many retained active responses",
            );
          this.responses.delete(oldest.ref.responseId);
        }
        this.settle(echo, payload);
        break;
      }
      case "response.ended": {
        const state = this.wireResponse(payload.response.responseId);
        if (state.done.isSet) return;
        state.token = payload.drainToken;
        state.done.set();
        state.changed.set();
        for (const [key, pending] of this.pending)
          if (
            pending.request.kind === "response.cancel" &&
            pending.target === state.ref.responseId
          )
            this.settle(key, payload);
        const { drainToken, ...ended } = payload;
        this.publish(ended, event.eventId, echo);
        return;
      }
      case "tool.call": {
        const state = this.wireResponse(payload.call.responseId),
          callId = payload.call.callId;
        if (state.calls.has(callId)) return;
        for (const other of this.responses.values())
          if (other.calls.has(callId))
            throw new RimeStreamError(
              "Tool call ID reused by another response",
            );
        if (state.calls.size >= LIMIT)
          throw new RimeResourceLimitError(
            "Too many tool calls in one response",
          );
        state.calls.set(callId, null);
        break;
      }
      case "message.started": {
        const state = this.wireResponse(payload.output.response.responseId);
        if (state.outputs.size >= LIMIT)
          throw new RimeResourceLimitError("Too many messages in one response");
        state.outputs.add(outputKey(payload.output));
        break;
      }
      case "text.delta":
      case "text.done":
      case "audio.delta":
      case "audio.done":
        this.wireResponse(payload.output.response.responseId);
        break;
      case "error": {
        const fault = payload.error,
          error = new t.RimeRealtimeError(fault);
        if (NOT_READY.has(fault.code)) this.ready.clear();
        this.reject(fault.requestId, error);
        if (fault.code === "typed_turn_superseded" && fault.itemId)
          for (const [key, pending] of this.pending)
            if (pending.itemId === fault.itemId) this.reject(key, error);
        if (fault.scope === "session") {
          this.fail(error);
          return;
        }
        break;
      }
    }
    this.publish(payload, event.eventId, echo);
  }
  private wireResponse(id: string): ResponseState {
    const state = this.responses.get(id);
    if (!state)
      throw new RimeStreamError("Event refers to an unknown response");
    return state;
  }
  private publish(
    payload: t.SessionEvent["payload"],
    eventId = "",
    requestId: string | null = null,
  ) {
    if (this.queue.length >= 256) {
      const error = new RimeResourceLimitError(
        "Realtime event consumer is too slow",
      );
      this.fail(error);
      throw error;
    }
    this.queue.push(
      Object.freeze({
        sessionId: this.id(),
        eventId,
        requestId,
        payload: Object.freeze(payload),
      }),
    );
    this.queued.set();
  }
  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = (async () => {
        if (!this.closed) {
          this.fail(new RimeStreamError("Realtime session closed"));
          this.failure = null;
        }
        await this.socket.close();
        await Promise.allSettled([...this.tasks]);
      })();
    }
    await this.closing;
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
