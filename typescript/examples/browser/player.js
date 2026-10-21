/** Schedule PCM on the audio clock, and report only samples that reached output. */
export class Player {
  constructor(context, report) {
    this.context = context;
    this.report = report;
    this.replies = new Map();
    this.tail = 0;
    this.queuedSeconds = 0;
    this.stopping = undefined;
  }
  begin(id) {
    if (this.stopping) throw new Error("Playback shutdown is pending");
    this.replies.set(id, {
      runs: [],
      queued: [],
      ended: false,
      reported: false,
    });
  }
  write(id, bytes) {
    const reply = this.replies.get(id);
    if (!reply || reply.reported || reply.interrupted) return;
    if (reply.ended) throw new Error("Audio arrived after playback was sealed");
    const context = this.context;
    const frames = bytes.length / 2;
    if (
      Math.max(0, this.tail - context.currentTime) +
        this.queuedSeconds +
        frames / 24000 >
      30
    )
      throw new Error("Playback queue exceeded 30 seconds");
    const buffer = context.createBuffer(1, frames, 24000);
    const values = buffer.getChannelData(0);
    const pcm = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let i = 0; i < frames; i++)
      values[i] = pcm.getInt16(i * 2, true) / 32768;
    reply.queued.push(buffer);
    this.queuedSeconds += buffer.duration;
    this.schedule();
  }
  schedule() {
    const context = this.context;
    // Later outputs can arrive before earlier ones finish generating. Hold
    // their audio until the earlier output is sealed, then schedule at its tail.
    for (const reply of this.replies.values()) {
      if (reply.reported || reply.interrupted) continue;
      for (const buffer of reply.queued) {
        const source = context.createBufferSource();
        source.buffer = buffer;
        source.connect(context.destination);
        const start = Math.max(this.tail, context.currentTime + 0.02);
        this.tail = start + buffer.duration;
        reply.runs.push({ source, start, duration: buffer.duration });
        source.start(start);
        this.queuedSeconds = Math.max(0, this.queuedSeconds - buffer.duration);
      }
      reply.queued = [];
      if (!reply.ended) break;
    }
  }
  end(id) {
    const reply = this.replies.get(id);
    if (reply) {
      reply.ended = true;
      if (reply.reported) this.replies.delete(id);
      this.schedule();
    }
  }
  outputTime() {
    const value = this.context.getOutputTimestamp().contextTime;
    if (!Number.isFinite(value) || value < 0)
      throw new Error("Invalid playback clock");
    return value;
  }
  finish(id, reply, interrupted) {
    if (reply.reported) return;
    const now = reply.cutoff ?? this.outputTime();
    const seconds = reply.runs.reduce(
      (total, run) =>
        total + Math.max(0, Math.min(run.duration, now - run.start)),
      0,
    );
    reply.reported = true;
    this.report({ type: "played", id, interrupted, playedMs: seconds * 1000 });
    if (reply.ended) this.replies.delete(id);
  }
  poll() {
    for (const [id, reply] of this.replies) {
      const last = reply.runs.at(-1);
      if (reply.interrupted && this.outputTime() >= reply.cutoff)
        this.finish(id, reply, true);
      else if (
        !reply.interrupted &&
        reply.ended &&
        !reply.queued.length &&
        (!last || this.outputTime() >= last.start + last.duration)
      )
        this.finish(id, reply, false);
    }
    if (this.stopping && this.outputTime() >= this.stopping.cutoff) {
      this.report({ type: "stopped", id: this.stopping.id });
      this.stopping = undefined;
    }
  }
  stop(id) {
    this.interrupt();
    this.stopping = { id, cutoff: this.context.currentTime };
    for (const key of this.replies.keys()) this.end(key);
    this.poll();
  }
  interrupt(ids) {
    const selected = ids === undefined ? undefined : new Set(ids);
    for (const [key, reply] of this.replies) {
      if (selected && !selected.has(key)) continue;
      if (reply.reported || reply.interrupted) continue;
      reply.interrupted = true;
      for (const buffer of reply.queued)
        this.queuedSeconds = Math.max(0, this.queuedSeconds - buffer.duration);
      reply.queued = [];
      // Samples already rendered into the device buffer can still be heard.
      reply.cutoff = this.context.currentTime;
      for (const run of reply.runs) run.source.stop(reply.cutoff);
    }
    this.tail = this.context.currentTime;
    for (const reply of this.replies.values()) {
      const last = reply.runs.at(-1);
      if (last && !reply.interrupted && !reply.reported)
        this.tail = Math.max(this.tail, last.start + last.duration);
    }
    this.schedule();
  }
}
