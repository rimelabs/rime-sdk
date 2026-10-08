/** Schedule PCM on the audio clock, and report only samples that reached output. */
export class Player {
  constructor(context, report) {
    this.context = context;
    this.report = report;
    this.replies = new Map();
    this.tail = 0;
  }
  begin(id) {
    this.replies.set(id, { runs: [], ended: false, reported: false });
  }
  write(id, bytes) {
    const reply = this.replies.get(id);
    if (!reply || reply.reported || reply.interrupted) return;
    const context = this.context;
    const frames = bytes.length / 2;
    if (Math.max(0, this.tail - context.currentTime) + frames / 24000 > 30)
      throw new Error("Playback queue exceeded 30 seconds");
    const buffer = context.createBuffer(1, frames, 24000);
    const values = buffer.getChannelData(0);
    const pcm = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let i = 0; i < frames; i++)
      values[i] = pcm.getInt16(i * 2, true) / 32768;
    const source = context.createBufferSource();
    source.buffer = buffer;
    source.connect(context.destination);
    const start = Math.max(this.tail, context.currentTime + 0.02);
    this.tail = start + buffer.duration;
    reply.runs.push({ source, start, duration: buffer.duration });
    source.start(start);
  }
  end(id) {
    const reply = this.replies.get(id);
    if (reply) {
      reply.ended = true;
      if (reply.reported) this.replies.delete(id);
    }
  }
  outputTime() {
    return this.context.getOutputTimestamp().contextTime;
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
        (!last || this.outputTime() >= last.start + last.duration)
      )
        this.finish(id, reply, false);
    }
  }
  interrupt() {
    for (const reply of this.replies.values()) {
      if (reply.reported || reply.interrupted) continue;
      reply.interrupted = true;
      // Samples already rendered into the device buffer can still be heard.
      reply.cutoff = this.context.currentTime;
      for (const run of reply.runs) run.source.stop(reply.cutoff);
    }
    this.tail = this.context.currentTime;
  }
}
