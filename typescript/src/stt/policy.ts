/** Independent resource and transport limits for recognition. Durations are seconds. */
export const policy = {
  target: "stt.api.rime.ai:443",
  connectionTimeout: 10,
  acceptanceTimeout: 10,
  completionTimeout: 120,
  cleanupTimeout: 2,
  receiveBytes: 1048576 + 65536,
  transcriptBytes: 1048576,
  queuedUpdates: 16,
};
