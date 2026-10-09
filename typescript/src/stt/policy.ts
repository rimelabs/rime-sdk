/** Independent resource and transport limits for recognition. Durations are seconds. */
export const policy = {
  target: "stt.api.rime.ai:443",
  connectionTimeout: 10,
  acceptanceTimeout: 10,
  completionTimeout: 120,
  cleanupTimeout: 2,
  receiveBytes: 262144,
  transcriptBytes: 65536,
  queuedUpdates: 16,
};
