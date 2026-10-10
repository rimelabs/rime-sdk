import { Player } from "./player.js";

const start = document.querySelector("#start");
const stop = document.querySelector("#stop");
const log = document.querySelector("#log");
let context, socket, microphone, capture, timer, player;
let generation = 0;
function message(text) {
  log.textContent += `${text}\n`;
}
async function close() {
  generation++;
  clearInterval(timer);
  capture?.disconnect();
  microphone?.getTracks().forEach((track) => track.stop());
  microphone = undefined;
  capture = undefined;
  socket?.close();
  socket = undefined;
  const closing = context;
  context = undefined;
  await closing?.close();
  start.disabled = false;
  stop.disabled = true;
}
stop.onclick = () => void close();
start.onclick = async () => {
  start.disabled = true;
  stop.disabled = false;
  const current = ++generation;
  try {
    context = new AudioContext({ sampleRate: 48000 });
    if (typeof context.getOutputTimestamp !== "function")
      throw new Error(
        "This example needs a browser with AudioContext.getOutputTimestamp",
      );
    await context.resume();
    if (current !== generation) return;
    await context.audioWorklet.addModule("/capture.js");
    if (current !== generation) return;
    socket = new WebSocket(`ws://${location.host}/audio`);
    const connection = socket;
    const send = (value) => {
      if (connection.readyState === WebSocket.OPEN)
        connection.send(JSON.stringify(value));
    };
    player = new Player(context, send);
    timer = setInterval(() => {
      try {
        player.poll();
      } catch (error) {
        message(String(error));
        void close();
      }
    }, 20);
    let mode;
    connection.onmessage = async (event) => {
      if (current !== generation) return;
      try {
        const value = JSON.parse(event.data);
        switch (value.type) {
          case "mode":
            mode = value.mode;
            break;
          case "ready":
            if (mode === "prism") {
              const stream = await navigator.mediaDevices.getUserMedia({
                audio: {
                  channelCount: 1,
                  echoCancellation: true,
                  noiseSuppression: true,
                },
              });
              if (current !== generation) {
                stream.getTracks().forEach((track) => track.stop());
                return;
              }
              microphone = stream;
              const source = context.createMediaStreamSource(stream);
              capture = new AudioWorkletNode(context, "capture");
              capture.port.onmessage = ({ data }) => {
                if (connection.readyState !== WebSocket.OPEN) return;
                if (connection.bufferedAmount > 384000) {
                  message("Microphone connection is too slow.");
                  void close();
                  return;
                }
                connection.send(data);
              };
              source.connect(capture);
              capture.connect(context.destination);
            }
            message(
              mode === "prism"
                ? "Ready. Speak now."
                : "Playing streamed speech. Press Stop to cancel.",
            );
            break;
          case "begin":
            player.begin(value.id);
            break;
          case "audio":
            player.write(
              value.id,
              Uint8Array.from(atob(value.data), (c) => c.charCodeAt(0)),
            );
            break;
          case "end":
            player.end(value.id);
            break;
          case "interrupt":
            player.interrupt();
            break;
          case "text":
            if (value.text) message(`${value.role}: ${value.text}`);
            break;
          case "error":
            message(value.message);
            break;
        }
      } catch (error) {
        message(String(error));
        await close();
      }
    };
    connection.onclose = () => {
      if (current === generation) {
        message("Session closed.");
        void close();
      }
    };
    connection.onerror = () => {
      if (current === generation) {
        message("Cannot connect to the local example server.");
        void close();
      }
    };
  } catch (error) {
    if (current === generation) {
      message(String(error));
      await close();
    }
  }
};
window.addEventListener("pagehide", () => void close());
