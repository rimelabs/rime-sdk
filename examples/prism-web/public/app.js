import { Player } from "./player.js";

const start = document.querySelector("#start");
const stop = document.querySelector("#stop");
const instructions = document.querySelector("#instructions");
const voice = document.querySelector("#voice");
const log = document.querySelector("#log");
let context, socket, microphone, capture, timer, player;
let generation = 0;

function message(text) {
  log.textContent += `${text}\n`;
  log.scrollTop = log.scrollHeight;
}

async function close() {
  generation++;
  clearInterval(timer);
  capture?.disconnect();
  microphone?.getTracks().forEach((track) => track.stop());
  microphone = capture = undefined;
  socket?.close();
  socket = undefined;
  const closing = context;
  context = undefined;
  await closing?.close();
  start.disabled = instructions.disabled = voice.disabled = false;
  stop.disabled = true;
}

// The browser cancels echo, so Prism does not hear itself through laptop speakers.
async function startMicrophone(connection, current) {
  const stream = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: 1,
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
  });
  if (current !== generation)
    return stream.getTracks().forEach((track) => track.stop());
  microphone = stream;
  const source = context.createMediaStreamSource(stream);
  capture = new AudioWorkletNode(context, "capture");
  capture.port.onmessage = ({ data }) => {
    if (connection.readyState !== WebSocket.OPEN) return;
    if (connection.bufferedAmount > 384000) {
      message("The connection to the local server is too slow.");
      return void close();
    }
    connection.send(data); // 40 ms of 48 kHz mono PCM16
  };
  source.connect(capture);
  capture.connect(context.destination);
}

stop.onclick = () => void close();
start.onclick = async () => {
  start.disabled = instructions.disabled = voice.disabled = true;
  stop.disabled = false;
  log.textContent = "";
  const current = ++generation;
  try {
    context = new AudioContext({ sampleRate: 48000 });
    // The player measures how much of each reply you heard from the output clock.
    if (typeof context.getOutputTimestamp !== "function")
      throw new Error(
        "This browser cannot measure audio playback. Use a current version of Chrome, Edge, Firefox, or Safari.",
      );
    await context.resume();
    await context.audioWorklet.addModule("/capture.js");
    if (current !== generation) return;
    const connection = (socket = new WebSocket(`ws://${location.host}/audio`));
    const send = (value) => {
      if (connection.readyState === WebSocket.OPEN)
        connection.send(JSON.stringify(value));
    };
    player = new Player(context, send);
    timer = setInterval(() => player.poll(), 20);
    message("Connecting to Prism...");
    connection.onopen = () =>
      send({
        type: "start",
        instructions: instructions.value,
        voice: voice.value,
      });
    connection.onmessage = (event) =>
      handle(JSON.parse(event.data)).catch(async (error) => {
        message(String(error));
        await close();
      });
    async function handle(value) {
      if (current !== generation) return;
      switch (value.type) {
        case "ready":
          await startMicrophone(connection, current);
          message("Ready. Start talking.");
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
          player.interrupt(value.ids);
          break;
        case "stop":
          player.stop(value.id);
          break;
        case "text":
          if (value.text) message(`${value.role}: ${value.text}`);
          break;
        case "error":
          message(value.message);
          break;
      }
    }
    connection.onclose = () => {
      if (current === generation) {
        message("Conversation closed.");
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
