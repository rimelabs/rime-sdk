# Talk to Prism in your browser

A local page captures your microphone and plays Prism's replies. A small server
keeps your Rime API key and connects to Prism with the published Rime SDK. The
browser cancels echo, so laptop speakers work, and you can talk over Prism to
interrupt it.

This folder stands alone: it pins the published `rimelabs-sdk` and
`@rimelabs/sdk` packages instead of this repository's workspace, so readers can
copy it and run it anywhere. The
[Prism SDK quickstart](https://docs.rime.ai/docs/prism/quickstart) walks through it.

## Run it

Set your key, then start one of the servers:

```sh
export RIME_API_KEY="your-api-key"

uv run server.py                 # Python 3.11+, with uv
npm install && node server.mjs   # or Node.js 22+
```

Open http://localhost:3000 in Chrome, edit the instructions, choose a voice, and
press **Start**. `PORT` changes the port and `PRISM_URL` the Prism address.

## Without a microphone

`check.py` and `check.mjs` send `question.wav`, a recorded caller who asks when
the cafe closes, and save Prism's answer to `reply.wav`:

```sh
uv run check.py        # or: node check.mjs
```

## Files

| File | What it does |
| --- | --- |
| `public/` | The page. `player.js` reports how much of each reply played, from the audio output clock. |
| `server.py`, `server.mjs` | The servers. Each serves the page, holds one conversation at a time, and accepts only local connections. |
| `check.py`, `check.mjs` | The test without a microphone |
