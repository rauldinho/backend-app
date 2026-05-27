const express = require('express');
const http    = require('http');
const { WebSocketServer } = require('ws');
const path    = require('path');

const app    = express();
const server = http.createServer(app);

// ─── Static client ────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ─── Health check (Render pings this) ─────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({
    status:  'ok',
    clients: wss.clients.size,
    history: messageHistory.length,
  });
});

// ─── In-memory state ──────────────────────────────────────────────────────────
const MAX_HISTORY = 50;     // keep last N messages (lost on cold start — expected)
const messageHistory = [];  // [{ clientId, username, text, timestamp }]

// ─── WebSocket server ─────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, path: '/ws' });

let nextClientId = 1;

wss.on('connection', (ws) => {
  ws.clientId   = nextClientId++;
  ws.username   = null;     // set on first message
  ws.isAlive    = true;     // for heartbeat

  // ── Heartbeat: pong from client resets the flag ──────────────────────────
  ws.on('pong', () => { ws.isAlive = true; });

  // ── First message must be a "join" to claim a username ───────────────────
  ws.on('message', (raw) => {
    let payload;
    try { payload = JSON.parse(raw.toString()); }
    catch { return; }

    // ── Join handshake ──────────────────────────────────────────────────────
    if (!ws.username) {
      if (payload.type !== 'join' || !payload.username?.trim()) {
        ws.send(JSON.stringify({ type: 'error', message: 'Send { type:"join", username:"<name>" } first.' }));
        return;
      }

      ws.username = sanitize(payload.username.trim()).slice(0, 24) || `User${ws.clientId}`;

      // Confirm to the joining client
      ws.send(JSON.stringify({
        type:     'welcome',
        clientId: ws.clientId,
        username: ws.username,
        history:  messageHistory,        // replay last N messages
        users:    onlineUsers(),
      }));

      // Announce to everyone else
      broadcast({
        type:    'user_joined',
        message: `${ws.username} joined the chat`,
        users:   onlineUsers(),
      }, ws);

      console.log(`[WS] ${ws.username} (#${ws.clientId}) joined  (online: ${wss.clients.size})`);
      return;
    }

    // ── Chat message ────────────────────────────────────────────────────────
    if (payload.type === 'message') {
      const text = sanitize(payload.text?.trim() ?? '');
      if (!text) return;

      const msg = {
        type:      'message',
        clientId:  ws.clientId,
        username:  ws.username,
        text,
        timestamp: new Date().toISOString(),
      };

      // Store in history (ring buffer)
      messageHistory.push(msg);
      if (messageHistory.length > MAX_HISTORY) messageHistory.shift();

      broadcast(msg);   // send to ALL including sender
      console.log(`[WS] <${ws.username}> ${text}`);
    }
  });

  // ── Disconnect ─────────────────────────────────────────────────────────────
  ws.on('close', () => {
    const name = ws.username ?? `Client #${ws.clientId}`;
    console.log(`[WS] ${name} left  (online: ${wss.clients.size})`);

    if (ws.username) {
      broadcast({
        type:    'user_left',
        message: `${ws.username} left the chat`,
        users:   onlineUsers(),
      });
    }
  });

  ws.on('error', (err) => {
    console.error(`[WS] Error on ${ws.username ?? ws.clientId}:`, err.message);
  });
});

// ─── Heartbeat interval ───────────────────────────────────────────────────────
// Render's proxy closes idle WS connections after ~100 s.
// Ping every 30 s; disconnect any client that missed a pong.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

wss.on('close', () => clearInterval(heartbeat));

// ─── Helpers ──────────────────────────────────────────────────────────────────
function broadcast(data, exclude = null) {
  const serialized = JSON.stringify(data);
  for (const client of wss.clients) {
    if (client !== exclude && client.readyState === 1 /* OPEN */) {
      client.send(serialized);
    }
  }
}

function onlineUsers() {
  return [...wss.clients]
    .filter(c => c.username && c.readyState === 1)
    .map(c => ({ clientId: c.clientId, username: c.username }));
}

function sanitize(str) {
  return str
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/&/g, '&amp;');
}

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT ?? 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat server listening on http://0.0.0.0:${PORT}`);
});
