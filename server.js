const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  pingInterval: 10_000,  // ping every 10s  (default: 25s)
  pingTimeout:   5_000,  // disconnect if no pong in 5s (default: 20s)
});

// ─── Static client ────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ─── Health check (Render pings this) ─────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({
    status:  'ok',
    clients: io.engine.clientsCount,
    history: messageHistory.length,
  });
});

// ─── In-memory state ──────────────────────────────────────────────────────────
const MAX_HISTORY = 50;
const messageHistory = [];
let nextClientId = 1;

// ─── Connection logic ─────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.clientId = nextClientId++;
  socket.username = null;

  // ── Join handshake ──────────────────────────────────────────────────────────
  socket.on('join', ({ username } = {}) => {
    if (!username?.trim()) {
      socket.emit('error', { message: 'Send { type:"join", username:"<name>" } first.' });
      return;
    }

    socket.username = sanitize(username.trim()).slice(0, 24) || `User${socket.clientId}`;

    socket.emit('welcome', {
      clientId: socket.clientId,
      username: socket.username,
      history:  messageHistory,
      users:    onlineUsers(),
    });

    socket.broadcast.emit('user_joined', {
      message: `${socket.username} joined the chat`,
      users:   onlineUsers(),
    });

    console.log(`[IO] ${socket.username} (#${socket.clientId}) joined  (online: ${io.engine.clientsCount})`);
  });

  // ── Chat message ─────────────────────────────────────────────────────────────
  socket.on('message', ({ text } = {}) => {
    if (!socket.username) return;
    const sanitizedText = sanitize(text?.trim() ?? '');
    if (!sanitizedText) return;

    const msg = {
      clientId:  socket.clientId,
      username:  socket.username,
      text:      sanitizedText,
      timestamp: new Date().toISOString(),
    };

    messageHistory.push(msg);
    if (messageHistory.length > MAX_HISTORY) messageHistory.shift();

    io.emit('message', msg);
    console.log(`[IO] <${socket.username}> ${sanitizedText}`);
  });

  // ── Disconnect ───────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const name = socket.username ?? `Client #${socket.clientId}`;
    console.log(`[IO] ${name} left  (online: ${io.engine.clientsCount})`);
    if (socket.username) {
      io.emit('user_left', {
        message: `${socket.username} left the chat`,
        users:   onlineUsers(),
      });
    }
  });
});

// ─── Helpers ──────────────────────────────────────────────────────────────────
function onlineUsers() {
  const users = [];
  for (const [, socket] of io.of('/').sockets) {
    if (socket.username) {
      users.push({ clientId: socket.clientId, username: socket.username });
    }
  }
  return users;
}

function sanitize(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT ?? 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat server listening on http://0.0.0.0:${PORT}`);
});
