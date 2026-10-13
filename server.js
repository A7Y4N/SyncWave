/**
 * SyncWave Universal Multi-Room Cloud Server
 *
 * Architecture:
 * - Multi-Room: Anyone can create a room or join with a 4-letter code (e.g. "BEAT").
 * - Multi-Source per Room:
 *     1. DJ Mode: Upload audio files (.mp3, .m4a, .wav, .flac) or queue tracks. The server decodes
 *        and streams the audio in timestamped chunks via ffmpeg/worker.
 *     2. Mac Live Capture: If hosted locally on Mac, captures system/Apple Music via BlackHole.
 *     3. Calibration Clicks: Precision metronome for instant acoustic alignment.
 * - Millisecond NTP Synchronization: Each device continually synchronizes its local clock to
 *   the room clock.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn, execFileSync } = require('child_process');
const { performance } = require('perf_hooks');
const { WebSocketServer } = require('ws');
const QRCode = require('qrcode');

const PORT = Number(process.env.PORT) || 8080;
const CAPTURE_DEVICE = process.env.CAPTURE_DEVICE || 'BlackHole 2ch';
const IS_MAC = os.platform() === 'darwin';

const RATE = 48000;
const BYTES_PER_FRAME = 4; // 16-bit stereo PCM
const CHUNK_FRAMES = 1920; // 40 ms chunks (resilient on mobile Wi-Fi)
const CHUNK_BYTES = CHUNK_FRAMES * BYTES_PER_FRAME;
const HEADER_BYTES = 16; // float64 frameIndex + float64 serverTimeMs

const now = () => performance.now();

// ------------------------------------------------------------- Room Management
class Room {
  constructor(code) {
    this.code = code;
    this.clients = new Map(); // id -> client
    this.nextId = 1;
    this.delayMs = 2000;
    this.source = IS_MAC ? 'music' : 'file'; // Default to live system audio on Mac!
    this.currentTrack = null; // { name, duration, url }
    this.queue = [];
    this.playbackState = 'stopped'; // 'playing' | 'paused' | 'stopped'
    this.playbackPosition = 0; // in seconds

    // Stream state
    this.totalFrames = 0;
    this.pending = Buffer.alloc(0);
    this.est = null;
    this.estCount = 0;
    this.activeProcess = null;
    this.clickTimer = null;
    this.heartbeatTimer = null;

    this.init();
  }

  init() {
    this.heartbeatTimer = setInterval(() => this.broadcastRoom(), 1000);
    if (this.source === 'music' && IS_MAC) {
      this.startLiveCapture();
    }
  }

  destroy() {
    this.stopSource();
    clearInterval(this.heartbeatTimer);
    for (const c of this.clients.values()) {
      try { c.ws.close(); } catch (e) {}
    }
    this.clients.clear();
  }

  resetTimeline() {
    this.pending = Buffer.alloc(0);
    this.est = null;
    this.estCount = 0;
  }

  onPcm(buf, arrival) {
    this.pending = this.pending.length ? Buffer.concat([this.pending, buf]) : buf;
    const framesEnd = this.totalFrames + Math.floor(this.pending.length / BYTES_PER_FRAME);
    const raw = arrival - (framesEnd * 1000) / RATE;

    if (this.est === null || Math.abs(raw - this.est) > 250) {
      this.est = raw;
      this.estCount = 1;
    } else {
      this.estCount++;
      const alpha = Math.max(0.002, 1 / this.estCount);
      this.est += (raw - this.est) * alpha;
    }

    while (this.pending.length >= CHUNK_BYTES) {
      const pcm = this.pending.subarray(0, CHUNK_BYTES);
      this.pending = this.pending.subarray(CHUNK_BYTES);
      this.broadcastChunk(this.totalFrames, this.est + (this.totalFrames * 1000) / RATE, pcm);
      this.totalFrames += CHUNK_FRAMES;
    }
  }

  broadcastChunk(frameIndex, serverTimeMs, pcm) {
    const msg = Buffer.allocUnsafe(HEADER_BYTES + pcm.length);
    msg.writeDoubleLE(frameIndex, 0);
    msg.writeDoubleLE(serverTimeMs, 8);
    pcm.copy(msg, HEADER_BYTES);
    for (const c of this.clients.values()) {
      if (c.ws.readyState !== 1 || c.ws.bufferedAmount > 2 * 1024 * 1024) continue;
      c.ws.send(msg, { binary: true });
    }
  }

  stopSource() {
    if (this.activeProcess) {
      const p = this.activeProcess;
      this.activeProcess = null;
      try { p.kill('SIGKILL'); } catch (e) {}
    }
    if (this.clickTimer) {
      clearInterval(this.clickTimer);
      this.clickTimer = null;
    }
  }

  startClicks() {
    this.stopSource();
    this.resetTimeline();
    const t0 = now();
    let produced = 0;
    const clickLen = Math.floor(RATE * 0.03);
    this.clickTimer = setInterval(() => {
      const target = Math.floor(((now() - t0) * RATE) / 1000);
      const n = target - produced;
      if (n <= 0) return;
      const buf = Buffer.alloc(n * BYTES_PER_FRAME);
      for (let i = 0; i < n; i++) {
        const idx = produced + i;
        const inBeat = idx % RATE;
        let v = 0;
        if (inBeat < clickLen) {
          const t = inBeat / RATE;
          const f = Math.floor(idx / RATE) % 4 === 0 ? 1760 : 880;
          v = Math.sin(2 * Math.PI * f * t) * Math.exp(-t * 140) * 0.85;
        }
        const s = Math.round(v * 32767);
        buf.writeInt16LE(s, i * 4);
        buf.writeInt16LE(s, i * 4 + 2);
      }
      produced = target;
      this.onPcm(buf, now());
    }, 10);
  }

  playFile(filePath, trackMeta) {
    this.stopSource();
    this.resetTimeline();
    this.currentTrack = trackMeta;
    this.playbackState = 'playing';

    // Pipe audio through ffmpeg decoded in real-time at 48kHz 16-bit stereo PCM
    const args = [
      '-hide_banner', '-loglevel', 'error',
      '-re', // Read input at native frame rate (realtime broadcast)
      '-i', filePath,
      '-ac', '2', '-ar', String(RATE),
      '-f', 's16le', '-'
    ];

    const proc = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    this.activeProcess = proc;
    proc.stdout.on('data', (d) => {
      if (this.activeProcess === proc) this.onPcm(d, now());
    });
    proc.stderr.on('data', (d) => console.error(`[room:${this.code}:ffmpeg]`, d.toString().trim()));
    proc.on('exit', () => {
      if (this.activeProcess === proc) {
        this.activeProcess = null;
        this.playbackState = 'stopped';
        // Auto play next in queue
        if (this.queue.length > 0) {
          const next = this.queue.shift();
          this.playFile(next.path, next);
        }
        this.broadcastState();
      }
    });

    this.broadcastState();
  }

  startLiveCapture() {
    this.stopSource();
    this.resetTimeline();
    if (IS_MAC) {
      try {
        execFileSync('SwitchAudioSource', ['-s', CAPTURE_DEVICE, '-t', 'output'], { stdio: 'ignore' });
        console.log(`[route] Switched Mac output -> "${CAPTURE_DEVICE}"`);
      } catch (e) {
        console.warn('[route] Could not set output to BlackHole:', e.message);
      }
    }
    const CAPTURE_BIN = path.join(__dirname, 'bin', 'capture');
    const [cmd, args] = (fs.existsSync(CAPTURE_BIN) && IS_MAC)
      ? [CAPTURE_BIN, [CAPTURE_DEVICE]]
      : ['ffmpeg', ['-hide_banner', '-loglevel', 'error', '-f', 'avfoundation', '-i', `:${CAPTURE_DEVICE}`,
          '-ac', '2', '-ar', String(RATE), '-f', 's16le', '-']];

    const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    this.activeProcess = proc;
    proc.stdout.on('data', (d) => {
      if (this.activeProcess === proc) this.onPcm(d, now());
    });
    proc.stderr.on('data', (d) => console.error(`[room:${this.code}:capture]`, d.toString().trim()));
    proc.on('exit', () => {
      if (this.activeProcess === proc) {
        this.activeProcess = null;
        setTimeout(() => { if (this.source === 'music') this.startLiveCapture(); }, 1000);
      }
    });
  }

  publicState() {
    return {
      room: this.code,
      delayMs: this.delayMs,
      source: this.source,
      currentTrack: this.currentTrack,
      playbackState: this.playbackState,
      queueLength: this.queue.length,
      hasMacCapture: IS_MAC,
    };
  }

  roomSnapshot() {
    return [...this.clients.values()]
      .filter((c) => c.joined)
      .map((c) => ({
        id: c.id,
        name: c.name,
        role: c.role,
        offsetMs: c.offsetMs,
        volume: c.volume,
        platform: c.platform,
        isDJ: c.isDJ,
        stats: c.stats
      }));
  }

  broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const c of this.clients.values()) {
      if (c.ws.readyState === 1) c.ws.send(s);
    }
  }

  broadcastState() { this.broadcast({ type: 'state', state: this.publicState() }); }
  broadcastRoom() { this.broadcast({ type: 'room', room: this.roomSnapshot() }); }
}

const rooms = new Map();

function getOrCreateRoom(code) {
  const cleanCode = (code || 'MAIN').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'MAIN';
  let r = rooms.get(cleanCode);
  if (!r) {
    r = new Room(cleanCode);
    rooms.set(cleanCode, r);
    console.log(`[rooms] Created room ${cleanCode}`);
  }
  return r;
}

// ------------------------------------------------------------- Static & Upload HTTP
const UPLOADS_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

const PUBLIC = path.join(__dirname, 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.mp4': 'video/mp4',
  '.json': 'application/json',
};

const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);

  // QR Code generator endpoint
  if (url === '/api/qr') {
    const targetUrl = new URL(req.url, `http://${req.headers.host}`).searchParams.get('url') || '';
    QRCode.toBuffer(targetUrl, { width: 300, margin: 2, color: { dark: '#f3f1ff', light: '#141226' } }, (err, buf) => {
      if (err) { res.writeHead(500).end(); return; }
      res.writeHead(200, { 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400' });
      res.end(buf);
    });
    return;
  }

  // Audio Upload endpoint for DJ mode
  if (req.method === 'POST' && url === '/api/upload') {
    const roomCode = req.headers['x-room-code'] || 'MAIN';
    const filename = decodeURIComponent(req.headers['x-file-name'] || `track_${Date.now()}.mp3`);
    const cleanFilename = `${Date.now()}_${filename.replace(/[^a-zA-Z0-9._-]/g, '_')}`;
    const targetPath = path.join(UPLOADS_DIR, cleanFilename);
    const writeStream = fs.createWriteStream(targetPath);

    req.pipe(writeStream);
    req.on('end', () => {
      const room = getOrCreateRoom(roomCode);
      const trackMeta = { name: filename, path: targetPath, addedAt: Date.now() };
      room.source = 'file';
      room.playFile(targetPath, trackMeta);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, track: trackMeta }));
    });
    req.on('error', (err) => {
      console.error('[upload]', err);
      res.writeHead(500).end('Upload error');
    });
    return;
  }

  // Serve static UI
  const file = path.normalize(path.join(PUBLIC, url === '/' ? 'index.html' : url));
  if (!file.startsWith(PUBLIC)) {
    res.writeHead(403).end();
    return;
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404).end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store',
    });
    res.end(data);
  });
});

// ------------------------------------------------------------- WebSocket Realtime
const wss = new WebSocketServer({ server, path: '/ws', perMessageDeflate: false });

wss.on('connection', (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const requestedRoom = (urlObj.searchParams.get('room') || 'MAIN').toUpperCase();
  const room = getOrCreateRoom(requestedRoom);

  const id = room.nextId++;
  const client = {
    id,
    ws,
    name: 'Device',
    role: 'stereo',
    offsetMs: 0,
    volume: 1,
    platform: '',
    isDJ: false,
    joined: false,
    stats: {}
  };
  room.clients.set(id, client);

  ws.send(JSON.stringify({ type: 'welcome', id, room: room.code, state: room.publicState() }));

  ws.on('message', (data, isBinary) => {
    const recv = now();
    if (isBinary) {
      // Live audio broadcast from client browser (tab / screen audio)
      if (data.length > 4 && data[0] === 0x53 && data[1] === 0x57 && data[2] === 0x50 && data[3] === 0x43) {
        if (room.source !== 'stream') {
          room.stopSource();
          room.resetTimeline();
          room.source = 'stream';
          room.playbackState = 'playing';
          room.broadcastState();
        }
        room.onPcm(data.subarray(4), recv);
      }
      return;
    }
    let m;
    try { m = JSON.parse(data); } catch { return; }

    switch (m.type) {
      case 'ping':
        ws.send(JSON.stringify({ type: 'pong', c: m.c, s: recv, s2: now() }));
        break;

      case 'hello':
        client.name = String(m.name || 'Device').slice(0, 32);
        client.role = ['stereo', 'left', 'right', 'mono'].includes(m.role) ? m.role : 'stereo';
        client.offsetMs = Math.min(500, Math.max(-500, Number(m.offsetMs) || 0));
        client.volume = Math.min(2, Math.max(0, Number(m.volume) || 1));
        client.platform = String(m.platform || '').slice(0, 16);
        client.isDJ = Boolean(m.isDJ);
        client.joined = true;
        console.log(`[room:${room.code}] ${client.name} joined as ${client.role} (${client.platform})`);
        room.broadcastRoom();
        break;

      case 'stats':
        client.stats = m.stats || {};
        break;

      case 'setGlobal':
        if (m.delayMs != null) room.delayMs = Math.min(5000, Math.max(300, Math.round(Number(m.delayMs))));
        if (m.source && m.source !== room.source) {
          room.source = m.source;
          if (m.source === 'clicks') room.startClicks();
          else if (m.source === 'music' && IS_MAC) room.startLiveCapture();
          else room.stopSource();
        }
        room.broadcastState();
        break;

      case 'setDevice': {
        const t = room.clients.get(m.id);
        if (!t) break;
        const patch = {};
        if (m.role) patch.role = t.role = m.role;
        if (m.offsetMs != null) patch.offsetMs = t.offsetMs = Math.min(500, Math.max(-500, Number(m.offsetMs)));
        if (m.volume != null) patch.volume = t.volume = Math.min(2, Math.max(0, Number(m.volume)));
        if (m.name) patch.name = t.name = String(m.name).slice(0, 32);
        if (t !== client) t.ws.send(JSON.stringify({ type: 'apply', ...patch }));
        room.broadcastRoom();
        break;
      }
    }
  });

  ws.on('close', () => {
    room.clients.delete(id);
    room.broadcastRoom();
    // Clean up empty non-main rooms after 10 minutes
    if (room.clients.size === 0 && room.code !== 'MAIN') {
      setTimeout(() => {
        if (room.clients.size === 0) {
          room.destroy();
          rooms.delete(room.code);
          console.log(`[rooms] Purged empty room ${room.code}`);
        }
      }, 600000);
    }
  });
});

// Boot server
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n  =========================================`);
  console.log(`  🎵 SyncWave Universal Multi-Room Cloud`);
  console.log(`  =========================================`);
  console.log(`  Port:      ${PORT}`);
  console.log(`  Local:     http://localhost:${PORT}`);
  console.log(`  Platform:  ${os.platform()}`);
  console.log(`  =========================================\n`);
});

// Clean exit handler: restore Mac speakers
function restoreMacSpeakers() {
  if (IS_MAC) {
    try {
      execFileSync('SwitchAudioSource', ['-s', 'MacBook Air Speakers', '-t', 'output'], { stdio: 'ignore' });
      console.log('\n[route] Restored Mac output -> "MacBook Air Speakers"');
    } catch {}
  }
}
process.on('SIGINT', () => { restoreMacSpeakers(); process.exit(0); });
process.on('SIGTERM', () => { restoreMacSpeakers(); process.exit(0); });
process.on('exit', () => restoreMacSpeakers());
