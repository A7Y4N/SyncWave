/* SyncWave client */
(() => {
  'use strict';

  const STREAM_RATE = 48000;
  const $ = (s) => document.querySelector(s);
  const $$ = (s) => [...document.querySelectorAll(s)];

  // ------------------------------------------------------------- platform
  const ua = navigator.userAgent;
  const isIOS = /iPad|iPhone|iPod/.test(ua) || (/Macintosh/.test(ua) && navigator.maxTouchPoints > 1);
  const isMac = !isIOS && /Macintosh/.test(ua);
  const platform = /iPhone|iPod/.test(ua) ? 'iphone' : isIOS ? 'ipad' : isMac ? 'mac' : 'other';
  const defaultName = { iphone: 'iPhone', ipad: 'iPad', mac: 'MacBook', other: 'Speaker' }[platform];

  const store = {
    get(k, d) {
      try {
        const v = localStorage.getItem('sw.' + k);
        return v == null ? d : JSON.parse(v);
      } catch {
        return d;
      }
    },
    set(k, v) {
      try {
        localStorage.setItem('sw.' + k, JSON.stringify(v));
      } catch {}
    },
  };

  // Read room code from URL query (?room=CODE) or stored preference
  const urlParams = new URLSearchParams(window.location.search);
  let activeRoom = (urlParams.get('room') || store.get('room', 'MAIN')).toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'MAIN';

  const settings = {
    room: activeRoom,
    name: store.get('name', defaultName),
    role: store.get('role', 'stereo'),
    offsetMs: store.get('offsetMs', 0),
    volume: store.get('volume', 1),
    sinkId: store.get('sinkId', ''),
  };
  const saveSettings = () => Object.entries(settings).forEach(([k, v]) => store.set(k, v));

  // ------------------------------------------------------------- state
  let ws = null;
  let myId = null;
  let joined = false;
  let group = { room: activeRoom, delayMs: 2000, source: 'file', currentTrack: null, playbackState: 'stopped' };
  let room = [];
  let anchor = null; // { n, s } latest frame index <-> server time
  let ctx = null;
  let node = null;
  let analyser = null;
  let gain = null;
  let wstats = null; // worklet stats
  let mapPrev = null;

  // ------------------------------------------------------------- clock sync
  const clock = {
    samples: [],
    offset: null, // serverTime - performance.now()
    rtt: null,
    jitter: 0,
    reset() {
      this.samples = [];
      this.offset = null;
      this.rtt = null;
    },
    ping() {
      if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'ping', c: performance.now() }));
    },
    onPong(m) {
      const t3 = performance.now();
      const proc = (m.s2 || m.s) - m.s;
      const rtt = t3 - m.c - proc;
      const off = m.s - (m.c + rtt / 2);
      this.samples.push({ rtt, off });
      if (this.samples.length > 60) this.samples.shift();
      const sorted = [...this.samples].sort((a, b) => a.rtt - b.rtt);
      const best = sorted.slice(0, Math.max(3, Math.ceil(sorted.length * 0.25)));
      const offs = best.map((s) => s.off).sort((a, b) => a - b);
      const med = offs[offs.length >> 1];
      this.rtt = best[0].rtt;
      this.jitter = offs[offs.length - 1] - offs[0];
      if (this.offset == null || Math.abs(med - this.offset) > 8) this.offset = med;
      else this.offset += (med - this.offset) * 0.2;
    },
  };

  let pingTimer = null;
  function startPinging() {
    clearInterval(pingTimer);
    let burst = 0;
    pingTimer = setInterval(() => {
      clock.ping();
      if (++burst === 30) {
        clearInterval(pingTimer);
        pingTimer = setInterval(() => clock.ping(), 500);
      }
    }, 60);
  }

  // ------------------------------------------------------------- websocket
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws?room=${encodeURIComponent(settings.room)}`);
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      setConn('ok', 'Connected');
      clock.reset();
      startPinging();
      if (isWorklet && node && node.port) node.port.postMessage({ type: 'reset' });
      else resetFallback();
      mapPrev = null;
      anchor = null;
      if (joined) sendHello();
    };
    ws.onmessage = (e) => {
      if (typeof e.data !== 'string') return onChunk(e.data);
      const m = JSON.parse(e.data);
      switch (m.type) {
        case 'pong':
          clock.onPong(m);
          break;
        case 'welcome':
          myId = m.id;
          settings.room = m.room || settings.room;
          saveSettings();
          updateRoomPill();
          setGroup(m.state);
          break;
        case 'state':
          setGroup(m.state);
          break;
        case 'room':
          room = m.room;
          renderRoom();
          break;
        case 'apply':
          for (const k of ['role', 'offsetMs', 'volume', 'name']) if (m[k] != null) settings[k] = m[k];
          saveSettings();
          applyLocal();
          syncControls();
          break;
      }
    };
    ws.onclose = () => {
      setConn('bad', 'Reconnecting…');
      clearInterval(pingTimer);
      setTimeout(connect, 1000);
    };
  }

  const send = (o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));
  const sendHello = () =>
    send({ type: 'hello', name: settings.name, role: settings.role, offsetMs: settings.offsetMs, volume: settings.volume, platform });
  const sendSelf = (patch) => myId != null && send({ type: 'setDevice', id: myId, ...patch });

  function onChunk(buf) {
    const dv = new DataView(buf);
    anchor = { n: dv.getFloat64(0, true), s: dv.getFloat64(8, true) };
    if (isWorklet && node && node.port) {
      node.port.postMessage({ type: 'pcm', n: anchor.n, buf }, [buf]);
    } else {
      writeFallback(anchor.n, buf);
    }
  }

  // Ring buffer and playback state for ScriptProcessor fallback (when AudioWorklet is unavailable)
  const FALLBACK_SIZE = 1 << 19; // ~10.9 s
  const FALLBACK_MASK = FALLBACK_SIZE - 1;
  const fbL = new Float32Array(FALLBACK_SIZE);
  const fbR = new Float32Array(FALLBACK_SIZE);
  let fbFirst = -1;
  let fbWritten = -1;
  let fbPos = null;
  let fbRatio = 1;
  let fbErr = 0;
  let fbUnderruns = 0;
  let fbSeeks = 0;
  let fbPlaying = false;
  let fbPeakL = 0;
  let fbPeakR = 0;
  let fbMap = null;
  let isWorklet = false;

  function resetFallback() {
    fbL.fill(0);
    fbR.fill(0);
    fbFirst = fbWritten = -1;
    fbPos = null;
    fbRatio = 1;
    fbErr = 0;
    fbPlaying = false;
  }

  function writeFallback(n, buf) {
    const pcm = new Int16Array(buf, 16);
    const frames = pcm.length >> 1;
    if (fbWritten < 0 || n + frames < fbWritten - FALLBACK_SIZE || n > fbWritten + FALLBACK_SIZE) {
      if (fbWritten >= 0) { fbL.fill(0); fbR.fill(0); }
      fbFirst = n;
      fbWritten = n;
    } else if (n > fbWritten) {
      for (let k = fbWritten; k < n; k++) {
        const i = k & FALLBACK_MASK;
        fbL[i] = 0;
        fbR[i] = 0;
      }
    }
    for (let k = 0; k < frames; k++) {
      const i = (n + k) & FALLBACK_MASK;
      fbL[i] = pcm[2 * k] / 32768;
      fbR[i] = pcm[2 * k + 1] / 32768;
    }
    if (n + frames > fbWritten) fbWritten = n + frames;
  }

  function herm(x0, x1, x2, x3, t) {
    const c1 = 0.5 * (x2 - x0);
    const c2 = x0 - 2.5 * x1 + 2 * x2 - 0.5 * x3;
    const c3 = 0.5 * (x3 - x0) + 1.5 * (x1 - x2);
    return ((c3 * t + c2) * t + c1) * t + x1;
  }

  function processFallback(e) {
    const oL = e.outputBuffer.getChannelData(0);
    const oR = e.outputBuffer.numberOfChannels > 1 ? e.outputBuffer.getChannelData(1) : oL;
    const N = oL.length;

    if (!fbMap || fbWritten < 0) {
      oL.fill(0);
      oR.fill(0);
      return;
    }

    const SR = STREAM_RATE;
    const target = fbMap.n0 + (ctx.currentTime - fbMap.c0) * SR;
    // Airfoil-style jitter tolerance: 120ms window before seeking, so Wi-Fi packet clumps never cause glitches
    const SEEK_THRESH = SR * 0.12;
    if (fbPos === null || Math.abs(target - fbPos) > SEEK_THRESH) {
      if (fbPos !== null && fbPlaying) fbSeeks++;
      fbPos = target;
      fbRatio = 1;
    }
    const errFrames = target - fbPos;
    fbErr = (errFrames / SR) * 1000;
    // Dynamic rate adjustment: smoothly catch up or slow down up to ±1.5% without audibly glitching
    const MAX_ADJ = 0.015;
    const want = 1 + Math.max(-MAX_ADJ, Math.min(MAX_ADJ, (errFrames / SR) * 0.4));
    fbRatio += (want - fbRatio) * 0.05;
    const step = (fbRatio * SR) / ctx.sampleRate;

    const lo = Math.max(fbFirst, fbWritten - FALLBACK_SIZE + 1024) + 1;
    const hi = fbWritten - 3;
    let pos = fbPos;
    let pkL = fbPeakL;
    let pkR = fbPeakR;
    let starved = false;
    let audible = false;
    const role = settings.role;

    for (let i = 0; i < N; i++) {
      const ip = Math.floor(pos);
      if (ip < lo || ip > hi) {
        oL[i] = 0;
        oR[i] = 0;
        if (ip > hi) starved = true;
      } else {
        audible = true;
        const t = pos - ip;
        const a = ip & FALLBACK_MASK, b = (ip + 1) & FALLBACK_MASK;
        // Fast, clean linear interpolation (imperceptible difference at 48kHz, 4x lower CPU)
        const l = fbL[a] + (fbL[b] - fbL[a]) * t;
        const r = fbR[a] + (fbR[b] - fbR[a]) * t;
        let outL, outR;
        if (role === 'left') outL = outR = l;
        else if (role === 'right') outL = outR = r;
        else if (role === 'mono') outL = outR = (l + r) * 0.5;
        else { outL = l; outR = r; }
        oL[i] = outL;
        oR[i] = outR;
        const al = Math.abs(outL);
        const ar = Math.abs(outR);
        if (al > pkL) pkL = al;
        if (ar > pkR) pkR = ar;
      }
      pos += step;
    }
    fbPos = pos;
    if (audible) fbPlaying = true;
    if (starved && fbPlaying) fbUnderruns++;
    fbPeakL = pkL * 0.97;
    fbPeakR = pkR * 0.97;

    wstats = {
      err: fbErr,
      ratio: fbRatio,
      aheadMs: ((fbWritten - fbPos) / SR) * 1000,
      underruns: fbUnderruns,
      seeks: fbSeeks,
      playing: fbPlaying,
      peakL: pkL,
      peakR: pkR,
    };
  }

  // ------------------------------------------------------------- audio
  async function startAudio() {
    const AC = window.AudioContext || window.webkitAudioContext;
    try {
      ctx = new AC({ sampleRate: STREAM_RATE, latencyHint: 'interactive' });
    } catch {
      ctx = new AC({ latencyHint: 'interactive' });
    }
    try {
      if (navigator.audioSession) navigator.audioSession.type = 'playback';
    } catch {}
    unlockIOSPlayback();

    // Check if AudioWorklet is supported in this context (e.g. Chrome, HTTPS, localhost)
    if (ctx.audioWorklet && typeof ctx.audioWorklet.addModule === 'function') {
      try {
        await ctx.audioWorklet.addModule('worklet.js');
        node = new AudioWorkletNode(ctx, 'sync-player', { numberOfInputs: 0, numberOfOutputs: 1, outputChannelCount: [2] });
        node.port.onmessage = (e) => (wstats = e.data);
        isWorklet = true;
      } catch (err) {
        console.warn('AudioWorklet failed, using ScriptProcessor fallback:', err);
        isWorklet = false;
      }
    } else {
      console.log('AudioWorklet not available in this context (Safari HTTP), using ScriptProcessor fallback');
      isWorklet = false;
    }

    if (!isWorklet) {
      resetFallback();
      // Buffer size 2048 gives Safari on iPadOS enough breathing room on the main JS thread
      node = ctx.createScriptProcessor(2048, 0, 2);
      node.onaudioprocess = processFallback;
    }

    analyser = ctx.createAnalyser();
    analyser.fftSize = 512;
    analyser.smoothingTimeConstant = 0.78;
    gain = ctx.createGain();
    node.connect(analyser).connect(gain).connect(ctx.destination);

    if (isMac) await applySink(settings.sinkId);
    applyLocal();
    await ctx.resume();
    ctx.onstatechange = updateResume;
    setInterval(updateMap, 100);
  }

  // Map "which stream frame must be heard at AudioContext time c".
  function updateMap() {
    if (!ctx || !node || !anchor || clock.offset == null || ctx.state !== 'running') return;
    const cNow = ctx.currentTime;
    const pNow = performance.now();

    // Hardware output latency (typically 10-25ms for internal DAC/speakers)
    const outLatencyMs = ((ctx.outputLatency || 0) + (ctx.baseLatency || 0.012)) * 1000;
    // ScriptProcessor processes one buffer ahead (2048 frames = ~42.6ms at 48kHz)
    const scriptProcLatencyMs = (!isWorklet ? (2048 / ctx.sampleRate) * 1000 : 0);
    const totalLatencyMs = outLatencyMs + scriptProcLatencyMs;

    // Time on server when the sample being generated right now will actually hit physical air:
    const heardServer = pNow + totalLatencyMs + clock.offset;
    let n0 = anchor.n + ((heardServer - group.delayMs - settings.offsetMs - anchor.s) * STREAM_RATE) / 1000;
    if (mapPrev) {
      const proj = mapPrev.n0 + (cNow - mapPrev.c0) * STREAM_RATE;
      const d = n0 - proj;
      if (Math.abs(d) < STREAM_RATE * 0.02) n0 = proj + d * 0.15;
    }
    mapPrev = { c0: cNow, n0 };
    if (isWorklet && node.port) {
      node.port.postMessage({ type: 'map', c0: cNow, n0 });
    } else {
      fbMap = { c0: cNow, n0 };
    }
  }

  function applyLocal() {
    if (isWorklet && node && node.port) {
      node.port.postMessage({ type: 'role', role: settings.role });
    }
    if (gain && ctx) {
      // Natural logarithmic curve up to 100%, and direct clean gain boost up to 2.0x above 100%
      const targetGain = settings.volume <= 1 ? (settings.volume * settings.volume) : (settings.volume);
      gain.gain.setTargetAtTime(targetGain, ctx.currentTime, 0.05);
    }
    mapPrev = null; // let offset changes take effect promptly
  }

  // iOS: an <audio> element playing makes Web Audio ignore the silent switch.
  function unlockIOSPlayback() {
    if (!isIOS) return;
    const sr = 8000, len = sr;
    const b = new ArrayBuffer(44 + len * 2);
    const v = new DataView(b);
    const w = (o, s) => [...s].forEach((ch, i) => v.setUint8(o + i, ch.charCodeAt(0)));
    w(0, 'RIFF'); v.setUint32(4, 36 + len * 2, true); w(8, 'WAVE'); w(12, 'fmt ');
    v.setUint32(16, 16, true); v.setUint16(20, 1, true); v.setUint16(22, 1, true);
    v.setUint32(24, sr, true); v.setUint32(28, sr * 2, true); v.setUint16(32, 2, true); v.setUint16(34, 16, true);
    w(36, 'data'); v.setUint32(40, len * 2, true);
    const a = new Audio(URL.createObjectURL(new Blob([b], { type: 'audio/wav' })));
    a.loop = true;
    a.setAttribute('playsinline', '');
    a.play().catch(() => {});
  }

  async function keepAwake() {
    try {
      if ('wakeLock' in navigator) {
        await navigator.wakeLock.request('screen');
        return;
      }
    } catch {}
    $('#keepAwake').play().catch(() => {});
  }

  function updateResume() {
    $('#resumeBtn').classList.toggle('hidden', !ctx || ctx.state === 'running');
  }

  // ------------------------------------------------------------- Mac output device
  const isCapture = (label) => /blackhole/i.test(label || '');

  async function listOutputs(askPermission) {
    if (!navigator.mediaDevices?.enumerateDevices) return [];
    let devs = await navigator.mediaDevices.enumerateDevices();
    let outs = devs.filter((d) => d.kind === 'audiooutput');
    if (askPermission && outs.every((d) => !d.label)) {
      try {
        const s = await navigator.mediaDevices.getUserMedia({ audio: true });
        s.getTracks().forEach((t) => t.stop());
        devs = await navigator.mediaDevices.enumerateDevices();
        outs = devs.filter((d) => d.kind === 'audiooutput');
      } catch {}
    }
    return outs;
  }

  function pickSpeaker(outs) {
    const saved = outs.find((d) => d.deviceId === settings.sinkId && !isCapture(d.label));
    if (saved) return saved.deviceId;
    const spk = outs.find((d) => /speaker/i.test(d.label) && !isCapture(d.label) && d.deviceId !== 'default');
    if (spk) return spk.deviceId;
    const other = outs.find((d) => d.label && !isCapture(d.label) && !['default', 'communications'].includes(d.deviceId));
    return other ? other.deviceId : '';
  }

  function fillSinkSelects(outs, selected) {
    for (const sel of [$('#joinSinkSelect'), $('#sinkSelect')]) {
      sel.innerHTML = '';
      const usable = outs.filter((d) => d.label && !['default', 'communications'].includes(d.deviceId));
      if (!usable.length) {
        sel.innerHTML = '<option value="">Allow access to list devices…</option>';
        continue;
      }
      for (const d of usable) {
        const o = document.createElement('option');
        o.value = d.deviceId;
        o.textContent = d.label + (isCapture(d.label) ? '  (capture — not allowed)' : '');
        o.disabled = isCapture(d.label);
        sel.appendChild(o);
      }
      sel.value = selected;
    }
  }

  async function prepareMacSink() {
    if (!window.AudioContext || !AudioContext.prototype.setSinkId) {
      throw new Error('On the Mac, open this page in Google Chrome. Safari can’t send audio to the speakers while BlackHole is the system output.');
    }
    const outs = await listOutputs(true);
    const id = pickSpeaker(outs);
    fillSinkSelects(outs, id);
    if (!id) throw new Error('Couldn’t find the MacBook speakers. Allow microphone access so the device list can be read, then try again.');
    settings.sinkId = id;
    saveSettings();
  }

  async function applySink(id) {
    if (!ctx || !ctx.setSinkId || !id) return;
    const outs = await listOutputs(false);
    const d = outs.find((x) => x.deviceId === id);
    if (d && isCapture(d.label)) return; // never feed back into the capture device
    await ctx.setSinkId(id);
    settings.sinkId = id;
    saveSettings();
  }

  // ------------------------------------------------------------- Room & Sharing
  const RANDOM_WORDS = ['WAVE', 'BEAT', 'BASS', 'VIBE', 'FLOW', 'ECHO', 'PULSE', 'SYNC', 'GROOVE', 'CLUB'];
  function generateRoomCode() {
    const word = RANDOM_WORDS[Math.floor(Math.random() * RANDOM_WORDS.length)];
    const num = Math.floor(10 + Math.random() * 90);
    return `${word}${num}`;
  }

  function updateRoomPill() {
    const pill = $('#roomPill');
    const text = $('#roomCodeText');
    if (text) text.textContent = settings.room;
    if (pill) pill.classList.remove('hidden');
    const inp = $('#roomInput');
    if (inp && document.activeElement !== inp) inp.value = settings.room;
  }

  function openQrModal() {
    const shareUrl = `${location.origin}?room=${encodeURIComponent(settings.room)}`;
    const txt = $('#shareUrlText');
    const img = $('#qrImg');
    if (txt) txt.textContent = shareUrl;
    if (img) img.src = `/api/qr?url=${encodeURIComponent(shareUrl)}`;
    $('#qrModal')?.classList.remove('hidden');
  }

  function closeQrModal() {
    $('#qrModal')?.classList.add('hidden');
  }

  // ------------------------------------------------------------- DJ File Upload
  function handleFileUpload(file) {
    if (!file) return;
    const valid = file.type.startsWith('audio/') || /\.(mp3|m4a|wav|flac|aac|ogg)$/i.test(file.name);
    if (!valid) {
      alert('Please select an audio file (.mp3, .m4a, .wav, .flac, .aac)');
      return;
    }
    const upBar = $('#uploadProgress');
    const upFill = upBar ? upBar.querySelector('.upload-fill') : null;
    if (upBar) upBar.classList.remove('hidden');
    if (upFill) upFill.style.width = '0%';

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/upload');
    xhr.setRequestHeader('x-room-code', settings.room);
    xhr.setRequestHeader('x-file-name', encodeURIComponent(file.name));

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && upFill) {
        const pct = Math.round((e.loaded / e.total) * 100);
        upFill.style.width = `${pct}%`;
      }
    };

    xhr.onload = () => {
      if (upBar) upBar.classList.add('hidden');
      if (xhr.status === 200) {
        try {
          const res = JSON.parse(xhr.responseText);
          const np = $('#nowPlayingText');
          if (np) np.textContent = `🎵 ${file.name}`;
        } catch {}
      } else {
        alert('Upload failed: ' + (xhr.responseText || xhr.statusText));
      }
    };

    xhr.onerror = () => {
      if (upBar) upBar.classList.add('hidden');
      alert('Upload error. Please check your network connection.');
    };

    xhr.send(file);
  }

  // ------------------------------------------------------------- Live Tab / Browser Audio Broadcast
  let broadcastStream = null;
  let broadcastCtx = null;
  let broadcastNode = null;
  let broadcastSource = null;

  async function startTabAudioBroadcast() {
    if (!navigator.mediaDevices?.getDisplayMedia) {
      alert('Live audio sharing is supported in Google Chrome, Edge, and modern desktop browsers.');
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getDisplayMedia({
        video: true,
        audio: {
          echoCancellation: false,
          noiseSuppression: false,
          autoGainControl: false,
        }
      });
      const audioTracks = stream.getAudioTracks();
      if (!audioTracks.length) {
        stream.getTracks().forEach((t) => t.stop());
        alert('No audio track selected! When Chrome opens the picker, make sure to check "Share audio" (or "Also share tab audio").');
        return;
      }
      broadcastStream = stream;
      broadcastCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: STREAM_RATE });
      broadcastSource = broadcastCtx.createMediaStreamSource(stream);
      broadcastNode = broadcastCtx.createScriptProcessor(2048, 2, 2);

      broadcastNode.onaudioprocess = (e) => {
        if (!ws || ws.readyState !== 1) return;
        const left = e.inputBuffer.getChannelData(0);
        const right = e.inputBuffer.getChannelData(1);
        const n = left.length;
        const buf = new ArrayBuffer(4 + n * 4);
        const dv = new DataView(buf);
        dv.setUint8(0, 0x53); dv.setUint8(1, 0x57); dv.setUint8(2, 0x50); dv.setUint8(3, 0x43);
        let off = 4;
        for (let i = 0; i < n; i++) {
          const sL = Math.max(-1, Math.min(1, left[i]));
          const sR = Math.max(-1, Math.min(1, right[i]));
          dv.setInt16(off, sL < 0 ? sL * 32768 : sL * 32767, true);
          dv.setInt16(off + 2, sR < 0 ? sR * 32768 : sR * 32767, true);
          off += 4;
        }
        ws.send(buf);
      };

      broadcastSource.connect(broadcastNode);
      broadcastNode.connect(broadcastCtx.destination);

      audioTracks[0].onended = stopTabAudioBroadcast;

      updateStreamButton(true);
      send({ type: 'setGlobal', source: 'stream' });
    } catch (err) {
      if (err.name !== 'NotAllowedError') console.error(err);
      stopTabAudioBroadcast();
    }
  }

  function stopTabAudioBroadcast() {
    if (broadcastStream) {
      broadcastStream.getTracks().forEach((t) => t.stop());
      broadcastStream = null;
    }
    if (broadcastNode) {
      broadcastNode.disconnect();
      broadcastNode = null;
    }
    if (broadcastSource) {
      broadcastSource.disconnect();
      broadcastSource = null;
    }
    if (broadcastCtx) {
      broadcastCtx.close().catch(() => {});
      broadcastCtx = null;
    }
    updateStreamButton(false);
  }

  function updateStreamButton(active) {
    const b = $('#toggleStreamBtn');
    const txt = $('#streamBtnText');
    if (txt) txt.textContent = active ? 'Stop Live Broadcast' : 'Start Audio Broadcast';
    if (b) b.classList.toggle('bad', active);
  }

  // ------------------------------------------------------------- join
  async function join() {
    const btn = $('#joinBtn');
    const errEl = $('#joinError');
    errEl.classList.add('hidden');
    btn.disabled = true;

    const chosenRoom = ($('#roomInput').value.trim() || 'MAIN').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'MAIN';
    const roomChanged = (chosenRoom !== settings.room);
    settings.room = chosenRoom;
    settings.name = $('#nameInput').value.trim() || defaultName;
    saveSettings();

    // Sync URL parameter so link is directly shareable
    const newUrl = `${location.pathname}?room=${encodeURIComponent(settings.room)}`;
    window.history.replaceState(null, '', newUrl);

    try {
      if (isMac) await prepareMacSink();
      await startAudio();
      keepAwake();
      joined = true;
      updateRoomPill();

      if (roomChanged || !ws || ws.readyState !== 1) {
        if (ws) { ws.onclose = null; ws.close(); }
        connect();
      } else {
        sendHello();
      }

      $('#joinView').classList.add('hidden');
      $('#playerView').classList.remove('hidden');
      syncControls();
      resizeViz();
    } catch (e) {
      console.error(e);
      errEl.textContent = e.message || String(e);
      errEl.classList.remove('hidden');
      if (ctx) {
        ctx.close().catch(() => {});
        ctx = node = null;
      }
    } finally {
      btn.disabled = false;
    }
  }

  // ------------------------------------------------------------- UI helpers
  function setConn(kind, text) {
    const p = $('#connPill');
    p.className = 'pill ' + kind;
    p.querySelector('span').textContent = text;
  }

  function fillRange(el) {
    const min = +el.min, max = +el.max;
    el.style.setProperty('--p', ((el.value - min) / (max - min)) * 100 + '%');
  }

  function bindSeg(seg, attr, onPick) {
    seg.addEventListener('click', (e) => {
      const b = e.target.closest('button');
      if (b) onPick(b.dataset[attr]);
    });
  }
  function setSeg(seg, attr, value) {
    seg.querySelectorAll('button').forEach((b) => b.classList.toggle('active', b.dataset[attr] === value));
  }

  const roleBadge = { stereo: 'L R', left: 'L', right: 'R', mono: 'MONO' };
  const roleName = { stereo: 'Stereo', left: 'Left', right: 'Right', mono: 'Mono' };
  const fmtOff = (v) => (v > 0 ? '+' : '') + Math.round(v) + ' ms';

  function syncControls() {
    $('#nameInput').value = settings.name;
    setSeg($('#joinRoleSeg'), 'role', settings.role);
    setSeg($('#roleSeg'), 'role', settings.role);
    $('#roleBadge').textContent = roleBadge[settings.role];
    $('#deviceTitle').textContent = settings.name;
    const vol = $('#volSlider');
    vol.value = Math.round(settings.volume * 100);
    $('#volVal').textContent = vol.value + '%';
    fillRange(vol);
    const off = $('#offSlider');
    off.value = settings.offsetMs;
    $('#offVal').textContent = fmtOff(settings.offsetMs);
    fillRange(off);
  }

  function setGroup(s) {
    group = { ...group, ...s };
    setSeg($('#sourceSeg'), 'source', group.source);

    // Toggle source sections
    const macSec = $('#macLiveSection');
    const streamSec = $('#streamSection');
    const fileSec = $('#fileSection');
    const clicksSec = $('#clicksSection');
    if (macSec) macSec.classList.toggle('hidden', group.source !== 'music' || !group.hasMacCapture);
    if (streamSec) streamSec.classList.toggle('hidden', group.source !== 'stream');
    if (fileSec) fileSec.classList.toggle('hidden', group.source !== 'file');
    if (clicksSec) clicksSec.classList.toggle('hidden', group.source !== 'clicks');

    const djChip = $('#djChip');
    if (djChip) {
      if (group.source === 'stream') {
        djChip.textContent = 'Live Broadcast';
        djChip.className = 'chip active';
      } else if (group.source === 'music') {
        djChip.textContent = 'Mac Live';
        djChip.className = 'chip active';
      } else if (group.currentTrack && group.playbackState === 'playing') {
        djChip.textContent = 'Playing';
        djChip.className = 'chip active';
      } else if (group.source === 'clicks') {
        djChip.textContent = 'Metronome';
        djChip.className = 'chip';
      } else {
        djChip.textContent = 'Pass the Aux';
        djChip.className = 'chip';
      }
    }

    const np = $('#nowPlayingText');
    if (np) {
      if (group.source === 'stream') {
        np.textContent = '🎙️ Broadcasting Live Tab / System Audio';
      } else if (group.source === 'music') {
        np.textContent = '🍎 Streaming Mac System Output (Apple Music, YouTube, Spotify)';
      } else if (group.source === 'clicks') {
        np.textContent = '⚡ Metronome Calibration';
      } else if (group.currentTrack) {
        np.textContent = `🎵 ${group.currentTrack.name}`;
      } else {
        np.textContent = 'Choose or drop song to play';
      }
    }

    const srcMusicBtn = $('#srcMusic');
    if (srcMusicBtn) {
      srcMusicBtn.classList.toggle('hidden', !group.hasMacCapture);
    }

    const ds = $('#delaySlider');
    if (ds && document.activeElement !== ds) {
      ds.value = group.delayMs;
      $('#delayVal').textContent = (group.delayMs / 1000).toFixed(1) + ' s';
      fillRange(ds);
    }
  }

  // ------------------------------------------------------------- status loop
  function statusLoop() {
    if (joined) {
      const st = $('#syncStatus');
      const text = st.querySelector('span');
      const w = wstats;
      let cls = '', msg;
      if (clock.offset == null) msg = 'Syncing clocks…';
      else if (!ctx || ctx.state !== 'running') { msg = 'Audio paused'; cls = 'bad'; }
      else if (!w || !w.playing) msg = `Buffering ${(group.delayMs / 1000).toFixed(1)} s…`;
      else if (w.aheadMs < 120) { msg = 'Network too slow. Raise the sync buffer.'; cls = 'bad'; }
      else if (Math.abs(w.err) < 2) { msg = `In sync · ±${Math.max(0.5, clock.rtt / 2).toFixed(1)} ms`; cls = 'locked'; }
      else msg = `Locking… ${w.err.toFixed(1)} ms`;
      st.className = 'sync-status ' + cls;
      text.textContent = msg;

      $('#mClock').textContent = clock.rtt != null ? `${clock.rtt.toFixed(1)} ms` : '—';
      $('#mLock').textContent = w ? `${w.err >= 0 ? '+' : ''}${w.err.toFixed(2)} ms` : '—';
      $('#mBuffer').textContent = w ? `${Math.max(0, w.aheadMs / 1000).toFixed(2)} s` : '—';
      $('#mGlitch').textContent = w ? String(w.underruns + w.seeks) : '0';
    }
    setTimeout(statusLoop, 250);
  }

  setInterval(() => {
    if (!joined || !wstats) return;
    send({
      type: 'stats',
      stats: {
        rtt: clock.rtt,
        jitter: clock.jitter,
        err: wstats.err,
        aheadMs: wstats.aheadMs,
        glitches: wstats.underruns + wstats.seeks,
        playing: wstats.playing,
        level: Math.max(wstats.peakL, wstats.peakR),
        state: ctx && ctx.state,
      },
    });
  }, 1000);

  // ------------------------------------------------------------- room list
  const icons = {
    mac: '<svg viewBox="0 0 24 24"><rect x="4" y="5" width="16" height="11" rx="1.5"/><path d="M2 19h20"/></svg>',
    iphone: '<svg viewBox="0 0 24 24"><rect x="7" y="2.5" width="10" height="19" rx="2.5"/><path d="M11 18.5h2"/></svg>',
    ipad: '<svg viewBox="0 0 24 24"><rect x="4.5" y="2.5" width="15" height="19" rx="2"/><path d="M11 18.5h2"/></svg>',
    other: '<svg viewBox="0 0 24 24"><rect x="6" y="3" width="12" height="18" rx="2"/><circle cx="12" cy="14" r="3"/><circle cx="12" cy="7" r="1"/></svg>',
  };
  const rows = new Map();

  function buildRow(d) {
    const el = document.createElement('div');
    el.className = 'dev';
    el.innerHTML = `
      <div class="dev-icon">${icons[d.platform] || icons.other}<span class="lvl"></span></div>
      <div class="dev-info"><div class="dev-name"></div><div class="dev-stats"></div></div>
      <div class="dev-ctrl">
        <select class="dev-role">${Object.entries(roleName).map(([k, v]) => `<option value="${k}">${v}</option>`).join('')}</select>
        <div class="mini-step"><button data-d="-1" aria-label="Earlier">−</button><span class="dev-off"></span><button data-d="1" aria-label="Later">+</button></div>
        <input type="range" class="dev-vol" min="0" max="100" />
      </div>`;
    const role = el.querySelector('.dev-role');
    role.id = `devRole${d.id}`;
    role.addEventListener('change', () => setDevice(d.id, { role: role.value }));
    el.querySelectorAll('.mini-step button').forEach((b) => {
      b.addEventListener('click', (e) => {
        const cur = room.find((x) => x.id === d.id);
        if (!cur) return;
        const step = (e.shiftKey ? 10 : 1) * +b.dataset.d;
        setDevice(d.id, { offsetMs: cur.offsetMs + step });
      });
    });
    const vol = el.querySelector('.dev-vol');
    vol.id = `devVol${d.id}`;
    vol.addEventListener('input', () => {
      fillRange(vol);
      setDevice(d.id, { volume: vol.value / 100 });
    });
    return el;
  }

  function setDevice(id, patch) {
    const d = room.find((x) => x.id === id);
    if (d) Object.assign(d, patch);
    if (id === myId) {
      Object.assign(settings, patch);
      saveSettings();
      applyLocal();
      syncControls();
    }
    send({ type: 'setDevice', id, ...patch });
    renderRoom();
  }

  function renderRoom() {
    const list = $('#roomList');
    $('#roomCount').textContent = `${room.length} ${room.length === 1 ? 'speaker' : 'speakers'}`;
    const seen = new Set();
    for (const d of room) {
      seen.add(d.id);
      let el = rows.get(d.id);
      if (!el) {
        el = buildRow(d);
        rows.set(d.id, el);
        list.appendChild(el);
      }
      el.classList.toggle('me', d.id === myId);
      el.querySelector('.dev-name').innerHTML = `${escapeHtml(d.name)}${d.id === myId ? '<span class="you">this device</span>' : ''}`;
      const s = d.stats || {};
      const parts = [];
      if (s.rtt != null) parts.push(`rtt ${s.rtt.toFixed(1)}ms`);
      if (s.aheadMs != null) {
        const cls = s.aheadMs < 150 ? 'bad' : s.aheadMs < 400 ? 'warn' : 'ok';
        parts.push(`<span class="${cls}">buf ${(s.aheadMs / 1000).toFixed(2)}s</span>`);
      }
      if (s.err != null) parts.push(`<span class="${Math.abs(s.err) < 2 ? 'ok' : 'warn'}">lock ${s.err.toFixed(1)}ms</span>`);
      if (s.glitches) parts.push(`<span class="warn">${s.glitches} glitch</span>`);
      if (s.state && s.state !== 'running') parts.push(`<span class="bad">${s.state}</span>`);
      el.querySelector('.dev-stats').innerHTML = parts.join(' · ') || 'connecting…';
      el.querySelector('.lvl').style.transform = `scaleX(${Math.min(1, (s.level || 0) * 1.4)})`;
      const role = el.querySelector('.dev-role');
      if (document.activeElement !== role) role.value = d.role;
      el.querySelector('.dev-off').textContent = fmtOff(d.offsetMs);
      const vol = el.querySelector('.dev-vol');
      if (document.activeElement !== vol) {
        vol.value = Math.round(d.volume * 100);
        fillRange(vol);
      }
    }
    for (const [id, el] of rows) {
      if (!seen.has(id)) {
        el.remove();
        rows.delete(id);
      }
    }
    if (!room.length && !list.querySelector('.room-empty')) list.innerHTML = '<div class="room-empty">No speakers yet</div>';
    else if (room.length) list.querySelector('.room-empty')?.remove();
  }

  const escapeHtml = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

  // ------------------------------------------------------------- visualizer
  const canvas = $('#viz');
  const g2 = canvas.getContext('2d');
  let freq = null;
  let smoothLvl = 0;

  function resizeViz() {
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const r = canvas.getBoundingClientRect();
    canvas.width = Math.max(1, r.width * dpr);
    canvas.height = Math.max(1, r.height * dpr);
    g2.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  window.addEventListener('resize', resizeViz);

  function draw(t) {
    requestAnimationFrame(draw);
    if (!joined) return;
    const W = canvas.clientWidth, H = canvas.clientHeight - 60;
    g2.clearRect(0, 0, canvas.clientWidth, canvas.clientHeight);
    const cx = W / 2, cy = H / 2;
    const base = Math.min(W, H) * 0.26;

    let bins = 96, data = null;
    if (analyser) {
      if (!freq) freq = new Uint8Array(analyser.frequencyBinCount);
      analyser.getByteFrequencyData(freq);
      data = freq;
    }
    let sum = 0;
    const vals = new Array(bins);
    for (let i = 0; i < bins; i++) {
      // log-ish mapping so bass doesn't dominate the ring
      const idx = data ? Math.min(data.length - 1, Math.floor(Math.pow(i / bins, 1.6) * data.length * 0.7) + 1) : 0;
      const v = data ? data[idx] / 255 : 0;
      const idle = 0.04 + 0.03 * Math.sin(t / 700 + i * 0.35);
      vals[i] = Math.max(v, idle);
      sum += v;
    }
    smoothLvl += (sum / bins - smoothLvl) * 0.12;

    // glow
    const glowR = base * (1.25 + smoothLvl * 1.2);
    const grd = g2.createRadialGradient(cx, cy, base * 0.2, cx, cy, glowR * 1.6);
    grd.addColorStop(0, `rgba(255,95,162,${0.22 + smoothLvl * 0.5})`);
    grd.addColorStop(0.5, `rgba(124,107,255,${0.12 + smoothLvl * 0.3})`);
    grd.addColorStop(1, 'rgba(124,107,255,0)');
    g2.fillStyle = grd;
    g2.beginPath();
    g2.arc(cx, cy, glowR * 1.6, 0, Math.PI * 2);
    g2.fill();

    // radial bars (mirrored)
    const rot = t / 9000;
    g2.lineCap = 'round';
    for (let side = -1; side <= 1; side += 2) {
      for (let i = 0; i < bins; i++) {
        const a = rot - Math.PI / 2 + side * (i / bins) * Math.PI;
        const len = 6 + vals[i] * base * 0.95;
        const r0 = base + 4;
        const hue = 330 - (i / bins) * 80;
        g2.strokeStyle = `hsla(${hue}, 100%, ${62 + vals[i] * 18}%, ${0.55 + vals[i] * 0.45})`;
        g2.lineWidth = Math.max(1.5, (Math.PI * base) / bins / 1.7);
        g2.beginPath();
        g2.moveTo(cx + Math.cos(a) * r0, cy + Math.sin(a) * r0);
        g2.lineTo(cx + Math.cos(a) * (r0 + len), cy + Math.sin(a) * (r0 + len));
        g2.stroke();
      }
    }

    // inner ring
    g2.strokeStyle = 'rgba(255,255,255,0.08)';
    g2.lineWidth = 1;
    g2.beginPath();
    g2.arc(cx, cy, base - 6, 0, Math.PI * 2);
    g2.stroke();
  }
  requestAnimationFrame(draw);

  // ------------------------------------------------------------- wire up
  function init() {
    $$('.mac-only').forEach((el) => el.classList.toggle('hidden', !isMac));
    $$('.ios-only').forEach((el) => el.classList.toggle('hidden', !isIOS));
    syncControls();
    updateRoomPill();

    // Room code input & Generator
    const roomInp = $('#roomInput');
    if (roomInp) {
      roomInp.value = settings.room;
      roomInp.addEventListener('input', (e) => {
        e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      });
    }

    const genBtn = $('#genRoomBtn');
    if (genBtn && roomInp) {
      genBtn.addEventListener('click', () => {
        const code = generateRoomCode();
        roomInp.value = code;
      });
    }

    // Room Pill & QR Invite Modal
    $('#roomPill')?.addEventListener('click', openQrModal);
    $('#closeQrBtn')?.addEventListener('click', closeQrModal);
    $('#qrModal')?.addEventListener('click', (e) => {
      if (e.target === $('#qrModal')) closeQrModal();
    });

    $('#copyUrlBtn')?.addEventListener('click', async () => {
      const shareUrl = `${location.origin}?room=${encodeURIComponent(settings.room)}`;
      try {
        await navigator.clipboard.writeText(shareUrl);
        const b = $('#copyUrlBtn');
        const old = b.textContent;
        b.textContent = 'Copied!';
        setTimeout(() => { b.textContent = old; }, 1500);
      } catch {
        prompt('Copy room link:', shareUrl);
      }
    });

    // DJ Aux File Drop & Input
    const dropZone = $('#dropZone');
    const fileInput = $('#fileInput');
    if (dropZone && fileInput) {
      dropZone.addEventListener('click', () => fileInput.click());
      dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
      });
      ['dragleave', 'dragend', 'drop'].forEach((ev) => {
        dropZone.addEventListener(ev, () => dropZone.classList.remove('dragover'));
      });
      dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        const files = e.dataTransfer?.files;
        if (files && files.length > 0) handleFileUpload(files[0]);
      });
      fileInput.addEventListener('change', () => {
        if (fileInput.files && fileInput.files.length > 0) {
          handleFileUpload(fileInput.files[0]);
          fileInput.value = '';
        }
      });
    }

    bindSeg($('#joinRoleSeg'), 'role', (r) => {
      settings.role = r;
      saveSettings();
      syncControls();
    });
    bindSeg($('#roleSeg'), 'role', (r) => setDevice(myId, { role: r }));
    bindSeg($('#sourceSeg'), 'source', (s) => {
      send({ type: 'setGlobal', source: s });
      if (s === 'stream' && !broadcastStream) {
        startTabAudioBroadcast();
      } else if (s !== 'stream' && broadcastStream) {
        stopTabAudioBroadcast();
      }
    });

    $('#toggleStreamBtn')?.addEventListener('click', () => {
      if (broadcastStream) stopTabAudioBroadcast();
      else startTabAudioBroadcast();
    });

    $('#joinBtn').addEventListener('click', join);
    $('#nameInput').addEventListener('change', (e) => {
      settings.name = e.target.value.trim() || defaultName;
      saveSettings();
    });

    const vol = $('#volSlider');
    vol.addEventListener('input', () => {
      settings.volume = vol.value / 100;
      $('#volVal').textContent = vol.value + '%';
      fillRange(vol);
      saveSettings();
      applyLocal();
      sendSelf({ volume: settings.volume });
    });

    const off = $('#offSlider');
    off.addEventListener('input', () => setDevice(myId, { offsetMs: +off.value }));
    $('#offMinus').addEventListener('click', () => setDevice(myId, { offsetMs: Math.max(-200, settings.offsetMs - 1) }));
    $('#offPlus').addEventListener('click', () => setDevice(myId, { offsetMs: Math.min(200, settings.offsetMs + 1) }));

    const ds = $('#delaySlider');
    ds.addEventListener('input', () => {
      $('#delayVal').textContent = (ds.value / 1000).toFixed(1) + ' s';
      fillRange(ds);
    });
    ds.addEventListener('change', () => send({ type: 'setGlobal', delayMs: +ds.value }));

    const onSink = async (e) => {
      const id = e.target.value;
      settings.sinkId = id;
      saveSettings();
      try {
        await applySink(id);
      } catch (err) {
        console.error(err);
      }
      $('#joinSinkSelect').value = $('#sinkSelect').value = id;
    };
    $('#joinSinkSelect')?.addEventListener('change', onSink);
    $('#sinkSelect')?.addEventListener('change', onSink);

    $('#resumeBtn').addEventListener('click', async () => {
      try {
        await ctx.resume();
      } catch {}
      unlockIOSPlayback();
      keepAwake();
      updateResume();
    });
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && joined) {
        updateResume();
        keepAwake();
      }
    });

    if (isMac) listOutputs(false).then((outs) => fillSinkSelects(outs, pickSpeaker(outs)));
    for (const r of $$('input[type=range]')) fillRange(r);

    connect();
    statusLoop();
  }

  init();
})();
