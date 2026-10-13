/**
 * SyncWave playback worklet.
 *
 * Holds a ~11 s ring buffer indexed by the *global* stream frame number. The main
 * thread tells us, via a linear map, which stream frame should be rendered at a
 * given AudioContext time. Every render quantum we compare where we are with where
 * we should be and nudge the playback rate by a tiny amount (max ±0.3 %, which is
 * inaudible) so we stay locked. Big errors (> 30 ms) cause an instant re-seek.
 */
const SR = 48000; // stream sample rate
const SIZE = 1 << 19; // 524288 frames ≈ 10.9 s
const MASK = SIZE - 1;
const MAX_ADJ = 0.003;
const SEEK_THRESHOLD = SR * 0.12; // 120 ms seek threshold for Wi-Fi jitter resilience

// 4-point Catmull-Rom interpolation (cheap, high quality for tiny rate changes)
function herm(x0, x1, x2, x3, t) {
  const c1 = 0.5 * (x2 - x0);
  const c2 = x0 - 2.5 * x1 + 2 * x2 - 0.5 * x3;
  const c3 = 0.5 * (x3 - x0) + 1.5 * (x1 - x2);
  return ((c3 * t + c2) * t + c1) * t + x1;
}

class SyncPlayer extends AudioWorkletProcessor {
  constructor() {
    super();
    this.L = new Float32Array(SIZE);
    this.R = new Float32Array(SIZE);
    this.first = -1; // first frame index ever written
    this.written = -1; // one past highest frame written
    this.pos = null; // current read position (fractional stream frame)
    this.map = null; // { c0, n0 }
    this.role = 'stereo';
    this.ratio = 1;
    this.err = 0;
    this.underruns = 0;
    this.seeks = 0;
    this.playing = false;
    this.peakL = 0;
    this.peakR = 0;
    this.lastReport = 0;

    this.port.onmessage = (e) => {
      const m = e.data;
      if (m.type === 'pcm') this.write(m.n, m.buf);
      else if (m.type === 'map') this.map = { c0: m.c0, n0: m.n0 };
      else if (m.type === 'role') this.role = m.role;
      else if (m.type === 'reset') this.reset();
    };
  }

  reset() {
    this.L.fill(0);
    this.R.fill(0);
    this.first = this.written = -1;
    this.pos = null;
    this.playing = false;
  }

  write(n, buf) {
    const pcm = new Int16Array(buf, 16);
    const frames = pcm.length >> 1;
    const { L, R } = this;
    if (this.written < 0 || n + frames < this.written - SIZE || n > this.written + SIZE) {
      // first data, or a discontinuity bigger than the ring: start fresh
      if (this.written >= 0) {
        L.fill(0);
        R.fill(0);
      }
      this.first = n;
      this.written = n;
    } else if (n > this.written) {
      for (let k = this.written; k < n; k++) {
        const i = k & MASK;
        L[i] = 0;
        R[i] = 0;
      }
    }
    for (let k = 0; k < frames; k++) {
      const i = (n + k) & MASK;
      L[i] = pcm[2 * k] / 32768;
      R[i] = pcm[2 * k + 1] / 32768;
    }
    if (n + frames > this.written) this.written = n + frames;
  }

  process(inputs, outputs) {
    const out = outputs[0];
    const oL = out[0];
    const oR = out[1] || out[0];
    const N = oL.length;

    if (!this.map || this.written < 0) {
      oL.fill(0);
      oR.fill(0);
      return true;
    }

    const target = this.map.n0 + (currentTime - this.map.c0) * SR;
    if (this.pos === null || Math.abs(target - this.pos) > SEEK_THRESHOLD) {
      if (this.pos !== null && this.playing) this.seeks++;
      this.pos = target;
      this.ratio = 1;
    }
    const errFrames = target - this.pos;
    this.err = (errFrames / SR) * 1000;
    const want = 1 + Math.max(-MAX_ADJ, Math.min(MAX_ADJ, (errFrames / SR) * 0.8));
    this.ratio += (want - this.ratio) * 0.08;
    const step = (this.ratio * SR) / sampleRate;

    const { L, R, role } = this;
    const lo = Math.max(this.first, this.written - SIZE + 1024) + 1;
    const hi = this.written - 3;
    let pos = this.pos;
    let pkL = this.peakL;
    let pkR = this.peakR;
    let starved = false;
    let audible = false;

    for (let i = 0; i < N; i++) {
      const ip = Math.floor(pos);
      if (ip < lo || ip > hi) {
        oL[i] = 0;
        oR[i] = 0;
        if (ip > hi) starved = true;
      } else {
        audible = true;
        const t = pos - ip;
        const a = (ip - 1) & MASK, b = ip & MASK, c = (ip + 1) & MASK, d = (ip + 2) & MASK;
        const l = herm(L[a], L[b], L[c], L[d], t);
        const r = herm(R[a], R[b], R[c], R[d], t);
        let outL, outR;
        if (role === 'left') outL = outR = l;
        else if (role === 'right') outL = outR = r;
        else if (role === 'mono') outL = outR = (l + r) * 0.5;
        else {
          outL = l;
          outR = r;
        }
        oL[i] = outL;
        oR[i] = outR;
        const al = outL < 0 ? -outL : outL;
        const ar = outR < 0 ? -outR : outR;
        if (al > pkL) pkL = al;
        if (ar > pkR) pkR = ar;
      }
      pos += step;
    }
    this.pos = pos;
    if (audible) this.playing = true;
    if (starved && this.playing) this.underruns++;
    this.peakL = pkL * 0.97;
    this.peakR = pkR * 0.97;

    if (currentTime - this.lastReport > 0.25) {
      this.lastReport = currentTime;
      this.port.postMessage({
        err: this.err,
        ratio: this.ratio,
        aheadMs: ((this.written - this.pos) / SR) * 1000,
        underruns: this.underruns,
        seeks: this.seeks,
        playing: this.playing,
        peakL: pkL,
        peakR: pkR,
      });
    }
    return true;
  }
}

registerProcessor('sync-player', SyncPlayer);
