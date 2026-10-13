# SyncWave 🎵 — Universal Multi-Room Sound System

SyncWave turns any group of phones (iPhone, Android), tablets (iPad), and laptops (Mac, Windows) into a synchronized, multi-channel surround sound system directly in the web browser.

---

## 🌟 Features

- **Multi-Room & Isolated Groups**: Anyone can generate a room code (e.g. `PARTY1`, `VIBE88`) or join an existing room. Groups of friends listen in their own rooms with zero audio cross-talk.
- **Pass the Aux / DJ Mode**: Drop any audio file (`.mp3`, `.m4a`, `.wav`, `.flac`, `.aac`) directly from your phone or laptop. It broadcasts instantly in real-time across all connected speakers.
- **Instant QR Code Sharing**: Click the Room Pill in the top bar to display a QR code; friends can scan with their phone camera to join the room in 1 second.
- **Sub-Millisecond Clock Synchronization**: Continuous NTP-style clock synchronization and sample-accurate dynamic rate nudging keeps all speakers in phase-accurate unison.
- **Independent Channels & Hardware Boost**: Assign devices as **Left**, **Right**, **Center/Mono**, or **Stereo**, with up to **200% volume boost** for smaller or older mobile speakers.
- **Mac Live Audio Capture**: When running locally on Mac, captures system audio / Apple Music via BlackHole.

---

## 🚀 Running Locally (Mac + Cloudflare Tunnel)

1. Run the one-click startup script:
   ```bash
   ./start.sh
   ```
2. The terminal will output:
   - **Local URL**: `http://localhost:8080`
   - **Public HTTPS URL**: `https://<random-id>.trycloudflare.com`
3. Open the public HTTPS link on your iPhone, iPad, or Android (works across restricted college/campus Wi-Fi and mobile data).
4. Tap **New Room** or enter a room code, then hit **Tap to Join Room**!

---

## ☁️ Free 24/7 Cloud Hosting for the World

You can host SyncWave online for free so anyone around the world can use it anytime without your laptop needing to be on.

### Option 1: Deploy on Render.com (Recommended - Free)
1. Push this folder to a GitHub repository.
2. Sign up / Log in to [Render.com](https://render.com).
3. Click **New +** -> **Web Service** -> Connect your GitHub repo.
4. Select **Docker** environment (or Render will automatically detect `render.yaml` / `Dockerfile`).
5. Set the Plan to **Free**.
6. Click **Deploy Web Service**.
7. Render gives you an instant free HTTPS domain (e.g. `https://syncwave.onrender.com`) with WebSocket support!

### Option 2: Deploy on Railway.app
1. Go to [Railway.app](https://railway.app).
2. Click **New Project** -> **Deploy from GitHub repo**.
3. Railway automatically detects the `Dockerfile` and deploys it in ~1 minute.

---

## 🛠️ Calibration & Sync Tuning

- **Metronome Mode**: Switch the audio source to **Metronome** in the DJ panel. All connected devices will click in rhythmic unison to test phase alignment.
- **Fine Sync**: Adjust the millisecond stepper (`+` / `-`) on any device to compensate for hardware speaker latency differences.
