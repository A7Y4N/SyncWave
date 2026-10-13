#!/bin/bash

# Move to the project directory
cd "$(dirname "$0")"

echo "============================================="
echo "🎵 Starting SyncWave Multi-Speaker System..."
echo "============================================="

# Kill any existing instances
pkill -f cloudflared 2>/dev/null
pkill -f "node server.js" 2>/dev/null
sleep 1

# Start Cloudflare Tunnel in background
./bin/cloudflared tunnel --url http://127.0.0.1:8080 > tunnel.log 2>&1 &
TUNNEL_PID=$!

echo "Waiting for tunnel connection..."
sleep 4

# Extract tunnel URL
TUNNEL_URL=$(grep -Eo "https://[a-zA-Z0-9-]+\.trycloudflare\.com" tunnel.log | tail -n 1)

echo ""
echo "🚀 SyncWave is UP and RUNNING!"
echo "---------------------------------------------"
echo "💻 On MacBook (Chrome):"
echo "   http://localhost:8080"
echo ""
echo "📱 On iPhone & iPad (Safari):"
if [ -n "$TUNNEL_URL" ]; then
  echo "   $TUNNEL_URL"
else
  echo "   Check tunnel.log for URL"
fi
echo "---------------------------------------------"
echo "Press Ctrl+C to stop everything."
echo ""

# Trap Ctrl+C to cleanly kill tunnel and restore speakers
trap "echo 'Stopping...'; kill $TUNNEL_PID 2>/dev/null; pkill -f cloudflared; exit 0" INT TERM

# Start the audio server
npm start
