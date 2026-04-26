#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
URL_FILE="$SCRIPT_DIR/current_tunnel_url.txt"

: > "$URL_FILE"

exec /opt/homebrew/bin/cloudflared tunnel --no-autoupdate --url http://127.0.0.1:5090 2>&1 \
  | tee >(awk 'match($0, /https:\/\/[-[:alnum:].]+\.trycloudflare\.com/) { print substr($0, RSTART, RLENGTH) > file; close(file) }' file="$URL_FILE")
