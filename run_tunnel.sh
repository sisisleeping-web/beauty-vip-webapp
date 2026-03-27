#!/usr/bin/env bash
set -euo pipefail
exec /opt/homebrew/bin/ngrok http 5090 --log=stdout
