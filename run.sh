#!/usr/bin/env bash
set -euo pipefail
cd /Users/openclaw/Projects/beauty-vip-webapp
source .venv/bin/activate
export APP_SECRET_KEY="beauty-vip-demo"
export MANAGER_PIN="1225"
exec python app.py
