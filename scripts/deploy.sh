#!/bin/bash
# deploy.sh — 一鍵部署美咖美容 VIP 系統到 PythonAnywhere
# 使用方式：./scripts/deploy.sh
# 前提：本機要有 CDP 可連的 PA 瀏覽器 session（agent-browser 或 Gemini browser）

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONSOLE_ID="46019051"
PA_USER="sisisleeping"
PA_DEPLOY_CMD="cd ~/beauty-vip-webapp && git pull --ff-only && touch /var/www/${PA_USER}_pythonanywhere_com_wsgi.py && echo DEPLOY_DONE"

echo "=== 美咖美容 VIP — 部署到 PythonAnywhere ==="

# Step 1: git push
cd "$PROJECT_DIR"
echo "[1/2] git push origin main..."
git push origin main
echo "      OK"

# Step 2: PA deploy via CDP
echo "[2/2] 連接 PA Bash console 執行 git pull..."
python3 - << PYEOF
import subprocess, sys, json, time

# Find CDP port from agent-browser
import glob, os
devtools_files = glob.glob("/var/folders/*/*/T/agent-browser-chrome-*/DevToolsActivePort")
if not devtools_files:
    print("找不到 CDP 瀏覽器，請手動在 PA console 執行：")
    print("  cd ~/beauty-vip-webapp && git pull --ff-only && touch /var/www/${PA_USER}_pythonanywhere_com_wsgi.py")
    sys.exit(1)

with open(devtools_files[0]) as f:
    port = int(f.readline().strip())

from playwright.sync_api import sync_playwright
CONSOLE_URL = f"https://www.pythonanywhere.com/user/${PA_USER}/consoles/${CONSOLE_ID}/"
DEPLOY_CMD = "${PA_DEPLOY_CMD}"

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(f"http://localhost:{port}")
    ctx = browser.contexts[0]
    page = ctx.new_page()
    page.goto(CONSOLE_URL, wait_until="domcontentloaded")

    for _ in range(20):
        time.sleep(2)
        try:
            if "\$" in page.evaluate("document.body.innerText"): break
        except: pass

    time.sleep(1)
    page.mouse.click(640, 300)
    time.sleep(0.5)
    page.keyboard.type(DEPLOY_CMD)
    page.keyboard.press("Enter")
    print("      指令已送出，等待執行...")
    time.sleep(12)
    page.screenshot(path="/tmp/beauty_deploy_result.png")
    browser.close()

print("      PA 部署完成（截圖：/tmp/beauty_deploy_result.png）")
PYEOF

echo ""
echo "✓ 部署完成：https://sisisleeping.pythonanywhere.com"
