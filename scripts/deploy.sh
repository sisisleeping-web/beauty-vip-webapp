#!/usr/bin/env bash
# 把本機 beauty-vip-webapp 的最新變更推到 GitHub，再同步到 PythonAnywhere 正式環境。
# 用法：scripts/deploy.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$HOME/.config/beauty-vip/pythonanywhere.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "找不到 $ENV_FILE，請先設定 PYTHONANYWHERE_USERNAME / PYTHONANYWHERE_DOMAIN / PYTHONANYWHERE_API_TOKEN / PYTHONANYWHERE_BROWSER_ACT_ID" >&2
    exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${PYTHONANYWHERE_USERNAME:?missing}"
: "${PYTHONANYWHERE_DOMAIN:?missing}"
: "${PYTHONANYWHERE_API_TOKEN:?missing}"
: "${PYTHONANYWHERE_BROWSER_ACT_ID:?missing}"

API_BASE="https://www.pythonanywhere.com/api/v0/user/${PYTHONANYWHERE_USERNAME}"
AUTH_HEADER="Authorization: Token ${PYTHONANYWHERE_API_TOKEN}"
BA_SESSION="beauty-vip-deploy-$$"

cleanup() {
    browser-act session close "$BA_SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$REPO_DIR"

if [ -n "$(git status --porcelain)" ]; then
    echo "提醒：本機還有未 commit 的修改（僅供參考，不會被推上去，git push 只送出已 commit 的內容）：" >&2
    git status --short >&2
fi

echo "==> git push origin main"
git push origin main

echo "==> 在 PythonAnywhere 建立 console"
CONSOLE_ID=$(curl -sS -X POST "${API_BASE}/consoles/" \
    -H "$AUTH_HEADER" \
    -d "executable=bash" -d "arguments=" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

echo "==> 喚醒 console（API 建立的 console 預設休眠，需開一次頁面才會啟動 pty）"
browser-act --session "$BA_SESSION" browser open "$PYTHONANYWHERE_BROWSER_ACT_ID" \
    "https://www.pythonanywhere.com/user/${PYTHONANYWHERE_USERNAME}/consoles/${CONSOLE_ID}/" >/dev/null
sleep 10

REMOTE_CMD="cd ~/beauty-vip-webapp && git pull --ff-only && echo '__UPDATE_DONE__'"

curl -sS -X POST "${API_BASE}/consoles/${CONSOLE_ID}/send_input/" \
    -H "$AUTH_HEADER" \
    --data-urlencode "input=${REMOTE_CMD}
" > /dev/null

echo "==> 等待遠端指令執行完成"
for _ in $(seq 1 30); do
    sleep 2
    OUTPUT=$(curl -sS "${API_BASE}/consoles/${CONSOLE_ID}/get_latest_output/" \
        -H "$AUTH_HEADER" | python3 -c "import sys,json; print(json.load(sys.stdin).get('output',''))" || true)
    if echo "$OUTPUT" | grep -q "__UPDATE_DONE__"; then
        echo "$OUTPUT"
        break
    fi
done

if ! echo "${OUTPUT:-}" | grep -q "__UPDATE_DONE__"; then
    echo "遠端指令逾時或失敗，請人工檢查 console id=${CONSOLE_ID}" >&2
    echo "https://www.pythonanywhere.com/user/${PYTHONANYWHERE_USERNAME}/consoles/${CONSOLE_ID}/" >&2
    exit 1
fi

curl -sS -X DELETE "${API_BASE}/consoles/${CONSOLE_ID}/" -H "$AUTH_HEADER" > /dev/null || true

echo "==> Reload web app"
curl -sS -X POST "${API_BASE}/webapps/${PYTHONANYWHERE_DOMAIN}/reload/" -H "$AUTH_HEADER"
echo
echo "==> 完成。檢查 https://${PYTHONANYWHERE_DOMAIN}"
