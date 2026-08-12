#!/usr/bin/env bash
# 推送 main，備份正式 DB，透過 PythonAnywhere Files API 同步程式並 reload。
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$HOME/.config/beauty-vip/pythonanywhere.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "找不到 $ENV_FILE" >&2
    exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

: "${PYTHONANYWHERE_USERNAME:?missing}"
: "${PYTHONANYWHERE_DOMAIN:?missing}"
: "${PYTHONANYWHERE_API_TOKEN:?missing}"

API_BASE="https://www.pythonanywhere.com/api/v0/user/${PYTHONANYWHERE_USERNAME}"
AUTH_HEADER="Authorization: Token ${PYTHONANYWHERE_API_TOKEN}"
REMOTE_BASE="${API_BASE}/files/path/home/${PYTHONANYWHERE_USERNAME}/beauty-vip-webapp"

cd "$REPO_DIR"
if [ -n "$(git status --porcelain --untracked-files=no | grep -v '^ M current_tunnel_url.txt$' || true)" ]; then
    echo "有未 commit 的 tracked 修改，停止部署：" >&2
    git status --short >&2
    exit 1
fi

git push origin main

BACKUP_DIR="data/backups/deploy_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR/data"
DB_CODE=$(curl -sS -o "$BACKUP_DIR/data/beauty_vip.db" -w '%{http_code}' \
    -H "$AUTH_HEADER" "$REMOTE_BASE/data/beauty_vip.db")
if [ "$DB_CODE" != "200" ]; then
    echo "正式 DB 備份失敗（HTTP $DB_CODE），停止部署" >&2
    exit 1
fi
python3 - "$BACKUP_DIR/data/beauty_vip.db" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
result = db.execute("PRAGMA integrity_check").fetchone()[0]
db.close()
if result != "ok":
    raise SystemExit(f"正式 DB 完整性檢查失敗：{result}")
PY

echo "==> 同步 PythonAnywhere 程式檔"
git ls-files -z -- app.py rules.json requirements.txt Procfile 'templates/*.html' \
    | while IFS= read -r -d '' FILE; do
        CODE=$(curl -sS -o /tmp/beauty-vip-upload-response -w '%{http_code}' \
            -X POST "$REMOTE_BASE/$FILE" -H "$AUTH_HEADER" -F "content=@$FILE")
        case "$CODE" in 200|201) ;; *) echo "上傳失敗：$FILE（HTTP $CODE）" >&2; exit 1;; esac
        LOCAL_HASH=$(shasum -a 256 "$FILE" | awk '{print $1}')
        REMOTE_HASH=$(curl -sS -H "$AUTH_HEADER" "$REMOTE_BASE/$FILE" | shasum -a 256 | awk '{print $1}')
        test "$LOCAL_HASH" = "$REMOTE_HASH" || { echo "遠端 hash 不一致：$FILE" >&2; exit 1; }
        echo "    $FILE ok"
    done

RELOAD_CODE=$(curl -sS -o /tmp/beauty-vip-reload-response -w '%{http_code}' \
    -X POST "$API_BASE/webapps/$PYTHONANYWHERE_DOMAIN/reload/" -H "$AUTH_HEADER")
test "$RELOAD_CODE" = "200" || { echo "Reload 失敗（HTTP $RELOAD_CODE）" >&2; exit 1; }

STATUS=$(curl -sS -o /dev/null -w '%{http_code}' "https://${PYTHONANYWHERE_DOMAIN}/spa/booking")
test "$STATUS" = "200" || { echo "正式站驗證失敗（HTTP $STATUS）" >&2; exit 1; }
echo "==> 部署完成：https://${PYTHONANYWHERE_DOMAIN}"
echo "注意：若本次 app.py 新增 DB schema，仍須先在 PA console 執行 app.init_db() 再 reload。"
