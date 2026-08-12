# 美咖美容 VIP 管理系統

兩間美容分店使用的顧客、消費、美咖幣、會員等級、升級禮與 SPA 預約管理系統。

## 主要功能

- 三種交易模式：一般消費、點數扣除、壽星充值
- 會員年度制與 S／P／A 等級升降、升級禮追蹤
- 美咖幣分批效期、FIFO 折抵與人工調整
- 報表、CSV 匯出、通訊錄與資料品質審核
- 公開 SPA 預約、容量控管與後台確認／取消
- 管理者 DB 備份下載

## 本機啟動與驗證

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py

python -m unittest discover -s tests -v
python scripts/db_check.py
```

本機預設網址為 <http://127.0.0.1:5090>。正式環境與資料主權、部署、遷移流程請見 `CLAUDE.md`。

## 必要環境變數

- `APP_SECRET_KEY`：正式環境必須使用高熵隨機值
- `MAIN_PIN`：員工入口 PIN
- `MANAGER_PIN`：管理入口 PIN
- `SESSION_COOKIE_SECURE`：HTTPS 正式環境維持 `1`；純 HTTP 本機測試才設 `0`

正式資料庫位於 PythonAnywhere；本機 `data/beauty_vip.db` 僅是參考副本，禁止反向覆蓋生產資料。

## 安全與資料契約

- 財務交易採作廢保留，不做硬刪除。
- SPA 預約取消保留原紀錄；容量檢查以 SQLite 寫入鎖避免同時超賣。
- 顧客刪除僅限管理者，並同步清除依賴帳本，避免孤兒資料。
- 所有敏感變更請先跑測試、`db_check.py`、秘密掃描，再部署並做 live smoke test。
