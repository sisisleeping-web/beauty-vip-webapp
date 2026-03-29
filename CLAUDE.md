# 美咖美容 VIP 管理系統 — CLAUDE.md

## 專案概覽
| 項目 | 說明 |
|------|------|
| 用途 | 兩間美容分店（斗六店、虎尾店）的 VIP 顧客消費管理 |
| 技術棧 | Python 3 + Flask + SQLite，純 Vanilla JS 前端 |
| 線上網址 | https://sisisleeping.pythonanywhere.com |
| 雲端倉庫 | https://github.com/sisisleeping-web/beauty-vip-webapp |
| 本機路徑 | /Users/openclaw/Projects/beauty-vip-webapp |
| DB 路徑 | data/beauty_vip.db（gitignored，PA 是生產 DB）|

---

## 系統架構

```
beauty-vip-webapp/
├── app.py               # Flask 主程式（全部路由 + 商業邏輯）
├── rules.json           # VIP 等級規則、壽星充值（不含9折，已移除）
├── templates/
│   ├── base.html        # 共用版型（導覽列、樣式）
│   ├── home.html        # 首頁（選分店入口）
│   ├── entry.html       # 美容師輸入頁（三模式：一般/扣點/壽星充值）
│   ├── report.html      # 報表查詢（月/年彙總、行內編輯）
│   ├── manager.html     # 管理後台（需 PIN）
│   ├── contacts.html    # 顧客通訊錄
│   └── review.html      # 資料稽核（可疑生日、重複姓名）
├── data/
│   └── beauty_vip.db   # SQLite DB（gitignored）
├── scripts/
│   ├── deploy.sh        # 一鍵部署到 PythonAnywhere
│   ├── db_check.py      # 資料完整性驗算
│   └── db_backup.py     # 本機 DB 備份
├── rules.json
└── requirements.txt
```

---

## 資料庫 Schema

### customers
| 欄位 | 型態 | 說明 |
|------|------|------|
| id | INTEGER PK | 自動遞增 |
| name | TEXT | 姓名 |
| phone | TEXT | 手機（選填）|
| birthday | TEXT | 生日 YYYY-MM-DD |
| created_at | TEXT | 建立時間 ISO |
| coin_balance | INTEGER | 當前點數餘額 |
| UNIQUE | (name, birthday) | 同名同生日視為同一人 |

### transactions
| 欄位 | 型態 | 說明 |
|------|------|------|
| id | INTEGER PK | |
| customer_id | INTEGER FK | |
| store_id | TEXT | store_a/store_b |
| txn_date | TEXT | YYYY-MM-DD |
| month_key | TEXT | YYYY-MM |
| amount | REAL | 美容師 key 入的原始實收金額 |
| final_amount | REAL | 應等於 amount（無折扣機制）|
| birthday_discount_applied | INTEGER | 永遠為 0（折扣機制已移除）|
| cashback | REAL | 回饋金（僅顯示用，不影響 balance）|
| coins_earned | INTEGER | 本筆獲得點數 |
| coins_redeemed | INTEGER | 本筆扣除點數 |
| entry_mode | TEXT | normal / coin_deduct / birthday_recharge |
| recharge_plan | TEXT | 壽星充值方案（10000/20000/30000）|
| recharge_amount | REAL | 壽星充值金額 |

### stores
| id | name |
|----|------|
| store_a | 斗六店 |
| store_b | 虎尾店 |

---

## 商業邏輯

### 交易三模式

**1. 一般消費（normal）**
- 美容師輸入實收金額（amount），無任何折扣
- final_amount = amount（恆等，birthday_discount 已移除）
- 依 VIP 等級計算 cashback_rate 和 points_rate
- 點數 = int(final_amount × points_rate)

**2. 點數扣除（coin_deduct）**
- 扣除顧客累積點數，不記入消費金額
- 不影響 VIP 等級計算

**3. 壽星充值（birthday_recharge）**
- 僅限當月壽星（birthday.month == txn_date.month）
- 三方案：10000元→500點 / 20000元→1000點 / 30000元→1500點
- 不記入消費金額，純加點

### VIP 等級（依單筆最高 or 年累計）
| 等級 | 單筆 OR 年累計 | cashback | points |
|------|--------------|----------|--------|
| 一般會員 | 無門檻 | 2% | 2% |
| S級美咖 | ≥8,000 OR ≥15,000 | 3% | 3% |
| P級美咖 | ≥12,000 OR ≥24,000 | 5% | 5% |
| A級美咖 | ≥30,000 OR ≥60,000 | 8% | 8% |

> 等級以「本筆交易之前」的累計計算（不含本筆）

### 點數計算規則
- coin_balance = Σ(coins_earned 含 normal+birthday_recharge) − Σ(coins_redeemed)
- 刪除交易時自動回滾 coin_balance
- 修改交易時先撤銷舊影響再套用新影響

---

## 部署架構

### PythonAnywhere（生產環境）

- 用戶：sisisleeping
- WSGI：/var/www/sisisleeping_pythonanywhere_com_wsgi.py
- DB 路徑（PA）：~/beauty-vip-webapp/data/beauty_vip.db
- Reload 方式：`touch /var/www/sisisleeping_pythonanywhere_com_wsgi.py`

### 部署流程（程式碼）

1. 本機：`git push origin main`
2. PA Bash console：`cd ~/beauty-vip-webapp && git pull --ff-only && touch /var/www/sisisleeping_pythonanywhere_com_wsgi.py`

---

## ⚠️ 資料主權：雲端 DB 為唯一真相來源

```text
【鐵則】PA 上的 beauty_vip.db = 生產資料 = 唯一正確來源
         本機 data/beauty_vip.db = 參考副本，可能過期
```

### 正確資料流向

```text
PA DB（雲端）→ 下載備份 → 本機（參考用）
                ↑
          美容師從網頁輸入資料
```

### 取得最新資料到本機

1. 登入 <https://sisisleeping.pythonanywhere.com/manager>
2. 點「備份下載」→ 下載 `beauty_vip_backup_YYYYMMDD_HHMMSS.db`
3. 覆蓋到本機 `data/beauty_vip.db`

### 資料修復原則

- 任何 DB 資料修復腳本必須在 **PA 上執行**，不是本機
- 流程：撰寫腳本 → 上傳到 PA → PA console 執行 → 驗算 → 刪腳本
- 永遠不要把本機 DB 推覆蓋 PA DB

### 快速腳本

```bash
./scripts/deploy.sh          # 一鍵部署程式碼（不含 DB）
python3 scripts/db_check.py  # 驗算本機 DB（或指定路徑）
python3 scripts/db_backup.py # 備份本機 DB 到 data/backups/（本機副本用）
# scripts/pa_migrate.py      # 上傳到 PA 執行的修復工具包
```

---

## ⚠️ 已知歷史問題與修復記錄

### 2026-03-29 — 壽星9折折扣錯誤回補
**問題**：系統曾錯誤套用壽星9折，但美容師輸入的就是實收金額，應無折扣
**修復**：
- 移除 entry 流程的折扣計算，`discount_applied = False`，`final_amount = amount`
- 34 筆歷史記錄 `final_amount` 回補為 `amount`，折讓總額 NT$57,731
- 34 位受影響顧客 `coins_earned` 重算、`coin_balance` 修正
- 移除 report 頁的「壽星折扣」欄位

### 2026-03-29 — update_transaction NameError Bug 修復
**問題**：`update_transaction` 路由未解析 form data，直接使用未定義變數
**影響**：報表頁「✏️ 修改」功能完全無法使用（500 崩潰）
**修復**：在函數頂部加入完整的 `request.form` 解析

---

## 環境設定

```bash
pip install -r requirements.txt
python3 app.py  # 本機開發 port 5000
```

**環境變數**
- `APP_SECRET_KEY`：Flask session 金鑰（預設 beauty-vip-demo）
- `MANAGER_PIN`：管理後台 PIN（預設 1225）
