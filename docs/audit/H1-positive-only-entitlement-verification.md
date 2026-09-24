# Positive-Only Entitlement Verification + Calculation Core Consistency Review

- **前置版本**：`H1-legacy-opening-balance-verification.md`（未納入 point_adjustments，
  該版 78 位/+31,905 點僅為 preliminary reconciliation evidence，不作為補發/扣回依據）
- **本次**：納入全部合法來源後的正式 reconciliation
- **模式**：READ-ONLY（來源 DB `mode=ro`；重放全在隔離暫存 SQLite，未寫回 Production）
- **`PRODUCTION_FINANCIAL_WRITES=0`**
- **附件**：
  - `H1-entitlement-reconciliation-v3-2026-09-25.csv`（286 位逐戶分級）
  - `H1-redemption-anomalies-v3-2026-09-25.csv`（7 筆 HISTORICAL_SOURCE_INCOMPLETE 證據）
  - `H1-point-adjustment-replay-log-2026-09-25.csv`（人工加點重放紀錄）

---

## Part A. Positive-Only Entitlement Verification

### A.1 合法影響點數餘額的 Source-of-Truth 盤點

逐一核對程式碼（不是猜測），確認唯一寫入 `coin_batches`／`customers.coin_balance`
的路徑：

| Source | 機制 | 方向 | 正式費率重算？ |
|---|---|---|---|
| `transactions`（normal） | `/entry` → `effective_tier_for_transaction` → `create_coin_batch` | + | 是（V3 會員年度制費率，H-1 已修） |
| `transactions`（birthday_recharge） | `/entry` → 固定方案 → `create_coin_batch` | + | 否，方案固定點數，非費率算出 |
| `transactions`（coin_deduct） | `/entry` → `redeem_coins_fifo` | − | 不適用（消費點數，非賺點） |
| `point_adjustments`（人工加點） | `/api/customers/<id>/add_points` → `create_coin_batch(immediate=True)` | + only | **否**，`points` 是合法業務輸入，直接採用，不重算 |
| Void/Refund | `/api/transactions/<id>/delete` → `reverse_earning`／`reverse_redemption` | 反向沖銷 | 不適用（沖銷原值，非重算） |
| 交易金額編輯 | `/api/transactions/<id>/update` → `effective_tier_for_transaction` | ± | 是（同上，H-1 已修） |
| 扣點編輯 | `/api/transactions/<id>/update_deduct` → 先 `reverse_redemption` 再 `redeem_coins_fifo` | ± | 不適用 |
| 人工加點編輯/刪除 | `/api/point_adjustments/<id>/update`／`delete` | ± | 否，沿用原值調整或沖銷 |
| Coin expiry | `get_usable_coin_total` 用 `expires_date` 日期過濾，**批次本身不被改寫** | 排除，非扣除 | 不適用 |
| Customer merge | `/api/customers/merge` → 搬移 `coin_batches`/`point_adjustments`/`transactions`/`tier_upgrades`，`_reconcile_tier_after_merge` 重判等級 | 搬移，總量不變 | 等級用真實歷史重放，非重算金額 |
| **2026-08-09 Legacy 整併** | `scripts/backfill_coin_batches.py`：`balance = customers.coin_balance`（當下快照），建 1 筆 `is_legacy=1` 批次 | 快照 | **否**，忠實搬運既有 `coin_balance`，不是獨立計算——它本身不是誤差來源，只是把「當時已經存在的誤差」凍結成快照 |

**結論**：整套系統只有「交易 (`transactions`)」跟「人工加點 (`point_adjustments`)」
兩種**正向**新增點數的合法來源，`coin_deduct` 是唯一**負向**消耗來源，其餘（void/
edit/merge）都是對既有值的沖銷或搬移，不會憑空產生數字。這跟需求裡的公式完全對應：

```
Expected entitlement
  = 系統交易應得（正確費率算出的 coins_earned，H-1 fix 後的 effective_tier_for_transaction）
  + 合法人工調整（point_adjustments.points，原樣採用，不重算）
  − 合法使用（coin_deduct 的 coins_redeemed，FIFO 真實重放）
  − 到期/作廢/退款等正式效果（voided 交易不重放；expires_date 篩選已包含在
    「as of 某日可用」的定義中）
  ± 可證明的 migration/merge adjustment（本次驗證範圍內：0 筆 merge，legacy 整併
    本身是快照非計算，不產生獨立誤差）
```

### A.2 Point Adjustments 資料完整性稽核

全庫僅 **2 筆**人工加點紀錄，**customer / points / operator / created_at / reason
五個欄位全部有值，無缺漏**：

| id | 顧客 | 點數 | 原因 | 操作者 | 時間 | 是否列入本次重放 |
|---|---|---|---|---|---|---|
| 3 | 280 廖佩瑛 | +600 | 其他手動加點 | staff | 2026-08-07（整併前） | 是 |
| 4 | 536 | +100 | 填寫線上問卷點數回饋 | staff | 2026-08-19（整併後） | 否（不影響 2026-08-09 opening balance） |

無缺欄位，**沒有 audit finding 需要列**；也沒有自行補造任何資料。

### A.3 重放方法修正

上一版（v2）用 HTTP POST 到 `/api/customers/<id>/add_points` 重放人工加點，結果
發現這條路由**內部寫死 `date.today()`**（給「現在」用的即時操作路由，不像
`/entry` 有開放 `txn_date` 可回填歷史日期）——重放時會把批次的 `credit_date`
灌成腳本**實際執行當天**，而不是這筆加點**真正發生**的日期，導致重放算出的
「2026-08-09 可用點數」漏算了它。

修正：改成直接呼叫 `add_points` 背後同一支正式函式 `create_coin_batch()`
（不是另外寫一套邏輯），只是把日期換成這筆調整的真實歷史日期——語意跟正式路由
完全一致，只是繞過路由層「只能用今天」的介面限制。

### A.4 全部 286 位 Legacy 客戶重新分級

```text
EXACT_MATCH                = 208
OVER_CREDIT_CONFIRMED      = 70（合計 34,103 點）
UNDER_CREDIT_CONFIRMED     = 1（96 點，HIGH confidence）
HISTORICAL_SOURCE_INCOMPLETE = 7（MEDIUM confidence，見下）
MANUAL_ADJUSTMENT_EXPLAINED = 0（全庫僅 1 筆整併前人工加點，套用後仍有殘餘落差，
                                 沒有任何顧客的落差是「完全」由人工加點解釋掉的）
```

**HISTORICAL_SOURCE_INCOMPLETE 的 7 位**（顧客 224、254、276、351、413、415、
446）：重放時發現他們在整併前的某筆扣點，**在正確重算下當時點數不足**——追查
`transactions`／`point_adjustments`／`customer_merge_operations` 三張表都找不到
對應來源，代表他們的點數歷史存在本次資料查不到的缺口（可能是比 2026-08-09
更早、沒有清楚稽核軌跡的資料搬遷）。**依 Owner 政策，MEDIUM confidence 一律不
自動補發**，這 7 位維持不處理。

### A.5 Owner 政策套用後的最終結果

- **歷史多發（OVER_CREDIT_CONFIRMED，70 位／34,103 點）**：公司吸收，不追回、
  不扣餘額、不製造負點。**本次不執行任何動作。**
- **歷史少發，唯一進入 Positive-Only Remediation Candidate 的**：

  | 顧客 | 整併記錄 opening balance | 完整重算後應有 | 缺口 |
  |---|---|---|---|
  | 林秋蘭（id=380） | 357 | 453 | **+96** |

  這是**全部 286 位裡，唯一同時滿足「HIGH confidence」+「完整納入合法人工調整
  後仍確認少發」兩個條件的顧客**。她沒有 redemption anomaly、沒有人工加點涉入，
  純粹是交易費率重算後應該多 96 點。
- **MEDIUM/LOW confidence 或 HISTORICAL_SOURCE_INCOMPLETE（7 位）**：依政策
  不自動補發，維持現狀。

**PRODUCTION_FINANCIAL_WRITES=0**：以上全部只是分級與盤點，沒有對正式
`coin_batches`／`customers.coin_balance`／任何資料表寫入任何一筆。

---

## Part B. Calculation Core Entry-Point Review

### B.1 分類定義

- **SHARED_CORE**：正式、當下即時流量在用的唯一計算引擎函式/路由。
- **SAFE_MANUAL_ADJUSTMENT**：合法人工輸入的介面，不重算費率，直接採用業務
  輸入或呼叫 SHARED_CORE 函式本身。
- **READ_ONLY**：純查詢/顯示，不寫入。
- **LEGACY_ONLY**：一次性遷移/清理用途，正常情況不該再執行第二次。
- **DANGEROUS_IF_RERUN**：重跑會用錯的邏輯覆寫正確資料。
- **DUPLICATED_BUSINESS_LOGIC**：跟 SHARED_CORE 各自維護一份不保證同步的等級/
  費率判斷邏輯。

### B.2 Routes（`app.py`）

| Route | 分類 | 說明 |
|---|---|---|
| `POST /entry`（normal/birthday_recharge/coin_deduct） | **SHARED_CORE** | 呼叫 `effective_tier_for_transaction`／`create_coin_batch`／`redeem_coins_fifo` |
| `POST /api/transactions/<id>/update` | **SHARED_CORE** | H-1 修好後跟 `/entry` 共用 `effective_tier_for_transaction` |
| `POST /api/transactions/<id>/delete`（void） | **SHARED_CORE** | `reverse_earning`／`reverse_redemption`／`reevaluate_tier_after_void` |
| `POST /api/transactions/<id>/update_deduct` | **SHARED_CORE** | `reverse_redemption` + `redeem_coins_fifo` |
| `POST /api/customers/<id>/add_points` | **SAFE_MANUAL_ADJUSTMENT** | 直接採用人工輸入點數，呼叫 `create_coin_batch`，不重算費率 |
| `POST /api/point_adjustments/<id>/update` | **SAFE_MANUAL_ADJUSTMENT** | 沿用/更新既有人工調整值，呼叫 `adjust_coin_batch_for_adjustment_edit` |
| `POST /api/point_adjustments/<id>/delete` | **SAFE_MANUAL_ADJUSTMENT** | `reverse_earning` 沖銷 |
| `GET /api/customers/<id>/point_adjustments` | **READ_ONLY** | |
| `POST /api/customers/merge` | **SHARED_CORE** | 搬移帳本 + `_reconcile_tier_after_merge` 用真實歷史重判等級 |
| `GET /api/customers/merge/preview` | **READ_ONLY** | |
| `POST /api/customer-merges/<id>/undo` | **SHARED_CORE** | 還原搬移並重判 |
| `GET /upgrades` | **READ_ONLY** | |
| `POST /api/upgrades/<id>/deliver`／`/skip`／`/reopen` | **SAFE_MANUAL_ADJUSTMENT** | 只改 `gift_status`，不碰點數/等級計算 |
| `POST /manager/reevaluate_expired` | **SHARED_CORE** | 批次呼叫 `reevaluate_and_persist_tier` |
| `POST /review/update_birthday/<id>` | **SAFE_MANUAL_ADJUSTMENT** | 修正生日資料本身，不重算歷史點數 |
| `POST /review/mark/<type>/<key>` | **READ_ONLY**（狀態註記） | 只改 `review_flags.status` |

### B.3 Scripts（`scripts/`）

| Script | 分類 | 依據 |
|---|---|---|
| `fix_coins_history.py` | **DANGEROUS_IF_RERUN + DUPLICATED_BUSINESS_LOGIC** | 已於上一階段 containment（執行前 guard，需明確環境變數才能跑）；跟 `update_transaction`（H-1 bug）同一套跟 V3 脫鉤的門檻公式，且有 98% 證據顯示已經對正式帳本跑過 |
| `backfill_member_tier.py` | **DUPLICATED_BUSINESS_LOGIC + DANGEROUS_IF_RERUN** | 自述用「all-time 最高單筆 + 目前日曆年累計」，跟 V3 會員年度制脫鉤；雖有「已有 tier_effective_date 就跳過」的保護，但對從未跑過的顧客仍會寫入不準確的錨點（`reanchor_member_tier_from_history.py` 的存在本身就是在修正這支的已知瑕疵） |
| `backfill_tier_upgrades.py` | **DUPLICATED_BUSINESS_LOGIC + DANGEROUS_IF_RERUN** | 檔案內硬編碼 `>= 30000`／`>= 8000` 等門檻常數，未 `import app`，完全獨立一份等級判斷邏輯 |
| `pa_migrate.py` | **DUPLICATED_BUSINESS_LOGIC + DANGEROUS_IF_RERUN** | 同上，`get_tier()`/`tier_name()` 各自硬編碼門檻（Baseline finding M-1 已記錄） |
| `fix_lin_siyin.py` | **DUPLICATED_BUSINESS_LOGIC + LEGACY_ONLY** | 單一顧客的一次性修正腳本，內含自己的門檻判斷；已完成階段性任務，不該再執行 |
| `backfill_coin_batches.py` | **LEGACY_ONLY** | 2026-08-09 整併腳本本身；純粹搬運既有 `coin_balance` 快照，不重算，有「已有 legacy 批次就跳過」保護，重跑安全但無意義 |
| `reconcile_legacy_coin_batches.py` | **SAFE_MANUAL_ADJUSTMENT** | `import app as beauty`，只處理「可機器驗證、尚未被使用」的候選，fail-closed，不重算費率 |
| `repair_merged_customer_tier.py` | **SAFE_MANUAL_ADJUSTMENT** | `import app as beauty`，呼叫正式函式，有 `--dry-run`，鎖定單一 `--customer-id` |
| `reanchor_member_tier_from_history.py` | **SAFE_MANUAL_ADJUSTMENT + LEGACY_ONLY** | `import app as beautyapp`，直接呼叫正式 `reevaluate_and_persist_tier()`，用真實歷史 `tier_upgrades.upgrade_date` 錨定，是目前唯一同時「重算」又「不重造一套邏輯」的遷移腳本 |
| `db_check.py` | **READ_ONLY** | 純 SQL 聚合驗算，不呼叫任何等級計算函式，本次多次拿來當獨立驗證用 |
| `db_backup.py`／`sync_from_cloud.py` | **READ_ONLY** | 備份/下載，不涉及計算 |

**只 review，沒有為了架構漂亮做全面重構**：`backfill_member_tier.py`、
`backfill_tier_upgrades.py`、`pa_migrate.py` 這三支跟 `fix_coins_history.py`
同款風險，但**這次任務沒有對它們做任何 containment 或程式碼變動**——上次只
點名 `fix_coins_history.py`，這三支維持原樣，如果要一併圍堵需要另外授權。

---

## Part C. 資料流驗證

**要求**：美容師輸入交易事實／合法人工調整 → 唯一 Calculation Core →
coin/tier source-of-truth → Manager / Action Board / 顧客 `/my` / Audit 自然連動；
`/my` 不得維護獨立餘額公式。

**驗證結果（讀程式碼確認，非假設）**：

- `customers.coin_balance` 全庫**只有一處**寫入點：`sync_coin_balance()`
  （`app.py` 第 1161 行），而它本身只是 `get_usable_coin_total()` 的快取，
  後者純粹用日期條件（`status='active' AND credit_date<=? AND expires_date>=?`）
  對 `coin_batches` 做 `SUM`——**沒有第二套餘額公式**。
- `_build_customer_result()`（`/my` 背後的函式）的 docstring 明確寫著：
  「The customer page must explain a balance, never derive a second balance
  in the browser.」實作上呼叫 `sync_coin_balance()` 拿權威值，再用
  `_build_coin_summary_map()` 從同一批 `coin_batches`／`transactions` 組出
  明細——**確認沒有獨立公式**。
- Manager 頁、Action Board、Audit（本次全部稽核腳本）都是讀
  `customers.coin_balance` 或直接對 `coin_batches` 做聚合，同一份
  source-of-truth，沒有分岔。
- **結論：資料流結構本身是健康的（單一 Calculation Core → 單一 source-of-truth
  → 各頁面自然連動），H-1 的問題出在 Calculation Core 內部曾經有兩套實作
  （`update_transaction` vs `entry()`），不是資料流分岔的問題——已在上一階段修好。**

**未來 Positive-Only 補發的技術約束（如果 Owner 核准要補）**：必須走
`create_coin_batch(..., source_adjustment_id=...)`（即 `add_points` 這條
SHARED_CORE 認證過的正式路徑），讓補發的點數變成一筆正常 `coin_batches`
紀錄，`/my`／Manager／Action Board 會自然顯示；**不得**直接
`customers.coin_balance += x`（那會製造出一筆跟任何 `coin_batches` 紀錄
對不上的孤兒數字，違反單一 source-of-truth 原則）。本次**沒有執行**這個補發，
只是先把「如果要做，該怎麼做」的正確路徑記錄下來。

---

## 最終狀態

```text
STATE=POSITIVE_ONLY_ENTITLEMENT_VERIFICATION_COMPLETE

CUSTOMERS_WITH_LEGACY_BATCH=286
EXACT_MATCH=208
OVER_CREDIT_CONFIRMED=70（34,103 點，依政策公司吸收，不處理）
UNDER_CREDIT_CONFIRMED_HIGH_CONFIDENCE=1（顧客 id=380 林秋蘭，96 點）
HISTORICAL_SOURCE_INCOMPLETE=7（MEDIUM confidence，依政策不自動補發）
MANUAL_ADJUSTMENT_EXPLAINED=0

POINT_ADJUSTMENTS_TOTAL=2
POINT_ADJUSTMENTS_DATA_QUALITY_ISSUES=0（全部欄位完整，無需 audit finding）

POSITIVE_ONLY_REMEDIATION_CANDIDATES=1
POSITIVE_ONLY_REMEDIATION_CANDIDATE_TOTAL_POINTS=96

CALCULATION_CORE_ENTRY_POINTS_REVIEWED=15 routes + 11 scripts
DANGEROUS_IF_RERUN_SCRIPTS=4（fix_coins_history.py 已 contain；
  backfill_member_tier.py／backfill_tier_upgrades.py／pa_migrate.py 本次未動）
DUPLICATED_BUSINESS_LOGIC_SCRIPTS=4（同上）
DATA_FLOW_SINGLE_SOURCE_OF_TRUTH=CONFIRMED
MY_PAGE_INDEPENDENT_FORMULA=NOT_FOUND（確認沿用共用核心）

PRODUCTION_FINANCIAL_WRITES=0
HISTORICAL_DATA_MODIFIED=NO
NEGATIVE_BALANCE_CREATED=NO

OWNER_DECISION_GATE=OPEN
NEXT_ACTION=
  1. 是否核准對顧客 380（林秋蘭）補發 96 點（唯一 HIGH-confidence 候選，
     若核准需走 create_coin_batch 正式路徑，不得直接改 coin_balance）。
  2. 70 位 OVER_CREDIT_CONFIRMED／7 位 HISTORICAL_SOURCE_INCOMPLETE 依現有
     政策不處理，是否需要另外備查存檔或知會相關人員。
  3. backfill_member_tier.py／backfill_tier_upgrades.py／pa_migrate.py
     是否也要比照 fix_coins_history.py 做 containment（本次未動，需另外授權）。
```

完成後停在這裡，不自行執行任何 Production remediation。
