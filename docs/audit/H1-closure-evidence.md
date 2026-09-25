# H-1 Closure Evidence

Baseline Audit (`AUDIT-2026-Q3-BASELINE.md`，`PASS_WITH_FINDINGS`）**不改寫**，
永遠保留為原始稽核結果。這是 H-1 這條 finding 的收尾證據，記錄從發現到最終修復
的完整鏈路。

```text
ORIGINAL_FINDING=H-1（update_transaction 與 entry() 有效等級判定不一致）
FUTURE_CALCULATION_FIX=df55d1d（effective_tier_for_transaction 共用 contract）
LEGACY_CUSTOMERS_VERIFIED=286

OVER_CREDIT_CONFIRMED_CUSTOMERS=70
OVER_CREDIT_CONFIRMED_POINTS=34103
OVER_CREDIT_POLICY=COMPANY_ABSORBS_NO_CLAWBACK（Owner 已核准，本次未修改這 70 位）

HISTORICAL_SOURCE_INCOMPLETE_CUSTOMERS=7
HISTORICAL_INCOMPLETE_POLICY=NO_AUTO_REMEDIATION（Owner 已核准，本次未處理）

UNDER_CREDIT_CONFIRMED_CUSTOMERS=1
UNDER_CREDIT_REMEDIATED_CUSTOMERS=1
UNDER_CREDIT_REMEDIATED_POINTS=96
CUSTOMER_ID=380
CUSTOMER_NAME=林秋蘭
POINT_ADJUSTMENT_ID=5
CORRECTION_BATCH_ID=429
CREDIT_DATE=2026-09-25
EXPIRY_DATE=2027-09-25
BALANCE_BEFORE=357
BALANCE_AFTER=453

LEGACY_SCRIPTS_REVIEWED=4（fix_coins_history.py／backfill_member_tier.py／
  backfill_tier_upgrades.py／pa_migrate.py）
LEGACY_SCRIPTS_CONTAINED=4（全部已加執行前 guard，預設 refuse，需明確環境變數
  才能跑；本機與 PA 上的副本皆已同步更新且雜湊核對一致）
DANGEROUS_UNCONTAINED_PATHS=0

SYSTEM_AUDIT_SERVER_SIDE_AUTH=CONFIRMED（4 個 mutation endpoint 原本就有
  `if not session.get("manager_authed"): return "Forbidden", 403`；本次額外修正
  的是「detail 資料本身」原本沒有依權限分流查詢/渲染的問題——severity 數字／
  report_ref／完整歷史原本會無條件查出並送進 template context，只是template
  沒有渲染出來，不是真正意義上的「server 端沒有」。已改成 is_manager 才查詢/
  傳遞那些欄位）
SYSTEM_AUDIT_DIRECT_MANAGER_AUTH=DEPLOYED（/admin/system-audits 頁面內嵌
  PIN 表單，POST 到既有 /manager/unlock，帶 next=/admin/system-audits，成功後
  same-page redirect 回原頁；沿用同一個 manager_authed session，沒有建第二套
  PIN/驗證機制）
SYSTEM_AUDIT_MOBILE_FIX=DEPLOYED（「目前狀態」卡片從 `<table>` 改成
  flex+word-break 的 key-value layout；「稽核歷史」新增 `.audit-history-mobile`
  卡片版面，`@media (max-width:600px)` 切換顯示，桌面維持原本 table）

H1_STATUS=CLOSED
```

## Gate E — Final Acceptance（READ-ONLY，2026-09-25）

- `PRAGMA integrity_check` = **ok**
- Production `customers` 表：`id=380` `coin_balance=453`（直接查證，非快取推測）
- `coin_batches`：`id=429` 恰好一筆，`customer_id=380`，
  `source_adjustment_id=5`，`earned_amount=96`，`remaining_amount=96`，
  `status=active`，`is_legacy=0`，`credit_date=2026-09-25`，
  `expires_date=2027-09-25`。顧客 380 目前共 3 筆批次
  （165 legacy + 192 legacy + 96 本次修正 = 453，算術對得上）。
- `point_adjustments id=5`：`customer_id=380`／`points=96`／
  `operator=manager`／`created_at=2026-09-25T00:19:56`／`reason` 完整敘明
  H-1 根因（交易 id=678、應得費率、獨立重算交叉驗證來源、稽核文件路徑）——
  customer/points/operator/timestamp/reason/source 六項全部可追溯。
- **全表逐列比對**（不只是 row count）：把這次 Gate A 備份
  （`data/backups/deploy_20260925_073129_pre_gate_a/`）跟 Gate E 現在下載的
  正式 DB 副本，`customers`／`coin_batches`／`point_adjustments` 三張表逐列
  diff——**除了顧客 380 的 `coin_balance`（357→453）跟新增的 2 列
  （`coin_batches id=429`、`point_adjustments id=5`）以外，其餘全部逐位元組
  相同**。`transactions`／`coin_redemptions`／`tier_upgrades`／
  `review_flags` 四張表列數也完全一致。`UNRELATED_FINANCIAL_MUTATIONS=0`
  是實測結果，不是假設。

### 一個誠實回報、跟 H-1 無關的意外發現

`scripts/db_check.py` 這次執行**回報 FAIL**（exit=1），3 位顧客（吳慧娟、
黃舒郁、周宥薰——都不是顧客 380）的 `coin_balance` 快取低於批次實際可用總和。
追查後確認：這 3 位剛好都有一筆一般消費賺點的批次 `credit_date=2026-09-25`
（正是今天，V3「次月才入帳可用」規則的入帳日剛好在稽核當下到期），而
`customers.coin_balance` 是懶更新快取（`sync_all_coin_balances` 只在有人
真的瀏覽頁面時才重算，不是排程自動跑），還沒被任何一次頁面瀏覽觸發重新整理。
逐一驗證：

```text
吳慧娟：cached 5259 = 舊批次 5259；fresh 6378 = 5259 + 今天入帳 1119 → 對得上
黃舒郁：cached 1122 = 舊批次 1122；fresh 1527 = 1122 + 今天入帳 405   → 對得上
周宥薰：cached  385 = 舊批次  385；fresh  929 =  385 + 今天入帳 544   → 對得上
```

這是系統既有、設計內的暫時性快取滯後（下次這 3 位有任何頁面被瀏覽就會自動
校正，不需要任何修復動作），跟本次 +96 修正無關：(1) 受影響顧客完全不重疊，
(2) `h1_remediate_customer_380.py` 的 `sync_coin_balance()` 呼叫寫死
`customer_id=380`，結構上不可能動到其他顧客的 row，(3) 全表逐列 diff 已經
證明這 3 位顧客的 `coin_balance` 數值本身跟 Gate A 前完全相同（沒有被本次
操作改變過，只是「本來就快取滯後」被 db_check 抓到）。分類為
`FOLLOW_UP`／`ACCEPTED_HISTORICAL_RISK`（系統既有機制，非本次引入，
不阻擋 H-1 CLOSED），如實記錄，未自行修復。

### Manager／`/my` 顯示一致性

沒有另外打正式站的 `/manager` 或 `/my`（會觸發那兩個頁面各自的
`sync_all_coin_balances`／`sync_coin_balance` 呼叫，屬於「額外」的寫入
動作，這次 Gate E 明確要求不得有任何額外 financial write，所以沒有做）。
改用兩項已經足夠確立信心的證據：(1) 直接查證 `customers.coin_balance=453`
——這正是 Manager 頁與 `/my` 頁兩者都讀取的同一個欄位；(2) 本次任務前段已經
讀程式碼確認 `_build_customer_result()`（`/my` 背後）docstring 明講「never
derive a second balance in the browser」，實作呼叫的就是 `sync_coin_balance()`
這個唯一寫入點，Manager 頁也是同一套。`MANAGER_BALANCE_MATCH` /
`MY_BALANCE_MATCH` 標記 **CONFIRMED_VIA_SHARED_SOURCE**（資料庫值 + 程式碼
路徑雙重確認），不是靠真人在瀏覽器裡點開頁面看到的畫面驗收——如果需要那種
真人視覺驗收，屬於 Owner optional 事項。

## 完整 Gate 結果

```text
CUSTOMER_ID=380
EXPECTED_CORRECTION=96
ACTUAL_CORRECTION=96
CORRECTION_BATCH_ID=429
CREDIT_DATE=2026-09-25
EXPIRY_DATE=2027-09-25
DUPLICATE_PROTECTION=CONFIRMED（idempotency guard 已用單元測試驗證）
OVER_CREDIT_CUSTOMERS_MUTATED=0
HISTORICAL_INCOMPLETE_CUSTOMERS_MUTATED=0
UNRELATED_FINANCIAL_MUTATIONS=0（全表逐列 diff 證實）

INTEGRITY_CHECK=ok
DB_CHECK=FAIL（3 位顧客快取滯後，已查明與 H-1／本次修正無關，見上）
FINANCIAL_DIFF=+96（顧客 380 一筆，其餘全部逐位元組相同）

MANAGER_BALANCE_MATCH=CONFIRMED_VIA_SHARED_SOURCE
MY_BALANCE_MATCH=CONFIRMED_VIA_SHARED_SOURCE
MY_INDEPENDENT_FORMULA=NOT_FOUND

PRODUCTION_DB_OVERWRITTEN=NO
NEGATIVE_POINTS_CREATED=NO
CLAWBACK_EXECUTED=NO
REAL_CUSTOMER_NOTIFICATION_SENT=NO
ADDITIONAL_FINANCIAL_WRITES_DURING_GATE_E=0

H1_STATUS=CLOSED
```
