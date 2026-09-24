# H-1 Closure Evidence

Baseline Audit (`AUDIT-2026-Q3-BASELINE.md`，`PASS_WITH_FINDINGS`）**不改寫**，
永遠保留為原始稽核結果。這是 H-1 這條 finding 的收尾證據，記錄從發現到（待
Owner 執行）最終修復的完整鏈路。

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
UNDER_CREDIT_REMEDIATED_CUSTOMERS=0（截至本文件撰寫時尚未執行，見下方 Gate D）
UNDER_CREDIT_REMEDIATED_POINTS=0（同上）
CUSTOMER_ID=380
CUSTOMER_NAME=林秋蘭
CORRECTION_BATCH_ID=（Gate D 執行後才會產生，由 Owner 在 PA console 跑
  scripts/h1_remediate_customer_380.py 後回填）
CORRECTION_CREDIT_DATE=（同上，執行當天）
CORRECTION_EXPIRY_DATE=（同上，執行當天 +1 年）

LEGACY_SCRIPTS_REVIEWED=4（fix_coins_history.py／backfill_member_tier.py／
  backfill_tier_upgrades.py／pa_migrate.py）
LEGACY_SCRIPTS_CONTAINED=4（全部已加執行前 guard，預設 refuse，需明確環境變數
  才能跑；本機與 PA 上的副本皆已同步更新且雜湊核對一致）
DANGEROUS_UNCONTAINED_PATHS=0

DB_CHECK=PASS（部署前後皆執行，唯讀副本）
INTEGRITY_CHECK=ok（部署前後皆執行）

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

H1_STATUS=REMEDIATION_PREPARED_AWAITING_OWNER_EXECUTION
```

**尚未 CLOSED 的原因**：本次任務所有「CC 能自己做」的部分（驗證、程式碼修正、
script containment、部署、驗收、regression）都已完成，但 +96 的實際 Production
寫入需要 Owner 在 PA Bash Console 手動執行（這個環境從頭到尾都沒有能直接寫入
正式 DB 的管道——沒有 MAIN_PIN/MANAGER_PIN、Files API 只能讀/傳程式碼檔案）。
待 Owner 執行完 Gate D 並回報結果後，才可以把
`UNDER_CREDIT_REMEDIATED_CUSTOMERS`／`CORRECTION_BATCH_ID` 等欄位填上真實值，
正式宣告 `H1_STATUS=CLOSED`。

## Owner 待執行：Gate D

在 PA Bash Console：

```bash
cd ~/beauty-vip-webapp
python3 scripts/h1_remediate_customer_380.py --dry-run   # 先確認印出 357 → 453
python3 scripts/h1_remediate_customer_380.py             # 實際寫入
```

腳本本身：
- 只對 `customer_id=380` 寫入，不碰任何其他顧客/交易/既有 batch。
- 走正式 `create_coin_batch()`（跟 `/api/customers/<id>/add_points` 同一支函式），
  `expires_date` 用正式 `_add_years()` 算，沒有另外手寫日期邏輯。
- 有 idempotency guard：已經跑過就會直接 refuse，不會重複加點（已用單元測試
  驗證）。
- 執行完會印出 `CORRECTION_BATCH_ID`／`CREDIT_DATE`／`EXPIRES_DATE`／
  `BALANCE_BEFORE`／`BALANCE_AFTER`／`INTEGRITY_CHECK`，請把這幾行貼回來，
  我會拿去更新這份文件並跑 Gate E 驗收（integrity/db_check 再跑一次、確認
  `/my` 顧客 380 頁面顯示 453、確認其他顧客/表格完全沒被動到）。
