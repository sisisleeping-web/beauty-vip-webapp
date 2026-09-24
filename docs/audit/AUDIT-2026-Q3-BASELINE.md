# AUDIT-2026-Q3-BASELINE — 美咖 VIP 系統首次 Baseline Deep Audit

- **執行者**：Claude Code（唯讀模式，AUDIT_MODE=READ_ONLY）
- **執行日期**：2026-09-25
- **稽核對象**：Production DB 唯讀副本（下載時間 2026-09-25，commit `071b842` 之後的正式站狀態）
- **稽核方法**：見「方法論與範圍界線」一節
- **結論**：`PASS_WITH_FINDINGS`（無 CRITICAL；1 項 HIGH；3 項 MEDIUM；其餘 LOW/INFO）
- **本次為第一次稽核**：所有 finding 標記 `BASELINE`（無上一期可比對 NEW/PERSISTING/RESOLVED）

---

## 方法論與範圍界線（誠實揭露，不假裝做了辦不到的事）

「Independent Recalculation」原則要求不能只呼叫 Production 同一支 calculator 再說一致。本次實際採用的方法分三層，各自的證據強度不同：

1. **全量彙總重算（Exhaustive，322 位顧客 / 824 筆有效交易全覆蓋）**：`coin_balance` 是否等於
   `Σcoins_earned(normal+birthday_recharge) − Σcoins_redeemed(coin_deduct)`，以及是否等於
   `coin_batches` 分筆帳本中 `status='active' AND credit_date<=today<=expires_date` 的
   `remaining_amount` 總和。這組查詢完全獨立於 app.py 的等級判定邏輯（`calc_tier`/
   `reevaluate_and_persist_tier`），純粹是帳本層的守恆驗證，用 `scripts/db_check.py`
   （純 SQL 聚合，不呼叫任何等級計算函式）對正式 DB 唯讀副本執行，**全部通過**。
2. **結構化程式碼交叉比對（Exhaustive on code paths，非資料樣本）**：把「建立交易」與「修改
   交易」兩條路徑各自用來決定 `coins_earned` 的等級判定邏輯逐行比對，找出兩者不一致
   （見 Finding H-1）。這是對演算法本身的獨立驗證，不依賴抽樣。
3. **代表性樣本人工追蹤（Sampled，非全量）**：完整追蹤 1 位有複雜作廢紀錄的顧客
   （id 533，7 進 2 出的作廢序列）驗證 void→coin_batches→coin_balance 全鏈路；驗證全部
   9 筆壽星充值交易（100% 覆蓋，因為筆數少可以全查）金額/點數/月份合規性。**沒有**逐筆
   手動重算全部 806 筆 normal 交易的點數計算（那等於重寫一份完整的 V3 會員年度制引擎，
   風險是引入審核者自己的新 bug，且超出本次時間範圍）——這是本次唯一的重大範圍限制，
   如實記錄如上。

---

## 18 個查核領域

### 1. Business Rule Correctness
`rules.json` 的 cashback/points rate（2/3/5/8%）與 `app.py` 一致；等級門檻
（8000/15000/12000/24000/30000/60000）在 `calc_tier`/`_tier_from_window_total` 兩處一致。
壽星充值三方案（10000→500 / 20000→1000 / 30000→1500）核對正式資料庫全部 9 筆交易
100% 合規（金額、點數、月份皆符合）。**PASS**。

### 2. Independent Financial Recalculation
見上方方法論。彙總層 100% 通過；個別交易點數計算的演算法正確性透過程式碼交叉比對
（非窮舉重算）找到一項不一致，見 **Finding H-1**。

### 3. Coin Ledger
`coin_batches` 322 位顧客的 `remaining_amount` 總和與 `customers.coin_balance` 完全一致
（`db_check.py` Check 7，全量）。作廢交易對應的批次正確標記 `status='voided'`（非刪除，
保留稽核軌跡）——以顧客 533 的完整交易/批次序列人工核對，逐筆吻合。**PASS**。

### 4. Membership-year accumulation（V3 會員年度制）
制度本身（`tier_effective_date`/`tier_expires_date`，效期屆滿用 `_window_total` 重判，
`pending_tier` 延遲生效）程式碼邏輯自洽，且有對應測試
（`test_void_recalculation_keeps_tier_when_calendar_year_threshold_remains` 等）覆蓋關鍵情境。
**發現**：`CLAUDE.md` 完全沒有記載這套 V3 年度制，只寫了「等級以本筆交易之前的累計計算」
的簡化版本——這是文件與實作嚴重脫節，見 **Finding M-3（併入 Source-of-truth）**。

### 5. Tier
`get_effective_tier`/`_project_tier_state` 是唯讀投影函式，不寫入（除非明確呼叫
`reevaluate_and_persist_tier`），符合「顧客沒來店不會被寫入變動，但顯示仍正確」的設計
意圖。**PASS**（程式碼審查 + 既有測試覆蓋）。

### 6. Upgrade/Renewal gift
`tier_upgrades` 目前 380 已發放／21 待發放／10 略過，待發放的 21 筆已計入 Action Board
🎁 分類（本次 session 已驗證邏輯與測試）。A→A 續會禮與 P/S→A 升級禮的區分邏輯
（`renewal = tier_before=='A級美咖' and tier_after=='A級美咖'`）正確。**PASS**。

### 7. Refund/Void
以顧客 533 完整追蹤：10 筆交易中 8 筆作廢、2 筆有效，對應 10 筆 `coin_batches`
（8 voided + 2 active），`coin_balance=703` 精確等於 2 筆有效批次餘額總和。**發現**：
23 筆作廢交易的 `void_reason` 欄位 100% 空白（0/23 有填寫內容），見 **Finding M-2**。

### 8. Birthday recharge
全部 9 筆 `birthday_recharge` 交易：方案金額/點數 100% 符合 rules，交易月份與顧客生日
月份 100% 吻合（含跨年出生年份不同的顧客）。**PASS，全量覆蓋**。

### 9. Customer merge
程式碼審查（`_reconcile_tier_after_merge`／`merge_customers`／`undo_customer_merge`）
邏輯自洽，且有 3 個既有測試覆蓋（保留最高等級、重算合併後年度累計、preview/undo 還原
所有帳本）。**目前正式環境 0 筆已合併顧客**，無法對正式資料做獨立實測，僅能以程式碼
審查 + 既有測試作為證據，標記 **Viable Model（非 Observed Reality）**。

### 10. Action Board
本次 session 內已完整建置 V1.1（actionable/informational 分離、生日分桶、0001 年份
顯示修正）與 Audit Governance V1（本文件所屬的稽核提醒機制），67 個測試全綠（除已知
SPA 預約無關失敗），正式站已驗收。**PASS**。

### 11. DB integrity
`PRAGMA integrity_check` 全程 `ok`（含本次稽核下載的唯讀副本、migration rehearsal
前後、migration 正式執行後）。**PASS**。

### 12. Source-of-truth
`CLAUDE.md` 對 V3 會員年度制完全未記載（見上方領域 4），對「本月壽星」Action Board
（V1.1 前）與稽核治理機制（本文件）也都未提及——**CLAUDE.md 已明顯落後於實際系統**，
不能作為新接手者理解系統的可靠依據，見 **Finding M-3**。

### 13. Dependency/coupling
等級門檻常數（8000/15000/12000/24000/30000/60000）在 `app.py` 內部重複 3 處
（`calc_tier`、`_tier_from_window_total`、一份 dict literal），另外在 6 支一次性維運腳本
（`scripts/backfill_member_tier.py`、`backfill_tier_upgrades.py`、`fix_coins_history.py`、
`pa_migrate.py`、`fix_lin_siyin.py`）各自硬編碼一份——共 9 處。目前全部數值一致，但沒有
單一 source of truth，未來門檻異動極容易漏改某一處而不自知。見 **Finding M-1**。
GitNexus 索引因 storage version mismatch 不可用（`GITNEXUS_UNAVAILABLE_KNOWN_NON_BLOCKING`），
本項依賴分析改用 grep 全文比對完成，非 blast-radius 圖形化分析。

### 14. Security/auth
`test_security_headers_and_cross_site_rejection`、`test_pin_login_is_rate_limited` 兩個既有
測試持續通過，涵蓋 CSP/HSTS/X-Frame-Options 等安全標頭與 PIN 登入速率限制。抽查所有
f-string 組出的動態 SQL（`has_column`／merge 相關 4 處）確認欄位/表名皆來自寫死的
Python literal，不是使用者輸入，非注入風險。**PASS**。

### 15. Test traceability
`tests/test_contracts.py` 共 67 個測試，涵蓋交易/退款/合併/預約/Action Board/Audit
Governance 等主要流程；51 個路由中多數有間接測試覆蓋（透過 test client 呼叫路由），
但沒有做到每個路由都有專屬測試（例如部分 CSV 匯出、少數 admin 端點）。**PASS_WITH_NOTE**
（覆蓋率合理但非 100%，不阻擋本次結論）。

### 16. Production/Git drift
本次 session 全程以 SHA-256 逐檔比對 Git commit 與正式站 filesystem，`071b842` 部署後
再次核對 `app.py`/所有 templates，**MATCH=YES**，無 drift。

### 17. Deployment contract drift
`CLAUDE.md`／memory 記載「`scripts/deploy.sh` 已在 2026-08-09 修正，`git pull` 後會自動
接著跑 `init_db()`」，但實際讀取 `scripts/deploy.sh` 只有一行 `echo` 提醒文字
（第 68 行），**並未真的執行 migration**。這正是本次 Audit Governance 部署過程中，
需要另外請 Owner 手動在 PA console 跑 `init_db()` 的原因——本次已依正確流程處理，但
文件本身的錯誤陳述尚未修正，見 **Finding M-3**。

### 18. Boundary/date scenarios
發現 1 筆交易（id 599，顧客蔡青吟 id 418）`txn_date='0001-11-08'`（與其本人生日欄位的
佔位年份完全相同），`month_key='1-11'`（格式也不符 `YYYY-MM` 慣例）。金額 1400 元、
28 點已計入該顧客 `coin_balance` 與全站營收彙總，但因 `month_key` 字串排序小於任何
`2020-01` 以後的月份篩選區間，**在 /report 月報表中永遠不會被篩選出來**——顧客總消費
金額對得上，但逐月對帳會「少一筆」，容易讓主管以為帳兜不起來。見 **Finding M-4**。
另外，季度排程的月底安全位移（1/31 + 3 個月 → 4/30）已在 Audit Governance 開發過程
中發現並修正一個逐月複合 clamp 的 bug（見本 session 稍早的 commit），已有回歸測試覆蓋。

---

## Findings（依嚴重度排序，全部標記 BASELINE）

### 🟧 H-1 [HIGH] 修改交易的點數重算邏輯與建立交易不一致

- **位置**：`app.py` `update_transaction`（約 2692-2699 行）vs 建立交易流程
  （約 2185-2195 行）
- **現象**：建立交易時，`coins_earned` 用的等級是 `reevaluate_and_persist_tier()` 算出的
  「V3 會員年度制當下有效等級」（`tier_before`）。但修改既有交易金額時
  （`/api/transactions/<id>/update`），`coins_earned` 改用
  `calc_tier(past_max_single, year_total_so_far)`——用「歷史單筆最高金額 + 日曆年累計」
  的原始門檻判斷，完全沒有考慮該顧客當下是否已經在一個生效中的 S/P/A 會員年度視窗內。
- **影響**：若顧客已透過門檻觸發進入會員年度視窗（例如已經是 A 級美咖、效期還沒到），
  之後修改（不是建立）該顧客任何一筆交易金額時，重算出的點數費率可能與「建立交易時應得
  的費率」不同，且不會被 `db_check.py` 抓到（因為批次帳本會照著錯的
  `coins_earned` 內部自洽地重建，帳本本身仍然「守恆」，只是數字起點就不對）。
- **證據型態**：Observed Reality（程式碼直接讀出的邏輯差異，非推測）；**歷史實際影響
  無法從現有資料反推**（schema 沒有「此交易是否曾被編輯」的稽核欄位），故實際財務衝擊
  未知——保守標記 HIGH 而非 CRITICAL。
- **建議**：`update_transaction` 應該呼叫跟建立交易同一套 `tier_before` 邏輯（或抽成共用
  函式），而不是另外用 `calc_tier` 重新判斷一次。

### 🟨 M-1 [MEDIUM] 等級門檻常數重複 9 處，無單一 source of truth

- **位置**：`app.py`（3 處）+ 6 支 `scripts/*.py` 一次性維運腳本
- **現象**：8000/15000/12000/24000/30000/60000 這組門檻散落在至少 9 個地方，各自硬編碼。
- **影響**：目前全部一致，但未來若門檻調整，極易漏改，且沒有測試能自動抓出「這 9 處
  是否還一致」。
- **建議**：抽成 `rules.json` 或 `app.py` 的單一常數/函式，維運腳本 import 共用。

### 🟨 M-2 [MEDIUM] 作廢交易 void_reason 100% 空白

- **位置**：`transactions.void_reason`，23 筆作廢交易全部空白
- **影響**：稽核時無法區分「重複輸入手誤」「客訴退款」「主管修正」等不同性質的作廢，
  降低可追溯性。
- **建議**：作廢表單改為必填理由（至少提供簡短原因選單）。

### 🟨 M-3 [MEDIUM] CLAUDE.md 文件與實作嚴重脫節

- **現象**：
  1. CLAUDE.md 完全未記載 V3 會員年度制（`tier_effective_date`/`tier_expires_date`/
     `pending_tier`/效期屆滿重判），只寫了已過時的簡化版等級規則。
  2. CLAUDE.md／記憶檔宣稱 `deploy.sh` 已於 2026-08-09 修正成「`git pull` 後自動執行
     `init_db()`」，但讀取 `scripts/deploy.sh` 第 68 行只是一句 `echo` 提醒文字，**沒有
     真的執行**——這正是本次 Governance migration 需要另外請 Owner 手動跑 console
     指令的原因。
- **影響**：任何人（含 CC）只看 CLAUDE.md 會誤判系統實際行為，2 月的落差已在本次
  session 造成一次真實的「以為會自動 migrate，結果沒有」的情境（本次已正確處理，
  但下次接手者可能不會意識到要多查一步）。
- **建議**：更新 CLAUDE.md 的「VIP 等級」與「部署流程」兩節，如實反映目前程式碼行為。

### 🟨 M-4 [MEDIUM] 單筆交易日期異常（txn_date 與顧客生日佔位年份相同）

- **位置**：`transactions.id=599`（顧客 id=418 蔡青吟）
- **現象**：`txn_date='0001-11-08'` 與該顧客 `birthday='0001-11-08'` 完全相同；
  `month_key='1-11'` 格式也不符慣例。
- **影響**：1400 元消費、28 點已計入該顧客總消費與 `coin_balance`，但因 `month_key`
  字串比對規則，在 `/report` 任何 2020 年以後的月份篩選中都不會出現——顧客總額對得上，
  月報表對不上。
- **建議**：請 Owner 確認這筆交易的正確日期後手動修正（唯讀稽核範圍內不代為修改）。

### 🔵 L-1 [LOW] 3 筆 backdated_entry 已開 1-2 個月未複核

- **位置**：`review_flags`（cust541 x2、cust302 x1，最早 2026-08-26 開立）
- **現象**：系統的補登日期防呆機制正常運作（正確攔截並標記為待複核，未自動誤判等級），
  但這 3 筆已經開了 1-2 個月尚未有人複核。
- **建議**：提醒管理者近期在 `/review` 頁面處理。

### ⚪ I-1 [INFO] Customer merge 域無法用正式資料實測

- 目前 0 筆已合併顧客，本次 merge 域結論僅基於程式碼審查 + 既有測試（3 個），
  分類為 Viable Model，非 Observed Reality。

### ⚪ I-2 [INFO] GitNexus 索引不可用，不影響本次結論

- Storage version mismatch（沿用本 session 稍早已記錄的已知狀態），依賴分析改用
  grep 全文比對完成。

---

## 核心問題的直接回答

> 會員等級、點數、累積金額、禮物、退款與正式資料，是否能從原始交易獨立重算並證明結果合理？

**在帳本守恆層面：可以，且已驗證（322 位顧客、824 筆有效交易全量通過）。**
**在單筆交易點數費率是否用對等級這件事上：發現一個真實的邏輯不一致（H-1），且無法
排除歷史上是否曾經因此算錯——這是本次 Baseline 最重要的發現，留待 Owner 決定是否要
往回查證受影響筆數。**

---

## Severity 統計

| CRITICAL | HIGH | MEDIUM | LOW | INFO |
|---|---|---|---|---|
| 0 | 1 | 4 | 1 | 2 |

（MEDIUM 計 4：M-1/M-2/M-3/M-4）
