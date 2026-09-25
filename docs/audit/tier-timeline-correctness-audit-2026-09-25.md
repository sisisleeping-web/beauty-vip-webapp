# Production 全庫 Tier Timeline Correctness Audit

- **模式**：READ-ONLY（來源 DB `mode=ro`；全部重放在隔離暫存 SQLite，
  從未寫回 Production）
- **方法**：用正式 Calculation Core（`app.py` 的
  `effective_tier_for_transaction`／`reevaluate_and_persist_tier`／
  `redeem_coins_fifo`／`create_coin_batch` 等），把全部 322 位有效顧客
  （`merged_into_customer_id IS NULL`）的非作廢交易 + 人工加點，依**真實
  建立時間**（`created_at`，不是 `txn_date`）依序透過真正的 `/entry` route
  重放，**沒有另外重新實作一套門檻/等級計算公式**——這是稽核 oracle 本身，
  跟 H-1 系列稽核用的同一套方法。
- **附件**：`tier-timeline-audit-2026-09-25.csv`（322 位逐戶）、
  `tier-timeline-point-deltas-2026-09-25.csv`（824 筆逐筆交易點數比對）

---

## 總覽

```text
TOTAL_CUSTOMERS_REPLAYED=322
EXACT_MATCH=294
EARLY_UPGRADE=5
LATE_UPGRADE=0
WRONG_CURRENT_TIER=2
WRONG_VALID_UNTIL=0
MISSING_TIER_EVENT=0
EXTRA_TIER_EVENT=10
HISTORICAL_SOURCE_INCOMPLETE=9
MANUAL_OR_LEGACY_UNCERTAIN=2
MISMATCH_CUSTOMERS=28（322 中 8.7%）

CURRENT_STATE_IMPACT_CUSTOMERS=7
FUTURE_EXPIRY_IMPACT_CUSTOMERS=14
HISTORICAL_POINT_OVER_CREDIT_CUSTOMERS=14
HISTORICAL_POINT_OVER_CREDIT_TOTAL=8442
HISTORICAL_POINT_UNDER_CREDIT_CUSTOMERS=2
HISTORICAL_POINT_UNDER_CREDIT_TOTAL=996

REPLAY_ERRORS=0
REPLAY_TEMP_DB_INTEGRITY=ok
```

**交叉驗證**：`HISTORICAL_POINT_OVER_CREDIT_TOTAL=8442` 與
`UNDER_CREDIT_TOTAL=996`，跟 H-1 Exposure Scan（`H1-exposure-scan-2026-09-25.csv`）
算出來的數字**完全一致**——兩次用不同角度（一次是「逐筆交易對不對」，
這次是「整條 tier 時間軸對不對」）算出同一組財務曝險數字，互相印證，
不是巧合。

---

## `WRONG_VALID_UNTIL` 為什麼是 0（不是沒問題，是分類邏輯把它併進其他類）

322 位裡沒有人被分類為單純的 `WRONG_VALID_UNTIL`，但**這不代表沒有
到期日錯誤**——`FUTURE_EXPIRY_IMPACT_CUSTOMERS=14` 才是真正的「到期日
跟重放結果對不上」的人數。分類優先序是
`EXTRA_TIER_EVENT`／`EARLY_UPGRADE` 等**事件層級**問題優先於單純的
到期日問題，因為到期日錯通常是事件時間點錯的**結果**，不是獨立成因——
14 位裡有 12 位同時被歸在 `EARLY_UPGRADE`（5）或 `EXTRA_TIER_EVENT`（7）
底下，只有 2 位（238、257 等，實際上這兩位事件本身也對不上，仍算
EARLY_UPGRADE）。詳細請看 CSV 的 `future_expiry_impact` 欄位，這是
獨立布林值，不受主分類影響。

---

## 特別驗證：customer_id=302 歐千詳

```text
CUSTOMER_302_RESULT:
  STORED_A_EFFECTIVE=2026-04-12   （tier_upgrades: 2026-04-11 upgrade_date +1）
  EXPECTED_A_EFFECTIVE=2026-05-31 （重放算出：2026-05-30 upgrade_date +1）— 完全複現上次查核的數字
  STORED_VALID_UNTIL=2027-04-12
  EXPECTED_VALID_UNTIL=2027-05-31 — 完全複現

  分類：EXTRA_TIER_EVENT
  重放事件（3 筆）：
    2026-01-03 一般會員→S級美咖 (upgrade)
    2026-01-03 S級美咖→P級美咖 (upgrade)
    2026-05-30 P級美咖→A級美咖 (upgrade)
  正式存的事件（4 筆，多一筆）：
    2026-01-03 一般會員→S級美咖 (upgrade)
    2026-01-03 S級美咖→P級美咖 (upgrade)
    2026-04-11 P級美咖→A級美咖 (upgrade)      ← 日期跟重放對不上
    2026-08-10 A級美咖→P級美咖 (downgrade)     ← 重放完全沒有這筆

  CURRENT_STATE_IMPACT=NO （今天不管用哪個生效日，她現在都早已是 A 級美咖，
    今天這個時間點兩邊算出來的「現在等級」剛好一樣）
  FUTURE_EXPIRY_IMPACT=YES （到期重判日差 7 週：2027-04-12 vs 2027-05-31）
  HISTORICAL_POINT_IMPACT=YES （over=570／under=900，跟上次查核一致）
```

**新發現（上次沒查到）**：正式資料庫還多一筆重放完全沒有的
**降級事件**（2026-08-10，A→P，`gift_status=skipped`）。追查發現
2026-08-10 正是她一筆 22999 元交易（id=1008，跟另一筆 id=1009 同金額
同日，1008 被作廢）被作廢的時間——這是 `reevaluate_tier_after_void`
（V3 Phase 2：交易作廢後即時重判等級）觸發的，不是批次腳本。但因為
1009（真正保留的那筆）金額跟 1008 相同，作廢 1008 理論上不該讓她的
真實消費總額下降，這筆降級事件本身的觸發邏輯也需要進一步理解——
**本次只如實記錄現象，未深究這一筆降級事件本身合不合理**，那是另一層
問題（void 重判邏輯），跟本次「tier timeline 對不對」的主查核目標不同，
標記 `OUT_OF_SCOPE`，留給 Owner 決定是否要另開查核。

**與已圍堵 legacy script 的 pattern 比對**：`STORED_A_EFFECTIVE`
（2026-04-12）只有在**不排除視窗起始日之前的交易**（把 2026-01-03
那筆 15999 元也算進「本年度累計」）才會在 04-11 這天跨過 60000 門檻。
這正是 `backfill_tier_upgrades.py`
（docstring：「Scans all transactions **chronologically per customer**」，
用的是純日曆年 `get_tier(max_single, year_total)`，完全沒有「視窗起始日」
這個概念）會產生的結果，跟現行正式 `_window_total()`（只加總
`tier_effective_date` 之後的交易）不一致。**Pattern 高度吻合**，但沒有
執行紀錄可以 100% 證明就是這支腳本寫入的，只能說證據強烈指向。

---

## 全部 28 位 mismatch 顧客

完整明細見 CSV，這裡列重點分群：

### A. 跟既有 H-1 稽核鏈重疊（21 位裡的 19 位在這次也被抓到）

`EARLY_UPGRADE`（5）、`EXTRA_TIER_EVENT` 裡的 246/301/302/352/354/392/533
（7）、`HISTORICAL_SOURCE_INCOMPLETE`（9 位中的 224/254/276/351/413/415/446，
7 位）、`MANUAL_OR_LEGACY_UNCERTAIN`（380、472）——這些顧客已經在
H-1 Exposure Scan／Positive-Only Entitlement Verification／Legacy Opening
Balance Verification 出現過，這次從「tier 時間軸」角度重新確認，數字
一致，沒有新增淨曝險金額。

**顧客 380（林秋蘭）特別說明**：這次顯示 `under_credit_points=96`，
**這不是新問題**——她原始交易 id=678 的 `coins_earned` 欄位本身仍然是
歷史存的 192（沒有被改動，本次稽核鏈一律不改寫歷史交易），但她的實際
帳戶餘額已經在 H-1 remediation 用**額外一筆 point_adjustment/coin_batches
correction batch** 補上了 +96（`H1-closure-evidence.md`，H1_STATUS=CLOSED）。
這裡看到的 96 只是「原始交易記錄本身」跟「正確值」的差，不代表她的
可用點數還缺 96——那已經在另一份文件裡結案。

### B. 全新發現、且有實質影響（3 位不在任何既有名單裡）

| 顧客 | 分類 | 現況 |
|---|---|---|
| 336 吳豫函 | `WRONG_CURRENT_TIER` | 目前顯示 A級美咖，重放結果應為 P級美咖；到期日也對不上（存 2027-09-09，應為 2027-03-19）；**沒有任何一筆交易點數算錯**——問題出在 `member_tier` 欄位本身跟她的交易歷史對不上，不是某筆交易費率算錯 |
| 402 莊惠如 | `WRONG_CURRENT_TIER` | 目前顯示 P級美咖，重放結果應為 S級美咖；到期日存 2027-08-11，應為 2027-06-09；同樣沒有任何一筆交易點數算錯 |

這兩位很可能是 `backfill_member_tier.py` 造成的——這支腳本的邏輯是
「用執行當下的判定邏輯算出應該是什麼等級，`tier_effective_date` 直接設成
**執行當天**（不是任何一筆真實交易的日期）」，這正好可以解釋「現在的
member_tier 對不上重放結果，但每一筆個別交易的點數都算對」這個特徵——
因為每筆交易當時是用它自己那個時間點正確累積出的等級算的，只有
metadata 欄位本身被後來的批次腳本覆寫過。

### C. 全新發現、但無實質影響（4 位）

273 謝銘珠、319 蔡青蓉、464 潘慧如：`EXTRA_TIER_EVENT`，但
`cur_impact=exp_impact=pt_impact=False`——重放跟正式存的事件筆數或內容
有差異，但現在的等級、到期日、歷史點數全部一致，代表這個差異目前沒有
任何實際影響（可能是重複記錄的升等事件、或已經被後續事件自然覆蓋掉），
標記 `ACCEPTED_HISTORICAL_RISK`，不需要處理。267 王睿檸屬於
`HISTORICAL_SOURCE_INCOMPLETE`（重放中遇到扣點當下點數不足，資料有
查不到的缺口，MEDIUM confidence，同既有政策不自動處理）。

---

## 財務曝險彙總（只計算，不修改）

```text
HISTORICAL_POINT_OVER_CREDIT_CUSTOMERS=14
HISTORICAL_POINT_OVER_CREDIT_TOTAL=8442  → OVER_CREDIT: COMPANY_ABSORBS / NO_CLAWBACK
HISTORICAL_POINT_UNDER_CREDIT_CUSTOMERS=2（顧客 380 已結案、472 劉容而 486 點未處理）
HISTORICAL_POINT_UNDER_CREDIT_TOTAL=996  → UNDER_CREDIT: REPORT_ONLY / OWNER_REVIEW
```

跟 H-1 系列稽核算出的數字完全一致，沒有發現本次獨有的新增財務曝險——
這次的價值在於**用不同角度（tier 事件時間軸）確認同一組數字**，並且
額外找到 2 位（336、402）**現在正在用錯誤等級**（不是歷史記錄問題，是
現在進行式的費率影響）的顧客，以及 302 那筆重放查不到的降級事件。

---

## 結論

```text
SUSPECTED_ROOT_CAUSE=backfill_tier_upgrades.py（EARLY_UPGRADE/多數
  EXTRA_TIER_EVENT 的升等日型態，pattern 與其「日曆年累計、不排除視窗起始
  日前交易」的邏輯高度吻合）＋ backfill_member_tier.py（WRONG_CURRENT_TIER
  的兩位，pattern 與其「metadata 直接設成腳本執行日、不對應任何真實交易」
  的邏輯高度吻合）。兩支皆已於前次任務 fail-closed（預設 refuse，需明確
  環境變數才能執行）。沒有執行紀錄可 100% 證實因果關係，這裡陳述的是
  pattern 一致性，不是確定的執行證據。

PRODUCTION_WRITES=0
BASELINE_AUDIT_REWRITTEN=NO
REMEDIATION_PROPOSED=NO
REMEDIATION_EXECUTED=NO
```

本次純粹是全庫範圍的唯讀盤點，沒有提出或執行任何修復。28 位 mismatch
顧客的完整明細、824 筆交易的逐筆點數比對都在附件 CSV，等 Owner 看完
再決定下一步。
