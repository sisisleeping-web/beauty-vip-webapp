# H-1 交易修改點數計算 — Root Cause、Future Fix、Historical Exposure Scan

- **來源**：`AUDIT-2026-Q3-BASELINE` Finding H-1
- **執行日期**：2026-09-25
- **Exposure Scan 模式**：`AUDIT_MODE=READ_ONLY`（詳見方法論一節）
- **附件**：`H1-exposure-scan-2026-09-25.csv`（824 筆非作廢交易的完整逐筆重算結果）

---

## 1. H-1 重現（RED）

在修正前，寫了 6 個 deterministic regression test（`tests/test_contracts.py`
`test_h1_*`，共涵蓋 8 個情境：General/S/P/A parity ×4、降級後編輯最新交易、
現況即最新交易（對照組）、達標交易本身舊費率、backdated 編輯），對照修正前的
`update_transaction`，**6/6 test 中的 6 個 assertion 直接 FAIL**（parity 測試
4 個 subTest 全部 FAIL、降級案例 FAIL、對照組"現況即最新" FAIL；threshold
own-transaction 與 coin-batch 兩個巧合沒有 FAIL，因為那兩個情境的初始條件剛好讓
新舊公式算出一樣的數字，不代表沒有 bug）。`H1_REPRODUCED=YES`。

最關鍵的重現案例：顧客歷史上有一筆 35000 元交易（早期），之後因為會員年度到期
被重判降級為一般會員。編輯該顧客「目前最新」的一筆 2500 元交易：

- 正確答案（顧客現在真正的等級＝一般會員 2%）：50 點
- 修正前的 `update_transaction`（用 `calc_tier(35000, 35000)` 硬算）：200 點（A 級 8%）

## 2. Root Cause

**CREATE（`entry()` normal mode）**：用 `reevaluate_and_persist_tier()` 算出
「交易當下」正式的 V3 會員年度制有效等級（`tier_effective_date`／
`tier_expires_date`／`pending_tier` 這條狀態機），backdated 交易則保守用
「目前 member_tier」並寫 `review_flags`，不自動升降級。

**UPDATE（`update_transaction`，修正前）**：完全不理會 V3 會員年度制狀態機，
另外用 `calc_tier(past_max_single, year_total_so_far)`——顧客「歷史單筆最高
金額」與「日曆年累計」這組跟 V3 完全脫鉤的門檻——重新硬算一次等級。

兩條路徑各自實作了一套「決定交易當下等級」的邏輯，從未共用，這是
**duplicated implementation**；divergence 只在特定情境會顯現（降級、backdated、
跨門檻交易本身），大部分「一路穩定成長、沒跨過門檻」的顧客兩套公式剛好算出
同一個答案，這也是為什麼這個 bug 長期沒被發現。

### 意外發現：同樣的 duplicated implementation 也存在於維運腳本層級，且證據顯示已經真的執行過

在 Root Cause 分析過程中，發現 `scripts/fix_coins_history.py`
（docstring：「Recalculate ALL normal transactions using the CORRECT business
rule」）用的是**跟 `update_transaction` 完全同一套** raw-threshold 公式
（`past_max_single`/`year_total_before`），對**全庫**所有 normal 交易依
`customer_id, txn_date, id` 排序重算並直接覆寫 `coins_earned`/`cashback`，
再重算 `coin_balance`。

唯讀重放這支腳本的邏輯（不寫入，只比對）發現：**目前 Production 786 筆
non-voided normal 交易中，98%（768 筆）的 `coins_earned` 已經完全等於這支腳本
「現在跑一次」會算出的數字**——換句話說，這支使用跟 H-1 同一種錯誤公式的
腳本，證據強烈指向**已經對整個歷史帳本執行過至少一次**，而不是 H-1 只透過
`update_transaction` 一筆一筆手動編輯造成的。本次 Historical Exposure Scan
找到的 36 筆 mismatch 中，有 12 筆同時出現在「這支腳本現在重跑仍會改變」的
清單裡——這 12 筆的暴露來源比較可能是「腳本執行後又有後續變動」，其餘 24 筆
目前已經跟腳本公式的輸出一致（腳本公式 ≠ 正確的 V3 公式，只是兩者剛好在這些
交易上重合）。

**這代表 H-1 的真正根因範圍比原始 Baseline finding 描述的更大**：不只是
`update_transaction` 這一條 API 路徑，維運腳本（`fix_coins_history.py`、
`backfill_member_tier.py`、`backfill_tier_upgrades.py`、`pa_migrate.py`）
全部複製了同一套跟 V3 會員年度制脫鉤的門檻邏輯，而且至少一支已經證據確鑿地
被執行過。這個發現超出本次「只修 update_transaction」的任務範圍，**如實記錄，
不在本次自動處理**，留給 Owner 決定後續（見第 8 節）。

## 3. Single Calculation Contract

新增 `effective_tier_for_transaction(db, customer_id, txn_day, rules, *,
exclude_txn_id=None, backdated_note="")`（`app.py`，`reevaluate_and_persist_tier`
之後）：

- 非 backdated（這筆本來就是、或改完之後仍是該顧客時間序上最新一筆）：
  呼叫 `reevaluate_and_persist_tier`，跟建立交易時完全同一條路徑。
- backdated：用目前 `customers.member_tier` 當保守估計，寫一筆
  `review_flags`（`item_type='backdated_entry'`），不自動升降級——跟
  `entry()` 原本對補登舊單的處理方式一致。

`entry()`（CREATE）與 `update_transaction`（UPDATE）現在都呼叫這支函式，
不再各自維護一份等級判定邏輯。`_is_backdated_entry` 加了
`exclude_txn_id` 參數，讓 UPDATE 判斷「排除自己之後，是否還有更新的交易」。

**沒有動到**：等級門檻、point rate、會員年規則、生日儲值、refund policy、
coin expiry、redemption ordering、A 續會邏輯、gift lifecycle、review
lifecycle、SPA booking、deploy.sh、WSGI、schema（zero migration）。

## 4. Regression Tests（GREEN）

6 個新測試（8 個情境，含 4-tier parity 的 subTest）全數通過：

1. `test_h1_editing_latest_transaction_reflects_current_downgraded_tier_not_raw_threshold`
2. `test_h1_editing_transaction_after_customer_upgraded_uses_tier_at_edit_time_not_latest`（對照組，證明不是無腦套用某個固定值）
3. `test_h1_create_update_parity_across_general_s_p_a`（一般/S/P/A 四級 parity）
4. `test_h1_threshold_transaction_keeps_old_rate_when_edited`（達標交易本身舊費率，不 retroactive）
5. `test_h1_backdated_edit_flagged_for_review_not_silently_recalculated`
6. `test_h1_untouched_coin_batch_recalculated_and_no_negative_after_edit`

全套 regression：**73 個測試，only known
`test_admin_move_cannot_overbook`（SPA booking，與 H-1 無關，逐字比對跟
baseline 完全相同）失敗**。`git diff --check` 乾淨。

## 5. Historical Exposure Scan 方法論

**不是**重新手刻一份重算引擎，**也不是**呼叫被查核的 UPDATE calculator 當
expected result。做法：

1. 下載正式 DB 的獨立唯讀副本（跟 fix/regression 用的是不同時間點的新下載）。
2. 建一個全新、隔離的暫存 SQLite（`beauty.init_db()` 建 schema），保留原始
   `customers.id`（用 name+birthday 對應，符合 `get_or_create_customer` 的
   查找邏輯）。
3. 把正式 DB 裡**全部 824 筆非作廢交易**依 **id 遞增（=原始建立順序）**，
   透過 Flask test client **真的 POST 到 `/entry`**（normal / birthday_recharge）
   ——也就是實際呼叫**修正後、目前正式站在跑的那套邏輯**——在乾淨的帳本上
   重新跑一次。`coin_deduct`（32 筆，不影響費率）不重放實際扣點邏輯，但仍
   插入一筆帶正確日期的紀錄，避免影響後續交易的「目前最新交易日期」判斷。
4. 重放完，逐筆比對「重放算出的 coins_earned」vs「正式 DB 原本存的
   coins_earned」。

這樣做是獨立的：不是拿 UPDATE 的答案當標準答案，而是拿「如果這筆交易的
事實（金額、日期、顧客、是否作廢）從頭到尾都只經過正確的建立交易路徑」
會得到的答案，去對照「正式資料庫現在實際存的值」。

## 6. 缺乏 Edit History 的限制（如實聲明）

Exposure Scan 找出的是「目前 stored 值 vs 正式重放後 expected 值」的差異，
**不等於**「這筆交易一定被 `update_transaction` 編輯過」。Schema 沒有任何
欄位記錄一筆交易是否被改過、改過幾次。找到的 mismatch 可能來自：(a)
`update_transaction` 真的被呼叫過、(b) 上一節提到的維運腳本執行過、(c) 其他
未知的一次性資料操作。**三者都指向同一個根本問題**（跟 V3 會員年度制脫鉤的
raw-threshold 公式），但無法從現有資料反推「哪一種」是特定某一筆的成因。

若某筆交易目前是 `EXACT_MATCH`，也只能說：**目前這筆 stored 值跟正式重放
結果一致；因為沒有編輯紀錄，無法證明它歷史上從未被錯誤公式動過又剛好被
後續操作蓋回正確值。**

## 7. Exposure Summary

```text
TOTAL_TRANSACTIONS_SCANNED=824
RECOMPUTABLE=792
NON_RECOMPUTABLE=32          （全部是 coin_deduct，不吃費率，非 H-1 範圍）
EXACT_MATCH=756
MISMATCH_COUNT=36
HIGH_CONFIDENCE_MISMATCH=36
MEDIUM_CONFIDENCE_MISMATCH=0
LOW_CONFIDENCE_MISMATCH=0
CUSTOMERS_AFFECTED=15
TOTAL_POINT_DELTA=-7446      （負值＝目前 stored 淨額比正確值多 7446 點）
  過度發放（stored 比正確值高）：32 筆，合計 8442 點
  發放不足（stored 比正確值低）：4 筆，合計 996 點
MAX_ABS_DELTA=1950（顧客 id=254，交易 id=829）
REPLAY_TEMP_DB_INTEGRITY=ok
REPLAY_ERRORS=0
```

Confidence：36 筆全部 HIGH——原始資料（金額/日期/作廢狀態）齊全，重放引擎
就是正式站目前實際在跑的程式碼（非另外手刻），非 backdated 或有清楚的
tier_upgrades 佐證可交叉核對（例如顧客 254 的 id=748，一筆交易觸發
一般→S→P→A 三級跳，tier_upgrades 記錄跟重放結果完全吻合，唯獨 stored
`coins_earned` 用了升等後的 A 級費率，違反「達標交易用舊費率」的明文規則）。

## 8. `0001-11-08` 特別查核（只查不修）

- 交易 id=599，顧客 id=418（蔡青吟）。
- **有參與本次重放**：`stored_coins=28, expected_coins=28, delta=0`——
  **EXACT_MATCH，跟 H-1 無關**（這位顧客全程都是一般會員，沒有跨過任何
  等級門檻，兩種公式在她身上不會有差異）。
- Lifetime total：已計入 `coin_balance=288`（7 筆交易之一）。
- Membership-year / tier：無影響（member_tier=一般會員，
  tier_effective_date/tier_expires_date 皆為空，從未觸發會員年度視窗）。
- Report impact：跟 Baseline finding M-4 描述一致——`month_key='1-11'`
  格式不符慣例，字串排序恆小於任何 `2020-01` 以後的月份篩選區間，
  在 `/report` 任何月份篩選中都不會出現，但總額/點數餘額都已正確累計。
- **本次未做任何修改**（Owner 若要修正這筆的日期，需另案處理）。

## 9. 是否需要歷史修復

**不執行**（硬性約束）。`HISTORICAL_REPAIR_EXECUTED=NO`。

若 Owner 決定要處理，這是 36 筆的 repair proposal 概要（完整明細見 CSV）：

- 32 筆需要**扣回**已多發的點數（合計 8442 點），15 位顧客受影響。
- 4 筆需要**補發**少發的點數（合計 996 點）。
- 已使用掉的點數不得強制造成負點——任何實際修復前，需先核對這 15 位顧客
  當下的 `coin_batches`／`coin_redemptions` 使用狀況，逐筆確認扣回不會讓
  已兌換的點數變成負值（本次稽核未做這一步，因為那已經是「執行修復」的
  前置作業，超出本次唯讀範圍）。
- 詳細顧客/交易清單見 `H1-exposure-scan-2026-09-25.csv`。

## 10. Audit Control Improvement（已落實，未擴大 Governance schema）

原本 Monthly Quick Audit 只依賴 `db_check.py`（帳本內部自洽性），抓不到
「帳本自洽但 business calculation 本身錯誤」這類問題（H-1 全程 `db_check.py`
都是綠燈）。本次在 §17 的既有稽核程序文件中新增一項提醒（純文件，未動
`system_audits` schema）：Monthly/Quarterly Audit 的查核清單應包含「stored
result vs 獨立重算 expected result」這一類 reconciliation，而不只是帳本
守恆檢查。
