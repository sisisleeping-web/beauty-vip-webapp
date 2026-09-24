# 2026-08-09 Legacy Opening Balance 獨立驗證

**問題**：2026-08-09 整併當下，每位會員被放進 legacy batch 的 opening
balance，是否等於依正式會員規則獨立重算後，當天真正應有的可用點數？

**簡短答案：不是。286 位有 legacy 批次的會員中，78 位（27%）的 opening
balance 跟獨立重算結果對不上，淨額被多算了 31,905 點。** 這個落差比
H-1 Historical Exposure Scan 原本找到的 7,446 點大超過 4 倍，代表
2026-08-09 那次整併有自己獨立的問題，不只是把 H-1 已知的 update_transaction
誤差原封不動地搬進 legacy 批次而已。

- **模式**：READ-ONLY（來源 DB `mode=ro` 開啟，SQLite 拒絕任何寫入；全部運算
  在隔離的暫存 SQLite 完成，未寫回 Production）
- **附件**：`H1-legacy-opening-balance-verification-2026-09-25.csv`（286 位
  逐戶明細）、`H1-legacy-redemption-anomalies-2026-09-25.csv`（7 筆異常扣點）

---

## 方法

跟 H-1 Exposure Scan 同一套「重放」邏輯，但這次目標不是逐筆交易的
`coins_earned`，而是問：**如果從頭到尾都走正確的路徑，2026-08-09 那天
這位顧客帳上應該有多少「可用」點數？**

1. 抓出全部 `created_at < 2026-08-09T12:44:33`（legacy 批次的建立時間，
   當作整併切點）且未作廢的交易（702 筆：668 normal、26 coin_deduct、
   8 birthday_recharge），依 id（＝真實建立順序）重放。
2. `normal`／`birthday_recharge` 透過 Flask test client **真的 POST 到
   `/entry`**（正式站現在在跑的、已修好 H-1 的邏輯）。
3. `coin_deduct` 也**真的重放扣點**（這點跟 H-1 Exposure Scan 不同，那次
   只留存根紀錄；這次要驗證「當下可用點數夠不夠」，所以必須真的走 FIFO
   扣點邏輯）。若在重算後的世界裡，某筆扣點當下點數不足，**不強行硬扣**
   （不製造負值），改記錄成「redemption anomaly」，並跳過那筆扣點繼續往
   下重放。
4. 全部重放完，直接呼叫正式站現在在用的 `get_usable_coin_total(customer_id,
   as_of=2026-08-09)`——跟 `sync_coin_balance` 背後同一支函式，會正確套用
   「次月才入帳可用」與到期排除規則——取得「正確重算後，2026-08-09 這天
   應該可用多少點」。
5. 跟該顧客 legacy 批次的 `earned_amount`（＝整併當下記錄的 opening
   balance）逐戶比對。

## 結果總覽

```text
CUSTOMERS_WITH_LEGACY_BATCH=286
EXACT_MATCH=208
MISMATCH_COUNT=78
TOTAL_DELTA(recalculated − actual)=-31,905     （負值＝整併當下把 opening balance 記高了）
MAX_ABS_DELTA=4,338（顧客 id=254，廖筱玟）
REDEMPTION_ANOMALIES=7（見下方，需要額外留意）
REPLAY_ERRORS=0
REPLAY_TEMP_DB_INTEGRITY=ok
```

78 筆 mismatch 裡，**73 筆是「整併當下記多了」**（actual > recalculated，
負 delta），**5 筆是「整併當下記少了」**（正 delta，含顧客 380 的
legacy_pooled 那筆 +96，之前在 remediation plan 就看過）。

## 兩個重要的方法論限制（誠實揭露，不誇大確定性）

### 1. `point_adjustments`（手動加點）沒有被重放進去

正式資料庫在整併前只有 **1 筆**手動加點紀錄（`id=3`，顧客 280 廖佩瑛，
+600 點，2026-08-07，備註「其他手動加點」）。本次重放**沒有**把這筆算進去，
所以顧客 280 的 `recalculated_correct_balance`（目前顯示 200）少算了這
600 點——若補上，正確值應該是 800，跟 actual 1300 的落差會從 1100 縮小到
500，但**方向不變、依然是整併記多了**。全庫只有這一筆手動加點會受影響，
其餘 285 位顧客的重算不受此限制影響。

### 2. 7 位顧客的重放中途遇到「扣點當下點數不足」，代表他們的 pre-cutover 歷史有本次資料表查不到的缺口

7 筆扣點（見 `H1-legacy-redemption-anomalies-2026-09-25.csv`）在正確重算下
會失敗（當下可用點數不夠扣）。深入追查其中最極端的一筆（顧客
254，2026-05-04 扣 600 點）：這是她重放序列裡**第一筆**交易（比她所有
已知的消費交易都早建立），代表在正確重算的世界裡，她在那個時間點根本還
沒有賺過任何點數。已排除的可能原因：

- 沒有更早、被作廢又的交易被漏算（已核對：全庫 0 筆「建立在整併前、但現在
  已作廢」的交易）。
- 不是合併顧客帶來的歷史（`customer_merge_operations` 目前 0 筆紀錄）。
- 不是手動加點（她名下沒有 `point_adjustments` 紀錄）。

**結論：這 7 位顧客的點數歷史裡，存在至少一個本次可查的資料表都解釋不了
的缺口**——最可能的解釋是在 2026-08-09 這次「已知」整併之前，還有更早一次
未留下清楚稽核軌跡的資料匯入或人工調整（例如系統上線初期把紙本/舊系統
資料批次匯入，用了跟 `coin_batches`／`point_adjustments`／
`customer_merge_operations` 都不同的方式）。這 7 位顧客的 mismatch
數字**信心層級降為 MEDIUM**（重放方法本身沒有錯，但輸入資料本身不完整），
其餘 71 位顧客的 mismatch（重放全程順暢、沒有卡住）維持 **HIGH confidence**。

## 最極端的案例（HIGH confidence，方法完整）

| 顧客 | 整併記錄的 opening balance | 正確重算應有 | 落差 |
|---|---|---|---|
| 丁雅芸(303) | 4354 | 454 | −3900 |
| 王儷倫(337) | 3376 | 297 | −3079 |
| 柯嘉君(208) | 4260 | 1860 | −2400 |
| 蔡汶珈(212) | 4120 | 1873 | −2247 |
| 吳瑞娟(519) | 2112 | 0 | −2112 |

（廖筱玟 254 的 −4338 因涉及上述資料缺口，列為 MEDIUM confidence，未放進
這張 HIGH-confidence 排行榜。）

## 這代表什麼

- H-1 Remediation Plan（上一份文件）只涵蓋了 15 位顧客、36 筆交易，那是
  用「逐筆交易 `coins_earned` 對不對」的角度找出來的，範圍**遠小於**
  2026-08-09 整併本身的問題。
- 這次驗證證明：**legacy 整併當下記錄的 opening balance，本身就不等於
  正確重算的結果**，而且影響的顧客數（286 位裡 78 位）與點數規模
  （31,905 點，HIGH-confidence 部分保守估計也有 2 萬點以上）都比 H-1
  單獨的發現大得多。
- 這 78 位顧客跟 H-1 remediation plan 的 15 位顧客有重疊，但不是子集/母集
  的簡單關係——有些顧客只在其中一份名單出現。兩份名單需要合併看待，不能
  只處理 H-1 那 15 位就當作處理完了。

## 沒有做的事（如實聲明，維持唯讀邊界）

- **沒有**修改任何 Production 資料、任何 `coin_batches`、任何顧客餘額。
- **沒有**嘗試回頭找出那個更早的「未知資料缺口」實際來源——那需要另外
  調查（可能得問問 Owner 印象中系統上線初期是怎麼把舊資料搬進來的）。
- **沒有**把這 78 位（或 71 位 HIGH confidence）併入任何 correction
  proposal 或執行任何 remediation——這純粹是驗證問題的答案，下一步要
  不要處理、怎麼跟 H-1 的 15 位顧客合併規劃，留給 Owner 決定。

## 最終狀態

```text
STATE=LEGACY_OPENING_BALANCE_VERIFICATION_COMPLETE

QUESTION_ANSWERED=NO_OPENING_BALANCE_DOES_NOT_MATCH_INDEPENDENT_RECALCULATION

CUSTOMERS_WITH_LEGACY_BATCH=286
MISMATCH_COUNT=78
HIGH_CONFIDENCE_MISMATCH=71
MEDIUM_CONFIDENCE_MISMATCH=7（redemption anomaly，pre-cutover 資料有查不到的缺口）
NET_DELTA=-31905

KNOWN_METHODOLOGY_GAPS=
  - point_adjustments 未重放（僅 1 筆、600 點、顧客 280 受影響，不影響整體方向）
  - 7 位顧客的 pre-cutover 歷史有本次資料表解釋不了的缺口，需要另外調查來源

PRODUCTION_WRITES=0
HISTORICAL_DATA_MODIFIED=NO

OWNER_DECISION_GATE=OPEN（跟 H-1 remediation plan 一起看，本文件不含
remediation proposal，只回答驗證問題）
```
