# H-1 Historical Remediation Plan + Dry Run

- **Evidence baseline**：`docs/audit/H1-exposure-scan-2026-09-25.csv`（commit `df55d1d`，36 筆 mismatch）
- **執行日期**：2026-09-25
- **模式**：全程 READ-ONLY 對 Production（來源 DB 以 `mode=ro` 開啟，SQLite 層級拒絕任何寫入）
- **`PRODUCTION_FINANCIAL_WRITES=0`**（本次沒有對正式 DB 做任何 INSERT/UPDATE/DELETE）
- **附件**：
  - `H1-remediation-plan-2026-09-25.csv`（36 筆逐筆分級明細）
  - `H1-remediation-summary-2026-09-25.csv`（15 位顧客逐戶彙總）

---

## 0. 重大前置發現：2026-08-09 Legacy 整併事件

在建 correction proposal 時，發現一件原本 Baseline/H-1 報告都沒提到的事：
**Production `coin_batches` 有 428 筆，其中 287 筆（67%）`is_legacy=1`、288 筆
`source_txn_id IS NULL`**。逐一核對後確認：**2026-08-09T12:44:33** 這個單一
時間點，把每位顧客當時的點數餘額整併成**一筆**不連結任何特定交易的 legacy
批次（`earned_amount = remaining_amount = 整併當下的 coin_balance`，
`credit_date=2026-08-09`，`expires_date=2027-08-09`）。這次任務要求的 15 位
受影響顧客，全部都在這次整併中各有一筆這樣的 legacy 批次（顧客 380 有兩筆）。

**這代表**：36 筆 mismatch 交易裡，只有發生在 2026-08-09 之後才建立批次的
那些，還有一筆獨立的 `coin_batches` 可以對應調整；**2026-08-09 之前的交易，
它們當初各自的批次（不管原本對不對）已經被揉進同一筆 legacy 批次裡，物理上
無法再拆回「這筆交易對應多少點」**。任何修正都只能在**顧客層級**對這一筆
legacy 批次做淨額調整，不能逐筆對應。

因此本次 remediation plan 分兩層：

- **Tier DIRECT**：mismatch 交易本身有獨立 `coin_batches`（`source_txn_id`
  對得上）。可以逐筆對那個批次做調整。
- **Tier LEGACY_POOLED**：mismatch 交易發生在整併之前，沒有獨立批次。只能對
  顧客那一筆 legacy 批次做「這位顧客所有 LEGACY_POOLED mismatch 的淨額」
  這個層級的調整。

36 筆中 **11 筆 DIRECT、25 筆 LEGACY_POOLED**。

---

## 1. 分類方法論

對每筆 mismatch（`delta = expected_coins − stored_coins`）：

- `delta > 0` → **UNDER_CREDIT**（顧客過去少領，需要補發，沒有負值風險）。
- `delta < 0` → **OVER_CREDIT**，再細分：
  - 對應批次（或顧客的 legacy 批次）**已過期**（`expires_date < today`）→
    **_EXPIRED**：這筆多發的點數本來就已經不能用了（`sync_coin_balance` 的
    可用點數計算本來就會用日期排除過期批次），修正對顧客「現在能花多少點」
    沒有任何實際影響，純粹是歷史帳目正確性問題。
  - 對應批次未過期，且 `remaining_amount == earned_amount`（完全沒被動用過）
    → **_UNUSED**：可以安全地把批次金額調降，不會碰到已經花掉的點數。
  - 對應批次未過期，且 `remaining_amount < earned_amount`（已經被部分/全部
    兌換）→ **_REDEEMED**：能安全扣回的上限是 `min(超額量, 目前剩餘量)`；
    超過剩餘量的部分是「已經被顧客實際花掉的超額點數」，扣不回來，只能算
    `unsafe_shortfall`（不能透過調整批次金額解決，牽涉到已完成的兌換）。

## 2. 本次實測結果：**全部 36 筆都可以安全修正，沒有任何已花掉、扣不回來的部分**

```text
DIRECT_OVER_CREDIT_UNUSED   : 8 筆
DIRECT_UNDER_CREDIT         : 3 筆
LEGACY_POOLED_OVER_CREDIT   : 21 筆（15 位顧客中有 12 位的 legacy 批次淨額為 over-credit）
LEGACY_POOLED_UNDER_CREDIT  : 4 筆
DIRECT_OVER_CREDIT_REDEEMED : 0 筆
DIRECT_OVER_CREDIT_EXPIRED  : 0 筆
NO_BATCH_FOUND（無法歸類）  : 0 筆
```

**15 位顧客的 legacy 批次目前全部 `remaining_amount == earned_amount`**（整併
以來沒有任何一位動用過那筆 legacy 點數），**且沒有任何一筆已過期**（
`expires_date=2027-08-09`，還有將近一年才到期）。這代表：

- 全部 32 筆 over-credit（DIRECT 8 筆 + LEGACY_POOLED 中 net over-credit 的
  部分）合計 8442 點，**理論上可以在不製造任何負值的前提下全額扣回**。
- 全部 4 筆 under-credit（DIRECT 3 筆 + LEGACY_POOLED 1 筆，客戶 380）合計
  996 點，補發沒有風險。
- `unsafe_shortfall`（已經花掉、扣不回來的部分）在 DIRECT 與 LEGACY_POOLED
  兩層加總都是 **0**。

這是本次 Dry Run 最重要的結論：**技術上沒有「顧客已經把多發的點數花掉」這種
最棘手的情境**——但這不代表可以直接執行，仍有下一節列出的政策/流程問題要
Owner 決定。

## 3. 15 位顧客逐戶 Correction Proposal 摘要

完整 45 個欄位在 `H1-remediation-summary-2026-09-25.csv`，這裡列重點：

| 顧客 | 受影響筆數 | DIRECT 淨額 | LEGACY 淨額 | Legacy 批次現況 | 建議動作 |
|---|---|---|---|---|---|
| 黃舒郁(238) | 2 | −162（可扣回） | −170（可扣回） | 1122/1122，未過期 | 兩層都扣回，共 −332 |
| 黃意玲(246) | 9 | −501（可扣回） | −381（可扣回） | 2457/2457，未過期 | 兩層都扣回，共 −882 |
| 廖筱玟(254) | 3 | 0 | −4938（可扣回） | 8583/8583，未過期 | legacy 扣回 4938（單一 30000 元觸發三級跳交易誤用 A 級費率是主因） |
| 詹筱雯(257) | 3 | 0 | −200（可扣回） | 1246/1246，未過期 | legacy 扣回 200 |
| 蔡雅文(291) | 1 | 0 | −100（可扣回） | 1141/1141，未過期 | legacy 扣回 100 |
| 唐怡芳(301) | 1 | −70（可扣回） | 0 | — | direct 扣回 70 |
| 歐千詳(302) | 5 | +900（需補發） | −570（可扣回） | 4450/4450，未過期 | direct 補發 900，legacy 扣回 570，淨 +330 |
| 邱芷緹(305) | 1 | 0 | −288（可扣回） | 1487/1487，未過期 | legacy 扣回 288 |
| 廖家瑢(332) | 3 | −27（可扣回） | −161（可扣回） | 1067/1067，未過期 | 兩層都扣回，共 −188 |
| 游馥瑋(352) | 2 | 0 | −104（可扣回） | 514/514，未過期 | legacy 扣回 104 |
| 馮曼婷(354) | 2 | 0 | −66（可扣回） | 758/758，未過期 | legacy 扣回 66 |
| 林秋蘭(380) | 1 | 0 | +96（需補發） | 357/357，未過期 | legacy 補發 96 |
| 李佩持(392) | 1 | −140（可扣回） | 0 | — | direct 扣回 140 |
| 劉容而(472) | 1 | 0 | −486（可扣回） | 1134/1134，未過期 | legacy 扣回 486 |
| 陳姿樺(533) | 1 | −78（可扣回） | 0 | — | direct 扣回 78 |

全站淨影響：−7446 點（32 筆過度發放 8442 點，4 筆發放不足 996 點）。

## 4. Dry Run 沒有回答、需要 Owner 決策的問題

技術上「能不能安全扣回」只是第一關。以下是實際執行前 Owner 需要決定的事，
本次唯讀稽核**不代為決定**：

1. **LEGACY_POOLED 層級的扣回精確性**：legacy 批次是「整戶淨額」層級的調整，
   不是逐筆對應。例如黃意玲（246）有 9 筆 mismatch，其中 6 筆落在 legacy
   整併之前、3 筆是整併之後的獨立批次——legacy 那 6 筆的淨額（−381）只能
   合併扣在同一筆 legacy 批次上，沒辦法像 DIRECT 那樣精確對應到「哪一筆
   交易多發了多少」。如果 Owner 想要逐筆留痕（而不是一筆合併調整），需要
   額外設計記錄方式。
2. **是否要真的執行 clawback**：對顧客而言，「已經入袋但還沒花」的點數被
   收回，即使系統上安全，也是體驗/信任層面的決定，不是純技術問題。
3. **對顧客的溝通**：若執行，是否需要通知顧客、如何解釋。
4. **legacy 整併本身的正確性尚未被本次稽核驗證**：本次只確認了
   「2026-08-09 這個時間點做了整併」與「整併後至今沒有人動用」，但**沒有
   驗證整併當下把每位顧客的 coin_balance 算對了沒有**——如果整併當下用的
   也是同一套錯誤公式彙總出來的 `coin_balance`，那麼 legacy 批次的
   `earned_amount` 本身可能也內含更多這次沒抓到的歷史誤差。這是一個新的、
   範圍更大的潛在問題，超出本次任務範圍，建議另開稽核追查整併腳本本身。
5. **是否要處理更早的根因**：本報告 §5 的腳本 containment 只防止「未來
   再次執行」，不解決「已經執行過一次造成的歷史落差」——那正是本次
   remediation plan 想解決但尚未執行的部分。

## 5. `scripts/fix_coins_history.py` Future-Risk Containment（已完成，未動歷史資料）

在檔案最上方加了一個執行前檢查：預設直接 `sys.exit(1)` 並印出說明（引用本
稽核鏈的三份報告），需要明確設定環境變數
`ALLOW_DEPRECATED_RAW_THRESHOLD_SCRIPT=i-understand-this-is-deprecated-see-H1-audit`
才會繼續執行，且加註「僅限本機唯讀副本測試，不該對正式 DB_PATH 執行」。

- 本機與 PA 上的副本**內容確認完全一致**（沒有意外覆蓋到不同版本）後才上傳。
- 只更新這一支腳本檔案本身，**沒有觸碰 `data/beauty_vip.db`**，也不需要
  reload web app（這支腳本不被 Flask app 匯入、不影響線上流量）。
- 沒有對 `backfill_member_tier.py`／`backfill_tier_upgrades.py`／
  `pa_migrate.py`（Baseline finding M-1 提到的其餘同款門檻邏輯複本）做
  同樣的處理——這次任務只點名 `fix_coins_history.py`，其餘三支維持原樣，
  如果要一併圍堵需要另外授權。
- 驗證：`python3 scripts/fix_coins_history.py`（無環境變數）已確認直接
  refuse（exit code 1），完整測試套件（73 個測試）不受影響。

## 6. 最終狀態

```text
STATE=H1_REMEDIATION_DRY_RUN_COMPLETE

DRY_RUN_MISMATCHES_ANALYZED=36
CUSTOMERS_ANALYZED=15
DIRECT_TIER_TRANSACTIONS=11
LEGACY_POOLED_TRANSACTIONS=25
LEGACY_CONSOLIDATION_EVENT_FOUND=YES（2026-08-09T12:44:33）

SAFE_TO_CORRECT_WITHOUT_NEGATIVE=36/36
UNSAFE_ALREADY_SPENT_SHORTFALL=0

TOTAL_OVER_CREDIT_CLAWBACK_IF_APPROVED=8442
TOTAL_UNDER_CREDIT_TOPUP_IF_APPROVED=996
NET_IMPACT_IF_FULLY_CORRECTED=-7446

PRODUCTION_FINANCIAL_WRITES=0
HISTORICAL_DATA_MODIFIED=NO
NEGATIVE_BALANCE_CREATED=NO

FIX_COINS_HISTORY_CONTAINMENT=DEPLOYED（本機 + PA 都已更新，純程式碼，零資料變動）

OWNER_DECISION_GATE=OPEN
NEXT_ACTION=等待 Owner 針對第 4 節列出的問題做決定；本次不自動執行任何
Production remediation。
```

完成後停在這裡，不自行執行任何 Production remediation。
