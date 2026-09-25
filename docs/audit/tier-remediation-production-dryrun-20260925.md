# Tier Current-State Remediation — Script Preparation + Disposable Validation + Production Dry-Run

```
STATE=READY_FOR_OWNER_TIER_REMEDIATION_WRITE_APPROVAL
```

依 Owner 提示詞「美咖 Tier Timeline — 7 位 Current-State Remediation」執行。本輪
**未對 Production 做任何寫入**——只做腳本準備、disposable-copy 驗證、部署、以及
Production `--dry-run`。完成後立即停止。

## 0. HEAD / Commit / Push / Deploy

```
HEAD=0e8aa29c001e315867b09f7a4032db1283050ab9
COMMITS_THIS_ROUND:
  20f5b6c docs: 7 位 fact sheets（既有依據，本輪未改寫）
  0e8aa29 feat: 新增 remediate_tier_current_state_20260925.py + 9 條測試
PUSH=origin/main（已推送，git push 確認成功）
DEPLOY=script uploaded via PA Files API（deploy.sh 不同步 scripts/，另行上傳）
  LOCAL_HASH=87fc628d614f25f288dd2ab84a9e9e574e2d4b4781872b1b90d41bfcb6d008d8
  REMOTE_HASH=87fc628d614f25f288dd2ab84a9e9e574e2d4b4781872b1b90d41bfcb6d008d8
  HASH_PARITY=CONFIRMED
MIGRATION_REQUIRED=NO（無 schema 變更，只是資料寫入，沿用既有 tier_upgrades／
  customers 欄位）
```

## 1. 固定 Target Population

`246,301,336,352,354,392,402`，共 7 位，寫死在
`scripts/remediate_tier_current_state_20260925.py` 的 `TARGET_IDS`，不接受
command-line 指定其他顧客，不接受 `--all` 之類的擴大 scope 選項。

## 2. Root-Cause 分群（沿用既有稽核文件既定分類）

- **Group A**（246,301,354,392,402）：STORED 的 P 級升等日精確對應「日曆年
  全部交易累計、換級不歸零視窗」的單一計算方式跨過 24,000 門檻的那一天；
  正確視窗制從各自真正的前一級生效日起算，同一時間點的累計都遠低於門檻
  （NT$7,698～NT$11,440）。高度吻合 `backfill_tier_upgrades.py`，但無
  execution log 佐證，不宣稱 proven cause。
- **Group B**（336）：STORED A級美咖沒有對應 `tier_upgrades` 紀錄，且累計
  至最後一筆交易僅 NT$44,495，未達 A 級 60,000 門檻——比較像欄位被直接
  覆寫，高度吻合 `backfill_member_tier.py`。
- **Group C**（352）：唯一會讓她跨過門檻的交易（578）本身是補登交易
  （建立時間晚於當時已存在的更新交易），依 V3 規則補登交易本不參與升等
  偵測，已由正式 `/entry` route／Calculation Core replay 確認排除後她從未
  合法跨過門檻。

## 3. Locked Business Decisions（未變動，本輪僅重申）

- Over-credit（14 位／8,442 點）：公司吸收，禁止 clawback/negative
  points/balance reduction。
- Under-credit（2 位／996 點）：customer 380 的 +96 已由 adjustment id=5、
  batch 429 完成，`H1_STATUS=CLOSED`，不得 reopen、不得再補。customer 302
  的 900 點 under-credit 因根因與 tier 欄位修正牽動，不在本輪處理（見
  §7 Owner Decision 待議）。
- 本輪腳本 **不寫** `transactions`／`coin_batches`／`coin_redemptions`／
  `point_adjustments`／`review_flags`／任何點數欄位。

## 4. Gate A — Production Preflight（READ-ONLY，已完成）

- 直接查 PA WSGI 檔確認實際載入路徑：`sys.path.append('/home/sisisleeping/
  beauty-vip-webapp')`，DB 於該目錄下 `data/beauty_vip.db`——跟文件記載一致，
  不只信文件。
- 下載 Production DB 新鮮副本，`PRAGMA integrity_check` = **ok**。
- `scripts/db_check.py` 對該副本執行：**✓ 所有檢查通過**（322 位顧客、824
  筆有效交易，本次無 lazy cache lag，跟 H-1 Gate E 當時發現的 3 位快取滯後
  是不同時間點的獨立現象，不重複處理）。
- 7 位 BEFORE 完整快照（`member_tier`／`tier_effective_date`／
  `tier_expires_date`／`pending_tier`／`pending_effective_date`）已記錄，
  逐位皆與 Fact Sheet 一致，且全部 `pending_tier` 為 NULL（無 in-flight
  pending 升等，CAS 不會跟 pending 機制打架）。

## 5. Gate B — Reconfirm Fact Sheets（PASS）

對同一份 Gate A 快照，重新以真實 `/entry` route + `reevaluate_and_persist_tier`
逐位獨立 replay（沿用既有方法論，未另寫第二套 tier oracle），7 位 STORED／
EXPECTED 結果跟 `tier-remediation-fact-sheets-2026-09-25.md`（commit
`20f5b6c`）**逐欄位完全一致**，無任何 drift。`GATE_B=PASS`。

## 6. Gate C — Authoritative Fields（PASS）

- 讀取路徑（`_project_tier_state` → `get_effective_tier`／
  `get_effective_tier_state`，`/my`／`/report`／`/contacts` 等頁面共用）
  **只讀** `customers.member_tier`／`tier_effective_date`／
  `tier_expires_date`／`pending_tier`／`pending_effective_date`，完全不讀
  `tier_upgrades`。`tier_upgrades` 純稽核／禮品觸發用途，無 runtime 讀取
  依賴——這 5 個 `customers` 欄位就是唯一 authoritative state。
- 未來會不會重新漂移：`/entry` 的升等偵測（一般消費）在顧客已有
  `window_start`（即 `tier_effective_date`）時，正確使用視窗制
  `_window_total`，本次修正把 `tier_effective_date` 導正後，往後的升等偵測
  自然沿用正確視窗，**不會**再回到錯誤軌跡。
- **新發現（FOLLOW_UP，不影響本輪判斷）**：`reevaluate_tier_after_void`
  （作廢交易觸發的即時降級路徑，非 deprecated 腳本，是現行程式碼的一部分）
  用的是 `calc_tier(new_max_single, customer_year_total(...))`——日曆年
  累計，不是視窗制 `_window_total`。此函式只會**降級**（`idx_recomputed >=
  idx_current` 就提早 return），不會造成任何顧客被錯誤升等，所以不會讓本次
  修正的 5 位 Group A 顧客重新漂移回錯誤的高等級；但若未來這 7 位（或其他
  顧客）有交易被作廢，其降級判斷基準跟視窗制不完全一致，是一個**獨立於
  本次任務**的既有邏輯不一致點，記錄供 Owner 後續評估，不在本輪修正範圍
  （屬於 Calculation Core 本身，改它= refactor，本輪明確禁止）。
- `GATE_C=PASS`（metadata-only 修正對「升等」方向是穩定的；「降級」方向的
  既有不一致是 pre-existing、與本次修正無關的 FOLLOW_UP）。

## 7. Historical tier_upgrades 保留政策

不刪除、不修改任何既有 `tier_upgrades` 列。腳本只在寫入時，每位**額外新增
一筆** `event_type='remediation'` 的稽核列，`trigger_reason` 完整記錄：修正
前後的 tier/日期、根因說明、被取代（superseded，但保留不刪）的既有
`tier_upgrades` id 清單、稽核文件依據路徑。

## 8. 腳本：`scripts/remediate_tier_current_state_20260925.py`

- `TARGET_IDS` 寫死 7 個 id，`CUSTOMERS` dict 逐位寫死 BEFORE／AFTER／
  root-cause／SOURCE_TRANSACTION／superseded `tier_upgrades` ids。
- **CAS**：7 位完整 BEFORE 狀態逐欄位比對，任何一位不符 → 整批拒絕，
  `sys.exit(1)`，0 writes。
- **Idempotency**：7 位全部已是 AFTER → `ALREADY_REMEDIATED / NO_WRITE`，
  正常結束（非例外）。
- **Partial/未知狀態** → `PARTIAL_OR_UNEXPECTED_STATE / REFUSE`，
  `sys.exit(1)`，0 writes。
- **All-or-nothing**：單一 sqlite transaction，全部 7 位 UPDATE + 7 筆
  `tier_upgrades` INSERT 完成後再驗證一次，financial fingerprint／
  非目標顧客 fingerprint 也一併驗證，任何一項不符就 `db.rollback()`。
- **Financial write guard**：`transactions`／`coin_batches`／
  `coin_redemptions`／`point_adjustments` 四張表 COUNT+SUM 指紋，寫入前後
  比對，dry-run 與 live-write 皆印出 `PRODUCTION_FINANCIAL_WRITES=0`。
- `--dry-run`：完整跑過驗證＋列印，但從未 `INSERT`/`UPDATE`，直接
  `db.close()` 結束（無殘留 transaction）。

## 9. Disposable-Copy Validation（6 項全通過）

以 Gate A 下載的 Production 唯讀副本複製出多份 disposable copy 測試（皆在
本機暫存目錄，never 觸碰正式站）：

| # | 情境 | 結果 |
|---|------|------|
| 1 | Happy path 7/7（先 `--dry-run` 再實際寫入） | ✓ 7/7 正確寫入 AFTER 狀態 |
| 2 | 第二次執行（idempotency） | ✓ `ALREADY_REMEDIATED / NO_WRITE` |
| 3 | CAS mismatch（手動竄改 246 的 `tier_effective_date`） | ✓ `CAS_PRECONDITION_FAILED / REFUSE`，7 位皆 0 modified |
| 4 | Partial-after（手動把 402 先改成 AFTER，其餘仍 BEFORE） | ✓ `PARTIAL_OR_UNEXPECTED_STATE / REFUSE`，0 writes（僅我方手動竄改的那筆，非腳本寫入） |
| 5 | 財務表指紋（write 前後） | ✓ `transactions`／`coin_batches`／`coin_redemptions`／`point_adjustments` 四張表 COUNT+SUM 完全一致 |
| 6 | 其餘 315 位顧客 tier 欄位 | ✓ 全表逐列 diff，僅 7 位目標顧客的 `customers` 列有變化，其餘 315 位逐位元組相同 |

## 10. Tests / Full Regression

新增 9 條測試（`tests/test_contracts.py`）：allow-list 精確性、Group A/B/C
覆蓋、dry-run 零寫入、happy-path 全 7 位 atomic 寫入、idempotency 第二次
零額外寫入、CAS mismatch 整批拒絕、partial-after 拒絕、找不到顧客整批拒絕、
非目標顧客不受影響。

```
Ran 90 tests in 1.345s
FAILED (failures=1)
FAIL: test_admin_move_cannot_overbook  — AssertionError: 400 != 200
```

比對 signature（同一 test 名稱、同一 assertion、同一 `400 != 200`）跟本次
任務**之前**（H-1 Gate 系列，2026-09-25 稍早）已記錄的 baseline 失敗完全
相同——`PRE_EXISTING=CONFIRMED`，與本輪修改無關，未修復（本輪明確禁止修
這個既有測試）。其餘 89 條（含全部 9 條新測試）**全數通過**。

## 11. Production Dry-Run（實跑，2026-09-25，PA console 47695144）

上傳腳本並確認 hash parity 後，在 Production 唯一執行：

```bash
cd ~/beauty-vip-webapp
python3 scripts/remediate_tier_current_state_20260925.py --dry-run
```

7 位逐戶結果（跟 Gate A/B 完全一致，Production 資料自 Gate A 以來無 drift）：

| CUSTOMER_ID | NAME | Group | CURRENT（live）| EXPECTED_AFTER | PRECONDITION |
|---|---|---|---|---|---|
| 246 | 黃意玲 | A | A級美咖 / 2026-08-01 | P級美咖 / 2026-05-30 | MATCH_BEFORE |
| 301 | 唐怡芳 | A | P級美咖 / 2026-07-05 | S級美咖 / 2026-06-21 | MATCH_BEFORE |
| 336 | 吳豫函 | B | A級美咖 / 2026-09-09 | P級美咖 / 2026-03-19 | MATCH_BEFORE |
| 352 | 游馥瑋 | C | S級美咖 / 2026-03-12 | 一般會員 / NULL | MATCH_BEFORE |
| 354 | 馮曼婷 | A | P級美咖 / 2026-04-24 | S級美咖 / 2026-03-08 | MATCH_BEFORE |
| 392 | 李佩持 | A | P級美咖 / 2026-07-23 | S級美咖 / 2026-04-23 | MATCH_BEFORE |
| 402 | 莊惠如 | A | P級美咖 / 2026-08-11 | S級美咖 / 2026-06-09 | MATCH_BEFORE |

```
7/7 MATCH_BEFORE = True
7/7 MATCH_AFTER  = False
DRY_RUN=PASS（7/7 precondition 通過，未寫入，即將 ROLLBACK）
PRODUCTION_TIER_METADATA_WRITES=0
PRODUCTION_FINANCIAL_WRITES=0
```

**獨立覆核**（不只信腳本自己的輸出）：dry-run 執行完後，重新下載一份全新
Production DB 副本逐項核對——`PRAGMA integrity_check=ok`；7 位顧客欄位跟
Gate A 完全相同；`tier_upgrades` 這 7 位的紀錄數仍是 14 筆（無新增）；含
`TIER-REMEDIATION-20260925` 標記的列數 = 0；`transactions`／`coin_batches`
COUNT+SUM 指紋跟 Gate A 逐位元組相同。`UNRELATED_MUTATIONS=0` 是實測結果。

## 12. Customer 302／541（重申 OUT_OF_SCOPE）

- **302**：2026-08-10 void 觸發的 A→P 事件與她的 tier_effective_date 錯位
  問題，`FOLLOW_UP / OUT_OF_SCOPE`——不在本輪 7 位名單內，未處理。
- **541**：疑似重複輸入（1068/1106），`OUT_OF_SCOPE`，禁止自動刪除，待店家
  人工確認。

## 13. H-1

`H1_STATUS=CLOSED`，本輪未觸碰、未 reopen；customer 380 的 +96 已完成，
本輪腳本也不含 customer 380。

## Summary

```
SCOPE=7 customers (246,301,336,352,354,392,402)，hard-coded allow-list
GATE_A=PASS  GATE_B=PASS  GATE_C=PASS
DISPOSABLE_VALIDATION=6/6 PASS
TESTS=9/9 new PASS；89/90 total PASS（1 pre-existing unrelated failure）
PRODUCTION_DRY_RUN=PASS（7/7 precondition，0 writes，獨立覆核確認）
PRODUCTION_TIER_METADATA_WRITES=0
PRODUCTION_FINANCIAL_WRITES=0
H1_STATUS=CLOSED（未觸碰）
CUSTOMER_302=OUT_OF_SCOPE  CUSTOMER_541=OUT_OF_SCOPE
NEW_FOLLOW_UP=reevaluate_tier_after_void 使用日曆年累計而非視窗制（pre-existing，
  只影響降級判斷，不影響本次 7 位的升等修正是否穩定）

STATE=READY_FOR_OWNER_TIER_REMEDIATION_WRITE_APPROVAL
```

本輪到此停止。是否對 Production 執行實際寫入（拿掉 `--dry-run`），等 Owner
看過這份 dry-run 證據後決定；腳本已具備 CAS／idempotency／all-or-nothing／
financial guard，Owner 核准後可直接在同一個 console 執行
`python3 scripts/remediate_tier_current_state_20260925.py`（無 `--dry-run`）。
