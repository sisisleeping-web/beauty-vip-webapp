# Tier Timeline — 7 位會員 Production Remediation Closure

```
STATE=TIER_REMEDIATION_CLOSED
```

## OWNER_APPROVAL

Owner 明確核准：`7_CUSTOMER_REMEDIATION=APPROVED_FOR_PRODUCTION_WRITE`，
Target population 固定 7 位：246, 301, 336, 352, 354, 392, 402。基於既有
evidence：`09aab0c`（Production dry-run PASS）、`da2737e`（void focused
review，CASE A，不阻擋本次 write）。

## PRE_WRITE_BACKUP

```
BACKUP_PATH=data/backups/deploy_20260925_122517_pre_gate_final_write/data/beauty_vip.db
BACKUP_TIMESTAMP=20260925_122517
BACKUP_SHA256=edb87abc71a37a68ed6ecd75c179b56f6ca6168298e6def237bf387d3dc22fce
INTEGRITY_BEFORE=ok
```

BEFORE fingerprints（7 張必查表）：

```
customers=322 rows
transactions=849 rows／sum(final_amount)=7,115,486／sum(coins_earned)=263,643／sum(coins_redeemed)=34,665
coin_batches=431 rows／sum(earned_amount)=235,774／sum(remaining_amount)=230,274
coin_redemptions=6 rows／sum(amount)=5,500
point_adjustments=3 rows／sum(points)=796
tier_upgrades=413 rows
review_flags=3 rows
```

## PRE_WRITE_CAS（Gate B — Final CAS）

正式 write 前，在 Production 重新執行一次 `--dry-run`：

```
TARGET_COUNT=7
MATCH_BEFORE=7
PRECONDITION_FAIL=0
DRY_RUN=PASS
```

7 位逐戶皆與 `09aab0c` 記錄的 approved BEFORE state 完全相符，沒有 Production
資料 drift（距上次 dry-run 之間的正常營運活動不影響這 7 位）。

## EXACT_BEFORE / EXACT_AFTER

| CUSTOMER_ID | NAME | BEFORE_TIER | AFTER_TIER | EXPECTED_TIER | BEFORE_EFFECTIVE | AFTER_EFFECTIVE | EXPECTED_EFFECTIVE | BEFORE_VALID_UNTIL | AFTER_VALID_UNTIL | EXPECTED_VALID_UNTIL | MATCH |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 246 | 黃意玲 | A級美咖 | P級美咖 | P級美咖 | 2026-08-01 | 2026-05-30 | 2026-05-30 | 2027-08-01 | 2027-05-30 | 2027-05-30 | YES |
| 301 | 唐怡芳 | P級美咖 | S級美咖 | S級美咖 | 2026-07-05 | 2026-06-21 | 2026-06-21 | 2027-07-05 | 2027-06-21 | 2027-06-21 | YES |
| 336 | 吳豫函 | A級美咖 | P級美咖 | P級美咖 | 2026-09-09 | 2026-03-19 | 2026-03-19 | 2027-09-09 | 2027-03-19 | 2027-03-19 | YES |
| 352 | 游馥瑋 | S級美咖 | 一般會員 | 一般會員 | 2026-03-12 | NULL | NULL | 2027-03-12 | NULL | NULL | YES |
| 354 | 馮曼婷 | P級美咖 | S級美咖 | S級美咖 | 2026-04-24 | 2026-03-08 | 2026-03-08 | 2027-04-24 | 2027-03-08 | 2027-03-08 | YES |
| 392 | 李佩持 | P級美咖 | S級美咖 | S級美咖 | 2026-07-23 | 2026-04-23 | 2026-04-23 | 2027-07-23 | 2027-04-23 | 2027-04-23 | YES |
| 402 | 莊惠如 | P級美咖 | S級美咖 | S級美咖 | 2026-08-11 | 2026-06-09 | 2026-06-09 | 2027-08-11 | 2027-06-09 | 2027-06-09 | YES |

`EXPECTED_AFTER_MATCH=YES`（7/7）——AFTER 值以正式 write 完成後**重新獨立
下載**的 Production DB 副本查證，不只信 script stdout。

## AUDIT_ROWS

7 筆 `tier_upgrades` 稽核列，`event_type='remediation'`，`gift_status=
'skipped'`（不觸發重複禮品發放）：

| CUSTOMER_ID | AUDIT_ROW_ID | EVENT_TYPE | CREATED_AT |
|---|---|---|---|
| 246 | 414 | remediation | 2026-09-25T04:25:59 |
| 301 | 415 | remediation | 2026-09-25T04:25:59 |
| 336 | 416 | remediation | 2026-09-25T04:25:59 |
| 352 | 417 | remediation | 2026-09-25T04:25:59 |
| 354 | 418 | remediation | 2026-09-25T04:25:59 |
| 392 | 419 | remediation | 2026-09-25T04:25:59 |
| 402 | 420 | remediation | 2026-09-25T04:25:59 |

`TRIGGER_REASON` 完整記錄修正前後 tier/日期、根因說明、被取代（保留不刪）的
既有 `tier_upgrades` id 清單、稽核文件路徑（見 script 內
`REMEDIATION_MARKER='TIER-REMEDIATION-20260925'`）。

```
AUDIT_ROWS_CREATED=7
LEGACY_TIER_EVENTS_DELETED=0（tier_upgrades 總筆數 413→420，恰好 +7，
  逐一核對既有 216/353/309/364/191/104/341 等既知歷史列全部保留原樣未變）
```

## NON_TARGET_DIFF

正式 write 前後兩份獨立下載的 `customers` 表逐列比對：

```
NON_TARGET_CUSTOMERS_CHANGED=0
```

差異列集合精確等於 `{246, 301, 336, 352, 354, 392, 402}`，其餘 315 位顧客
tier metadata 逐位元組相同。

## FINANCIAL_FINGERPRINT

```
FINANCIAL_FINGERPRINT_BEFORE：transactions(849,7115486.0,263643,34665) /
  coin_batches(431,235774,230274) / coin_redemptions(6,5500) /
  point_adjustments(3,796)
FINANCIAL_FINGERPRINT_AFTER ：transactions(849,7115486.0,263643,34665) /
  coin_batches(431,235774,230274) / coin_redemptions(6,5500) /
  point_adjustments(3,796)
FINANCIAL_DIFF=0

TRANSACTION_WRITES=0
POINT_WRITES=0
COIN_BATCH_WRITES=0
COIN_REDEMPTION_WRITES=0
POINT_ADJUSTMENT_WRITES=0
HISTORICAL_CLAWBACK=0
```

## INTEGRITY

```
INTEGRITY_AFTER=ok
```

`scripts/db_check.py` 對 write 後獨立下載的副本執行：

```
✓ 所有檢查通過
顧客數：322　交易筆數：826（另有 23 筆已作廢）　正常消費總額：6,879,648
```

本次未出現 lazy coin_balance cache lag（跟本次 7 位 tier remediation 完全
無關的獨立現象，這次剛好沒有觸發）。

```
DB_CHECK=PASS
KNOWN_UNRELATED_LAZY_CACHE_LAG=N/A（本次未出現）
```

## IDEMPOTENCY

Production 正式 write 成功後，**在同一個 console 再執行一次相同正式
script**（無 `--dry-run`）：

```
SECOND_RUN_RESULT=ALREADY_REMEDIATED / NO_WRITE
SECOND_RUN_WRITES=0
```

獨立重新下載一份 Production DB 副本核對：`tier_upgrades` 總筆數仍是 420
（沒有多出第 8 筆）、7 位顧客欄位跟第一次 write 後完全相同、financial
fingerprint 逐位元組不變。

```
IDEMPOTENCY=PASS
```

## RUNTIME_SANITY

用唯讀方式（直接呼叫正式 `get_effective_tier_state()`——`/my`／`/report`／
`/contacts` 共用的同一個 pure-read 投影函式，只讀不寫，不經過任何觸發
cache-sync 的頁面）確認 7 位的 authoritative state 能被 Production runtime
正確讀出：

```
CURRENT_TIER_SOURCE_MATCH=YES（7/7）
TIER_EFFECTIVE_DATE_MATCH=YES（7/7）
TIER_VALID_UNTIL_MATCH=YES（7/7）
```

## VOID_PATH_FOLLOW_UP

```
VOID_PATH_BUG_CONFIRMED=YES
VOID_PATH_SEVERITY=MEDIUM
VOID_PATH_FIXED_THIS_TASK=NO
```

Evidence：`docs/audit/void-tier-reevaluation-focused-review-2026-09-25.md`
（commit `da2737e`）。本輪未修改 `reevaluate_tier_after_void()`，維持獨立
future follow-up，不因本次 7 人 remediation 完成而視為已解決。

## Existing Decisions（重申，未變動）

```
H1_STATUS=CLOSED（未 reopen，customer 380 的 +96 為最終值，未再補發／追討）
HISTORICAL_OVER_CREDIT_CLAWBACK=NO（14 位／8,442 點，公司吸收，本輪未動）
CUSTOMER_302=NOT_IN_7_CUSTOMER_TARGET（本輪未修改；她的 void-triggered
  event／historical effective-date discrepancy 維持既有 evidence／獨立
  follow-up）
CUSTOMER_541=OUT_OF_SCOPE（未動）
```

## Required Final Report

```
STATE=TIER_REMEDIATION_CLOSED

# Production
PRODUCTION_APP_PATH=/home/sisisleeping/beauty-vip-webapp
PRODUCTION_DB_PATH=/home/sisisleeping/beauty-vip-webapp/data/beauty_vip.db
HEAD=da2737e30db7c9845fc2edb98e876753369d2dac
SCRIPT_HASH=87fc628d614f25f288dd2ab84a9e9e574e2d4b4781872b1b90d41bfcb6d008d8
  （SCRIPT_CHANGED_SINCE_DRY_RUN=NO，本地／PA 部署版一致）

# Backup
BACKUP_PATH=data/backups/deploy_20260925_122517_pre_gate_final_write/data/beauty_vip.db
BACKUP_SHA256=edb87abc71a37a68ed6ecd75c179b56f6ca6168298e6def237bf387d3dc22fce
INTEGRITY_BEFORE=ok

# Final CAS
TARGET_COUNT=7
MATCH_BEFORE=7
PRECONDITION_FAIL=0
FINAL_DRY_RUN=PASS

# Write
WRITE_EXECUTED=YES
UPDATED_COUNT=7
FAILED_COUNT=0
TRANSACTION_COMMITTED=YES
ROLLBACK=NO

# Target Verification
CUSTOMER_246=PASS
CUSTOMER_301=PASS
CUSTOMER_336=PASS
CUSTOMER_352=PASS
CUSTOMER_354=PASS
CUSTOMER_392=PASS
CUSTOMER_402=PASS

EXPECTED_AFTER_MATCH_COUNT=7/7

# Audit Evidence
AUDIT_ROWS_CREATED=7
LEGACY_TIER_EVENTS_DELETED=0

# Blast Radius
NON_TARGET_CUSTOMERS_CHANGED=0
FINANCIAL_DIFF=0
TRANSACTION_WRITES=0
POINT_WRITES=0
COIN_BATCH_WRITES=0
POINT_ADJUSTMENT_WRITES=0

# Integrity
INTEGRITY_AFTER=ok
DB_CHECK=PASS
KNOWN_UNRELATED_LAZY_CACHE_LAG=N/A（本次未出現）

# Idempotency
SECOND_RUN_RESULT=ALREADY_REMEDIATED / NO_WRITE
SECOND_RUN_WRITES=0

# Existing Decisions
H1_STATUS=CLOSED
HISTORICAL_OVER_CREDIT_CLAWBACK=NO
VOID_PATH_BUG_CONFIRMED=YES
VOID_PATH_FIXED_THIS_TASK=NO

# Closure
CLOSURE_EVIDENCE=docs/audit/tier-remediation-production-closure-2026-09-25.md
CLOSURE_COMMIT=（本檔 commit 後填入）
PUSH=YES

TIER_REMEDIATION_STATUS=CLOSED
BLOCKERS=NONE
```
