# Tier Remediation Candidate Table（Proposal Only，2026-09-25）

READ-ONLY / PROPOSAL-ONLY。承接 `tier-timeline-correctness-audit-2026-09-25.md`
找到的 28 位 mismatch 顧客，逐位展開成本表要求的 15 欄。`EXACT_FIELDS_TO_CHANGE`
是「如果要修，該改哪些欄位、改成什麼值」的**文件說明**，本次**沒有執行任何
一筆**。`PRODUCTION_FINANCIAL_WRITES=0`、`PRODUCTION_TIER_METADATA_WRITES=0`。

方法沿用整段稽核鏈一致的作法：不重新發明計算公式，全部交易＋point_adjustments
依時間序透過正式 `/entry` route／`create_coin_batch`／`reevaluate_and_persist_tier`
在隔離副本重放，`EXPECTED_*` 欄一律來自這次真實重放結果，`STORED_*` 欄一律來自
2026-09-25 當天重新下載的正式站唯讀副本（`PRAGMA integrity_check=ok`）。

## 彙總

```text
MISMATCH_CUSTOMERS=28
CURRENT_STATE_IMPACT=YES: 7 位（246/301/336/352/354/392/402）
FUTURE_EXPIRY_IMPACT=YES: 14 位
REMEDIATION_REQUIRED=YES: 7 位（= 全部 CURRENT_STATE_IMPACT=YES 名單，門檻/等級判定本身錯）
REMEDIATION_REQUIRED=OWNER_DECISION: 7 位（僅 tier_effective_date/valid_until 偏移，目前顯示等級仍對，Owner 決定是否要修正到期重判日）
REMEDIATION_REQUIRED=NO: 14 位（含 6 位純點數 over-credit、380 已於 H-1 修正的 under-credit、其餘完全一致）

歷史點數總計（與 H-1 Exposure Scan／Tier Timeline Audit 交叉核對一致）：
  HISTORICAL_POINT_OVER_CREDIT  合計 = 8442
  HISTORICAL_POINT_UNDER_CREDIT 合計 =  996（含已修正的顧客 380 的 96）
```

三份獨立稽核（H-1 Exposure Scan、Legacy Opening Balance v3、Tier Timeline Audit）
與本表的點數總額完全收斂在 8442 / 996，交叉驗證一致，非單一方法論的巧合。

## 主表（14 欄；`EXACT_FIELDS_TO_CHANGE` 見下方逐位展開，表格太寬故分開列）

| CUSTOMER_ID | NAME | STORED_CURRENT_TIER | EXPECTED_CURRENT_TIER | STORED_TIER_EFFECTIVE_DATE | EXPECTED_TIER_EFFECTIVE_DATE | STORED_VALID_UNTIL | EXPECTED_VALID_UNTIL | CURRENT_STATE_IMPACT | FUTURE_EXPIRY_IMPACT | HISTORICAL_POINT_OVER_CREDIT | HISTORICAL_POINT_UNDER_CREDIT | ROOT_CAUSE_CONFIDENCE | REMEDIATION_REQUIRED |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 224 | 謝小貞 | A級美咖 | A級美咖 | 2026-01-18 | 2026-01-18 | 2027-01-18 | 2027-01-18 | NO | NO | 0 | 0 | MEDIUM | NO |
| 238 | 黃舒郁 | P級美咖 | P級美咖 | 2026-05-07 | 2026-08-26 | 2027-05-07 | 2027-08-26 | NO | YES | 332 | 0 | HIGH | OWNER_DECISION |
| 246 | 黃意玲 | A級美咖 | P級美咖 | 2026-08-01 | 2026-05-30 | 2027-08-01 | 2027-05-30 | **YES** | YES | 882 | 0 | HIGH | **YES** |
| 254 | 廖筱玟 | A級美咖 | A級美咖 | 2026-02-09 | 2026-06-15 | 2027-02-09 | 2027-06-15 | NO | YES | 4938 | 0 | MEDIUM | OWNER_DECISION |
| 257 | 詹筱雯 | P級美咖 | P級美咖 | 2026-05-01 | 2026-07-27 | 2027-05-01 | 2027-07-27 | NO | YES | 200 | 0 | HIGH | OWNER_DECISION |
| 267 | 王睿檸 | A級美咖 | A級美咖 | 2026-03-08 | 2026-03-08 | 2027-03-08 | 2027-03-08 | NO | NO | 0 | 0 | MEDIUM | NO |
| 273 | 謝銘珠 | P級美咖 | P級美咖 | 2026-07-10 | 2026-07-10 | 2027-07-10 | 2027-07-10 | NO | NO | 0 | 0 | HIGH | NO |
| 276 | 林芸如 | P級美咖 | P級美咖 | 2026-03-09 | 2026-03-09 | 2027-03-09 | 2027-03-09 | NO | NO | 0 | 0 | MEDIUM | NO |
| 291 | 蔡雅文 | P級美咖 | P級美咖 | 2026-05-24 | 2026-06-28 | 2027-05-24 | 2027-06-28 | NO | YES | 100 | 0 | HIGH | OWNER_DECISION |
| 301 | 唐怡芳 | P級美咖 | S級美咖 | 2026-07-05 | 2026-06-21 | 2027-07-05 | 2027-06-21 | **YES** | YES | 70 | 0 | HIGH | **YES** |
| 302 | 歐千詳 | A級美咖 | A級美咖 | 2026-04-12 | 2026-05-31 | 2027-04-12 | 2027-05-31 | NO | YES | 570 | 900 | HIGH | OWNER_DECISION |
| 305 | 邱芷緹 | P級美咖 | P級美咖 | 2026-07-07 | 2026-07-14 | 2027-07-07 | 2027-07-14 | NO | YES | 288 | 0 | HIGH | OWNER_DECISION |
| 319 | 蔡青蓉 | S級美咖 | S級美咖 | 2026-05-28 | 2026-05-28 | 2027-05-28 | 2027-05-28 | NO | NO | 0 | 0 | HIGH | NO |
| 332 | 廖家瑢 | P級美咖 | P級美咖 | 2026-02-27 | 2026-09-02 | 2027-02-27 | 2027-09-02 | NO | YES | 188 | 0 | HIGH | OWNER_DECISION |
| 336 | 吳豫函 | A級美咖 | P級美咖 | 2026-09-09 | 2026-03-19 | 2027-09-09 | 2027-03-19 | **YES** | YES | 0 | 0 | HIGH | **YES** |
| 351 | 許瓊文 | P級美咖 | P級美咖 | 2026-02-26 | 2026-02-26 | 2027-02-26 | 2027-02-26 | NO | NO | 0 | 0 | MEDIUM | NO |
| 352 | 游馥瑋 | S級美咖 | 一般會員 | 2026-03-12 | NULL | 2027-03-12 | NULL | **YES** | YES | 104 | 0 | HIGH | **YES** |
| 354 | 馮曼婷 | P級美咖 | S級美咖 | 2026-04-24 | 2026-03-08 | 2027-04-24 | 2027-03-08 | **YES** | YES | 66 | 0 | HIGH | **YES** |
| 380 | 林秋蘭 | S級美咖 | S級美咖 | 2026-03-15 | 2026-03-15 | 2027-03-15 | 2027-03-15 | NO | NO | 0 | 96 | MEDIUM | NO（已於 H-1 修正） |
| 392 | 李佩持 | P級美咖 | S級美咖 | 2026-07-23 | 2026-04-23 | 2027-07-23 | 2027-04-23 | **YES** | YES | 140 | 0 | HIGH | **YES** |
| 402 | 莊惠如 | P級美咖 | S級美咖 | 2026-08-11 | 2026-06-09 | 2027-08-11 | 2027-06-09 | **YES** | YES | 0 | 0 | HIGH | **YES** |
| 413 | 黃毓文 | P級美咖 | P級美咖 | 2026-04-12 | 2026-04-12 | 2027-04-12 | 2027-04-12 | NO | NO | 0 | 0 | MEDIUM | NO |
| 415 | 陳招鐶 | A級美咖 | A級美咖 | 2026-04-14 | 2026-04-14 | 2027-04-14 | 2027-04-14 | NO | NO | 0 | 0 | MEDIUM | NO |
| 446 | 林毓晴 | A級美咖 | A級美咖 | 2026-05-01 | 2026-05-01 | 2027-05-01 | 2027-05-01 | NO | NO | 0 | 0 | MEDIUM | NO |
| 464 | 潘慧如 | 一般會員 | 一般會員 | NULL | NULL | NULL | NULL | NO | NO | 0 | 0 | HIGH | NO |
| 472 | 劉容而 | P級美咖 | P級美咖 | 2026-05-27 | 2026-05-27 | 2027-05-27 | 2027-05-27 | NO | NO | 486 | 0 | MEDIUM | NO（over-credit，政策：公司吸收不追討） |
| 517 | 黃麗華 | A級美咖 | A級美咖 | 2026-07-23 | 2026-07-23 | 2027-07-23 | 2027-07-23 | NO | NO | 0 | 0 | MEDIUM | NO |
| 533 | 陳姿樺 | A級美咖 | A級美咖 | 2026-08-17 | 2026-08-17 | 2027-08-17 | 2027-08-17 | NO | NO | 78 | 0 | HIGH | NO（over-credit，政策：公司吸收不追討） |

## EXACT_FIELDS_TO_CHANGE（proposal only，未執行）

只列出有欄位差異的 14 位；其餘 14 位標記「無需變更欄位」（純點數 over-credit
或完全一致）。

- **238 黃舒郁**：`customers.tier_effective_date`: `'2026-05-07'` → `'2026-08-26'`；`customers.tier_expires_date`: `'2027-05-07'` → `'2027-08-26'`
- **246 黃意玲**：`customers.member_tier`: `'A級美咖'` → `'P級美咖'`；`customers.tier_effective_date`: `'2026-08-01'` → `'2026-05-30'`；`customers.tier_expires_date`: `'2027-08-01'` → `'2027-05-30'`
- **254 廖筱玟**：`customers.tier_effective_date`: `'2026-02-09'` → `'2026-06-15'`；`customers.tier_expires_date`: `'2027-02-09'` → `'2027-06-15'`
- **257 詹筱雯**：`customers.tier_effective_date`: `'2026-05-01'` → `'2026-07-27'`；`customers.tier_expires_date`: `'2027-05-01'` → `'2027-07-27'`
- **291 蔡雅文**：`customers.tier_effective_date`: `'2026-05-24'` → `'2026-06-28'`；`customers.tier_expires_date`: `'2027-05-24'` → `'2027-06-28'`
- **301 唐怡芳**：`customers.member_tier`: `'P級美咖'` → `'S級美咖'`；`customers.tier_effective_date`: `'2026-07-05'` → `'2026-06-21'`；`customers.tier_expires_date`: `'2027-07-05'` → `'2027-06-21'`
- **302 歐千詳**：`customers.tier_effective_date`: `'2026-04-12'` → `'2026-05-31'`；`customers.tier_expires_date`: `'2027-04-12'` → `'2027-05-31'`
- **305 邱芷緹**：`customers.tier_effective_date`: `'2026-07-07'` → `'2026-07-14'`；`customers.tier_expires_date`: `'2027-07-07'` → `'2027-07-14'`
- **332 廖家瑢**：`customers.tier_effective_date`: `'2026-02-27'` → `'2026-09-02'`；`customers.tier_expires_date`: `'2027-02-27'` → `'2027-09-02'`
- **336 吳豫函**：`customers.member_tier`: `'A級美咖'` → `'P級美咖'`；`customers.tier_effective_date`: `'2026-09-09'` → `'2026-03-19'`；`customers.tier_expires_date`: `'2027-09-09'` → `'2027-03-19'`
- **352 游馥瑋**：`customers.member_tier`: `'S級美咖'` → `'一般會員'`；`customers.tier_effective_date`: `'2026-03-12'` → `NULL`；`customers.tier_expires_date`: `'2027-03-12'` → `NULL`
- **354 馮曼婷**：`customers.member_tier`: `'P級美咖'` → `'S級美咖'`；`customers.tier_effective_date`: `'2026-04-24'` → `'2026-03-08'`；`customers.tier_expires_date`: `'2027-04-24'` → `'2027-03-08'`
- **392 李佩持**：`customers.member_tier`: `'P級美咖'` → `'S級美咖'`；`customers.tier_effective_date`: `'2026-07-23'` → `'2026-04-23'`；`customers.tier_expires_date`: `'2027-07-23'` → `'2027-04-23'`
- **402 莊惠如**：`customers.member_tier`: `'P級美咖'` → `'S級美咖'`；`customers.tier_effective_date`: `'2026-08-11'` → `'2026-06-09'`；`customers.tier_expires_date`: `'2027-08-11'` → `'2027-06-09'`

`HISTORICAL_POINT_OVER_CREDIT`／`HISTORICAL_POINT_UNDER_CREDIT` 兩欄不建議透過
`EXACT_FIELDS_TO_CHANGE` 執行任何 `coin_balance`／`coin_batches` 寫入——沿用
H-1 已核准的 Owner 政策：over-credit 一律公司吸收不追討；under-credit 僅
HIGH-confidence 且來源完整者才會是補發候選（本表中僅顧客 380 的 96 點符合，
且已於 H-1 完成修正）；顧客 302 的 900 點 under-credit 雖為 HIGH confidence，
但其根因（升等日錯位）與欄位修正互相牽動，若要處理應與
`tier_effective_date` 欄位修正一併決定，不建議單獨補點。

## 讀法提醒

1. **CURRENT_STATE_IMPACT=YES 的 7 位是唯一「現在顯示的等級本身就是錯的」**
   名單（246/301/336/352/354/392/402）——這批如果要修，動的是「顧客現在
   享有的權益等級」本身，Owner 決策優先度應高於其餘 21 位。
2. **FUTURE_EXPIRY_IMPACT=YES 但 CURRENT_STATE_IMPACT=NO 的 7 位**
   （238/254/257/291/305/332/302）是「目前等級碰巧沒錯，但到期重判日算錯」
   ——不影響今天顧客拿到的權益，但會影響未來某天的自動降級判斷時間點。
3. **純點數 over-credit（472/533，及與 380 一起的既有名單）**：這批的
   `CURRENT_STATE_IMPACT`／`FUTURE_EXPIRY_IMPACT` 皆為 NO，等級/到期日完全
   正確，只有歷史點數對不上，依既有政策不建議動。
4. 本表**沒有**新增任何本次之前未發現的顧客或金額——28 位、8442/996 點，
   與既有三份稽核文件完全一致，屬於同一組發現的不同呈現角度。

`PRODUCTION_FINANCIAL_WRITES=0`。`PRODUCTION_TIER_METADATA_WRITES=0`。
本次僅產出文件，未修改任何 `customers`／`coin_batches`／`point_adjustments`／
`tier_upgrades` 紀錄。CSV：`docs/audit/tier-remediation-candidates-2026-09-25.csv`。
