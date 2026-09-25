# Tier Remediation Fact Sheets — CURRENT_STATE_IMPACT=YES Priority Set

READ-ONLY / PROPOSAL-ONLY，承接 `tier-remediation-candidates-2026-09-25.md`。
這 7 位是全部 28 位 mismatch 顧客裡，**現在顯示的等級本身就是錯的**那組
（`CURRENT_STATE_IMPACT=YES`）。`SOURCE_TRANSACTION`／`EXPECTED_TIER_EVENT`
不是手算，是把每位顧客自己的交易依真實 `created_at` 插入順序，重放過真正的
`/entry` route，直接讀回程式自己寫進 `tier_upgrades` 的 `trigger_txn_id`／
`trigger_reason`。`POINT_WRITE_REQUIRED=NO`、`HISTORICAL_CLAWBACK=NO` 為固定值
（沿用 H-1 以來的 Owner 政策：本文件不執行、不補點、不追討）。

## 關鍵發現：7 位裡有 5 位共用同一個根因機制

246／301／354／392／402 這 5 位的 STORED（錯誤）P 級升等日，全部精確對應到
「**日曆年全部交易累計總額**（不分視窗、不會在換級時歸零重算）跨過 24,000
門檻的那一天」——但正確的 V3 視窗制規則是「只算**這一級生效日之後**的累計」。
5 位的正確視窗制累計在同一個時間點都遠低於門檻（NT$7,698～NT$11,440，門檻
24,000/12,000）。這跟顧客 302（`backdated-entry-review-2026-09-25.md`）已經
查出的根因完全同型，這裡是同一個機制的第 6、7、8、9、10 個獨立實例，指向
同一支已圍堵的 legacy 腳本（`backfill_tier_upgrades.py`）。

336 是不同機制：連「日曆年全部累計」這種寬鬆算法都不夠格（累計僅 NT$44,495，
A 級門檻 60,000），且 `tier_upgrades` 完全沒有對應紀錄——是欄位被直接覆寫，
比較符合 `backfill_member_tier.py`（已知會繞過稽核紀錄直接寫 customers 欄位）
的特徵。

352 又是另一種機制：唯一會讓她跨過門檻的那筆交易（578，2026-03-11）
本身是補登交易（2026-04-13 才建立，晚於當時已存在的 2026-04-07 交易），
依系統規則補登交易本來就不做升等偵測（已在真實重放中正確觸發
`review_flags` 的 `backdated_entry` 提示）——STORED 資料看起來是用「照
txn_date 排序处理、忽略建立時間」的方式算的，才會誤把這筆補登也算進升等
判定。

---

## 246｜黃意玲

```
CUSTOMER_ID=246
NAME=黃意玲

CURRENT:
member_tier=A級美咖
tier_effective_date=2026-08-01
tier_valid_until=2027-08-01

EXPECTED:
member_tier=P級美咖
tier_effective_date=2026-05-30
tier_valid_until=2027-05-30

WHY=STORED 軌跡（S@03-20正確→P@04-28→A@07-31）完全對應到「日曆年全部交易累計、不分視窗歸零」的單一算法：累計在txn654(04-28)跨過24,000、在txn984(07-31)跨過60,000。正確視窗制（每次換級後重新從生效日歸零累計）從S生效日(2026-03-21)起算，只在txn806(05-29)跨過24,000一次，之後再也沒有跨過60,000——正確最終等級為P，不是A。
SOURCE_TRANSACTION=id=806, 2026-05-29, NT$4,100（視窗制累計自2026-03-21起達NT$25,429，跨過P級年度24,000門檻）
EXPECTED_TIER_EVENT=S級美咖→P級美咖升等，生效2026-05-30；此後無交易再跨過視窗制A級門檻

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 301｜唐怡芳

```
CUSTOMER_ID=301
NAME=唐怡芳

CURRENT:
member_tier=P級美咖
tier_effective_date=2026-07-05
tier_valid_until=2027-07-05

EXPECTED:
member_tier=S級美咖
tier_effective_date=2026-06-21
tier_valid_until=2027-06-21

WHY=STORED P級（生效07-05）對應「日曆年全部交易累計」在txn891(07-04)跨過24,000（累計NT$28,348）——但視窗制從她真正的S級生效日(2026-06-21)起算，txn891單筆僅NT$8,700，未達P級單筆12,000或年度24,000門檻。STORED後續的「08-10 P→S降級」事件很可能是這個錯誤視窗後續被重新評估後的連帶結果，不是獨立第二個問題。
SOURCE_TRANSACTION=id=857, 2026-06-20, NT$4,999（日曆年累計達NT$19,648，跨過S級年度15,000門檻）
EXPECTED_TIER_EVENT=一般會員→S級美咖升等，生效2026-06-21；此後無交易再跨過視窗制P級門檻，維持S級

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 336｜吳豫函

```
CUSTOMER_ID=336
NAME=吳豫函

CURRENT:
member_tier=A級美咖
tier_effective_date=2026-09-09
tier_valid_until=2027-09-09

EXPECTED:
member_tier=P級美咖
tier_effective_date=2026-03-19
tier_valid_until=2027-03-19

WHY=STORED A級美咖/2026-09-09完全沒有對應的tier_upgrades紀錄（真實重放的紀錄在03-18的P級升等就結束了），而且沒有任何一筆交易夠格——就連寬鬆的「日曆年全部交易累計」算到2026-09-08也只有NT$44,495，離A級門檻60,000還有一段距離。這不是視窗制漏算的問題，比較像member_tier/tier_effective_date欄位被直接覆寫，繞過了升等偵測與稽核紀錄——特徵符合已知的backfill_member_tier.py（會直接寫customers欄位，不留tier_upgrades痕跡）。
SOURCE_TRANSACTION=id=502, 2026-03-18, NT$16,500（單筆一次跨過S的8,000與P的12,000兩個門檻）
EXPECTED_TIER_EVENT=一般會員→S級美咖→P級美咖（同一天雙級跳，由txn502觸發），生效2026-03-19；累計至2026-09-08視窗制總額NT$44,495，未達A級門檻，維持P級

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 352｜游馥瑋

```
CUSTOMER_ID=352
NAME=游馥瑋

CURRENT:
member_tier=S級美咖
tier_effective_date=2026-03-12
tier_valid_until=2027-03-12

EXPECTED:
member_tier=一般會員
tier_effective_date=NULL
tier_valid_until=NULL

WHY=STORED S級是掛在txn578（txn_date=2026-03-11）跨過日曆年15,000門檻——但txn578本身是2026-04-13才建立的補登交易（建立當下，系統上已存在txn_date=2026-04-07的交易，比它晚），依V3規則補登交易本來就不做升等偵測（本次真實重放已正確觸發對應的backdated_entry review flag）。STORED資料看起來是照txn_date排序處理、而非依真實建立時間處理，才會誤把這筆補登也算進升等判定。
SOURCE_TRANSACTION=無有效觸發——本該觸發的txn578（2026-03-11, NT$6,750）因為是2026-04-13才補登建立而被規則排除
EXPECTED_TIER_EVENT=無——依即時（非補登）判定，她從未合法跨過15,000門檻，正確目前等級為一般會員

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 354｜馮曼婷

```
CUSTOMER_ID=354
NAME=馮曼婷

CURRENT:
member_tier=P級美咖
tier_effective_date=2026-04-24
tier_valid_until=2027-04-24

EXPECTED:
member_tier=S級美咖
tier_effective_date=2026-03-08
tier_valid_until=2027-03-08

WHY=STORED P級（生效04-24）對應「日曆年全部交易累計」在txn631(04-23)跨過24,000（累計NT$25,085）——但視窗制從她真正的S級生效日(2026-03-08)起算，txn631單筆僅NT$9,599，未達P級任一門檻。跟246/301/302/392/402同一個視窗漏算根因。
SOURCE_TRANSACTION=id=492, 2026-03-07, NT$1,620（日曆年累計達NT$15,486，跨過S級年度15,000門檻——單筆金額不大，但正好是把累計推過門檻的那一筆）
EXPECTED_TIER_EVENT=一般會員→S級美咖升等，生效2026-03-08；此後無交易再跨過視窗制P級門檻，維持S級

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 392｜李佩持

```
CUSTOMER_ID=392
NAME=李佩持

CURRENT:
member_tier=P級美咖
tier_effective_date=2026-07-23
tier_valid_until=2027-07-23

EXPECTED:
member_tier=S級美咖
tier_effective_date=2026-04-23
tier_valid_until=2027-04-23

WHY=STORED P級（生效07-23）對應「日曆年全部交易累計」在txn960(07-22)跨過24,000（累計NT$27,618）——但視窗制從她真正的S級生效日(2026-04-23)起算，txn960單筆僅NT$11,440，未達P級任一門檻。跟246/301/302/354/402同一個視窗漏算根因。
SOURCE_TRANSACTION=id=630, 2026-04-22, NT$10,298（單筆直接跨過S級8,000門檻）
EXPECTED_TIER_EVENT=一般會員→S級美咖升等，生效2026-04-23；此後無交易再跨過視窗制P級門檻，維持S級

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

## 402｜莊惠如

```
CUSTOMER_ID=402
NAME=莊惠如

CURRENT:
member_tier=P級美咖
tier_effective_date=2026-08-11
tier_valid_until=2027-08-11

EXPECTED:
member_tier=S級美咖
tier_effective_date=2026-06-09
tier_valid_until=2027-06-09

WHY=STORED P級（生效08-11）對應「日曆年全部交易累計」在txn1020(08-10)跨過24,000（累計NT$25,394）——但視窗制從她真正的S級生效日(2026-06-09)起算，累計只有NT$7,698（txn840+899+1020），未達P級任一門檻。跟246/301/302/354/392同一個視窗漏算根因，七位優先顧客裡已有六位確認同一機制。
SOURCE_TRANSACTION=id=817, 2026-06-08, NT$3,000（日曆年累計達NT$17,696，跨過S級年度15,000門檻）
EXPECTED_TIER_EVENT=一般會員→S級美咖升等，生效2026-06-09；此後無交易再跨過視窗制P級門檻，維持S級

POINT_WRITE_REQUIRED=NO
HISTORICAL_CLAWBACK=NO
CONFIDENCE=HIGH
```

---

`PRODUCTION_FINANCIAL_WRITES=0`。`PRODUCTION_TIER_METADATA_WRITES=0`。本次僅
產出文件，未修改任何 `customers`／`tier_upgrades`／`coin_batches`／
`point_adjustments` 紀錄。
