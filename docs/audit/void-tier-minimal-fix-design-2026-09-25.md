# Live Void Tier Path — 最小修正設計 Gate

READ-ONLY／DESIGN-ONLY。回答 `commit 6097b09` 當初真正想解決的 edge case
是什麼、以及如何在維持 V3 authoritative membership-window semantics 的
前提下正確處理它。**本輪不修改任何程式碼、不部署、不寫 Production。**

```
STATE=VOID_TIER_FIX_PLAN_READY
```

## ROOT_CAUSE

`reevaluate_tier_after_void()` 原本（`75280ca`，V3 Phase 2 首次引入）就是
用 `_window_total()`（視窗制）。`commit 6097b09`（2026-09-10 14:13:20）把
它改成 `customer_year_total()`（日曆年），程式碼註解自述理由：「先前使用
member_tier 的效期窗口，會在作廢重複交易時遺失會員升等前已累積的消費，
讓仍符合年度門檻的會員被錯誤降級。」——這個關切**本身是真實、有效的
edge case**，但改成「一律日曆年」是過度修正：它同時也會把「已經被用來
賺取更早一級的錢」重新算進來，用不同方式製造新的不一致（已在
`void-tier-reevaluation-focused-review-2026-09-25.md` 記錄）。

`BUSINESS_JUSTIFICATION_FOR_CALENDAR_YEAR=NOT_FOUND`（重申既有結論，不再
重新搜尋一次，證據見 `da2737e`）。

## 6097B09_RECONSTRUCTION

完整讀過 `git show 6097b09`（app.py + tests/test_contracts.py 兩處 diff）、
`git blame`、以及新增測試 `test_void_recalculation_keeps_tier_when_
calendar_year_threshold_remains` 的完整 fixture。逐項還原：

```
1. 被 void 的 transaction 日期＝2026-08-01（NT$3,500）
2. void 發生日期＝2026-08-10
3. pre-void tier＝P級美咖
4. tier_effective_date＝2026-07-05
5. window start/end＝[2026-07-05, 2027-07-05)
6. calendar-year qualifying total（扣掉被作廢那筆）＝
   6000+1899+6750+4999+8700＝NT$28,348
7. window-scoped qualifying total（扣掉被作廢那筆）＝NT$0
   （唯一落在 [07-05,…) 視窗內的交易就是被作廢的那筆 08-01；07-04 的
   8700 那筆早於視窗起始日 07-05，依規則不列入）
8. 原（視窗制）implementation 得出的 tier＝S級美咖（get_past_max_single()
   是 ALL-TIME，不受視窗限制＝NT$8,700，單筆≥8,000 觸發 S；
   annual=0 不足以再高）
9. 6097b09 想保留的 tier＝P級美咖（測試明確斷言）
10. 為什麼原 implementation 在該情境被認為錯：作廢的是一筆**跟目前等級
    建立完全無關**、時間上**晚於**觸發交易（07-04 的 8700）的重複輸入；
    視窗制卻因為「07-04 那筆本來就被排除在視窗之外（它已經『用過一次』
    去賺 P 級）」而把它也一併排除在『這位顧客是否還撐得住 P 級』的
    判斷之外——這是把「偵測下一級」用的排除規則，誤用在「維持目前等級」
    這個不同的問題上
```

```
BEFORE_6097B09_BEHAVIOR=視窗制（`_window_total`，從 `tier_effective_date`
  到 `tier_expires_date`）
AFTER_6097B09_BEHAVIOR=一律日曆年（`customer_year_total`），不論是否已有
  既存視窗
TEST_ADDED_BY_6097B09=test_void_recalculation_keeps_tier_when_calendar_
  year_threshold_remains
TEST_EXPECTATION=作廢一筆晚於升等觸發交易的重複單後，member_tier 應維持
  P級美咖不降級
```

**重要但不可忽略的事實**：這個 fixture 裡「觸發 P 級升等」的說法本身站不
住腳——`單筆消費 8,700 元達標` 這個 trigger_reason 用的是**單筆消費**理由，
但 P 級單筆門檻是 NT$12,000，NT$8,700 **從未**真正達到（見下方
`CUSTOMER_301_EDGE_CASE`）。也就是說，這個測試 fixture 示範的「保留 P 級」
本身建立在一個**從未合法成立**的 P 級之上——這不影響本輪要解決的
edge case 是否真實存在（它是真實的，見下方用「合法門檻」重建的乾淨版本），
但這個特定 fixture 不是一個乾淨示範。

## CUSTOMER_301_EDGE_CASE

用既有唯讀副本（`docs/audit/tier-remediation-production-closure-2026-09-25.md`
completion 前抓取的 Production 資料，本輪未重新下載新副本，不需要——沿用
既有 evidence 即可）還原真實事件：

```
CUSTOMER_301_PRE_VOID_TIER=P級美咖（tier_effective_date=2026-07-05，此值
  本身已經是錯的——7 位 remediation 已修正為 S級美咖/2026-06-21，
  commit a8e07f3）
CUSTOMER_301_PRE_VOID_EFFECTIVE_DATE=2026-07-05
CUSTOMER_301_VOID_TXN=id=1013（2026-08-01，NT$3,500，跟 id=1012 同金額
  同日期，重複輸入後 24 秒內作廢）
CUSTOMER_301_VOID_TXN_DATE=2026-08-01
CUSTOMER_301_VOID_EXECUTED_AT=2026-08-10T06:57:56

WINDOW_TOTAL_AFTER_VOID=NT$3,500（真實世界跟合成測試不同：1012 這筆同額
  重複單**沒有**被作廢，所以視窗內還留著它）
CALENDAR_YEAR_TOTAL_AFTER_VOID=NT$31,848（407+481+566+857+891+1012）

OLD_WINDOW_RESULT=S級美咖（get_past_max_single=8,700全時段最高單筆≥8,000，
  觸發S；跟合成測試殊途同歸）
6097B09_CALENDAR_RESULT=P級美咖（31,848≥24,000，維持P；idx_recomputed>=
  idx_current 提早return，這正是 Gate A dry-run／正式 write 前觀察到她
  BEFORE 狀態穩定、void path 未進一步影響的原因）
CURRENT_V3_EXPECTED_RESULT=S級美咖（7 位 remediation 已核實、已於
  Production 生效——她從未合法跨過 P 級門檻）
```

**為什麼「單純 window total」在這個真實案例會得到一個開發者認為錯誤的
結果，真正原因**：不是候選清單裡的「window anchor becomes invalid」或
「歷史必須重放」——是更精確的一種：`_window_total()` 這個 helper 的
排除規則（排除視窗起始日**之前**的交易）是專門設計來防止「同一筆錢
被拿去justify兩個不同等級」（NEXT-tier 偵測用），但 `reevaluate_tier_
after_void()` 拿它來回答一個**不同的問題**——「作廢這筆之後，顧客還撐不
撐得住『目前』這個等級」。回答這個問題時，**目前等級本身賴以成立的那筆
觸發交易，理應被算進去**（它就是讓顧客站上這個等級的錢），但視窗制的
排除規則卻正好把它排除掉了——這是把兩個不同語意的問題，錯誤共用同一套
排除規則造成的。

（附帶確認：911（實際上是891）trigger_reason「單筆消費8,700元達標」本身
不合法——8,700 從未達到 P 級單筆 12,000 門檻，這證實 301 的 P 級本身就是
既有稽核鏈已定案的 Group A 根因錯誤，不是這次 void 事件造成，7 位
remediation 已經修正完畢，`a8e07f3`。）

## AUTHORITATIVE_BUSINESS_SEMANTICS — Scenario 定義

以現行 V3 規則為準（達標當筆次日生效；S/P/A 效期 = 正式生效日 + 1 年；
年度累計屬於會員年度視窗，不是日曆年）：

| Scenario | 定義 | Expected Behavior |
|---|---|---|
| A | 被 void 的交易跟目前 tier 的建立無關（既非觸發交易，也不在觸發交易之後、對維持現有等級有貢獻的視窗累計內） | 完全不動 tier |
| B | 被 void 的交易就是讓會員升到目前等級的觸發交易本身 | 目前等級的整個 transition 都不該存在——退回觸發前那個等級（`tier_upgrades.tier_before`／`upgrade_date` 前的狀態），不是單純重算一個數字 |
| C | 被 void 的交易落在目前會員年度視窗內，是原本「年度累計達標」型觸發總額的一部分（trigger_reason 是「年度累計」型，不是「單筆消費」型） | 扣除後重新用同一個視窗基準判斷，可能真的要降級 |
| D | 移除觸發交易後，`tier_effective_date` 本身不再有事實根據 | 不能只改一個數字了事，要往回找「這位顧客當下真正站在哪一級」 |
| E | 存在更早的合法門檻交易，扣除被 void 那筆之後仍然撐得住目前等級（或某個較低但仍高於一般會員的等級） | 應該落在那個由更早交易撐住的等級，不是直接掉回一般會員 |
| F | 補登交易／merge 搬移過帳本，導致真正的時間序或提供者無法可靠重建 | Fail-safe：不猜，寫入 `review_flags` 交人工複核，不自動寫 tier |

顧客 301 這個真實案例屬於 **Scenario A**（被 void 的 1013 既不是觸發交易，
也不在「原本用來合法建立 P 級」的任何合法基礎裡——因為 P 級本身從未
合法建立，這是 Scenario A 疊加既有 Group A 根因錯誤的複合案例，但
void-path本身面對的局部問題確實是A型）。6097b09 想解決的、**若 P 級
真的合法建立**的那個版本，也是 Scenario A。

## VOID_REEVALUATION_MODEL

```
CORRECT_VOID_REEVALUATION_MODEL=HYBRID
```

理由：純 `WINDOW_RECALC`（revert 到 `_window_total`）對 Scenario A 系統性
錯誤（會排除掉目前等級自己的合法觸發交易，重演 6097b09 想解決的問題）。
純 `HISTORICAL_REPLAY`（排除被 void 交易後，從頭完整重放整個交易史）
對全部 6 個 scenario 都正確，但代價是要把 `/entry` 那套會產生真實
coin_batches／transactions 副作用的流程抽成一個純函式版本才能安全重放
（本身就是一次不小的 Calculation Core 邊界異動，超出「minimal fix」）。

Hybrid 作法：**沿用 `_window_total()` 這個既有 helper 不動，只改變傳給它
的 `window_start`**——不是用「目前這一級」的 `tier_effective_date`，而是
往回找「目前這一級的**前一級**」的 `tier_effective_date`（透過既有
`tier_upgrades` 表查最近一筆 `tier_after` = 目前等級的列，取它的
`upgrade_date` 前一級效期，或者如果前一級是一般會員就不設下界）。這樣
「目前等級自己的觸發交易」自然落在新視窗內，不會被誤排除；同時仍然排除
「更早、已經被用去賺前一級」的錢，不會像日曆年那樣無限往回借用。若
`tier_upgrades` 查不到任何一筆對應的既有升等紀錄（== 336／402 那種
「沒有對應紀錄的直接覆寫」訊號），落入 Scenario F，寫入 `review_flags`，
不自動判定。

用這個模型手算 301 的真實資料驗證：前一級（S）的 `tier_upgrades` 列
（id=293，`upgrade_date=2026-06-20`，效期 `2026-06-21`）→ 新視窗
`[2026-06-21, 2027-07-05)`，扣掉被作廢的 1013：891(07-04,8700)+
1012(08-01,3500)＝NT$12,200，`get_past_max_single`（全時段）仍是
NT$8,700 → `calc_tier(8700, 12200)`：單筆 8,700≥8,000 觸發 **S級美咖**
——跟 7 位 remediation 已核實的正確答案完全一致，證明這個模型手算可信。

## SHARED_CORE_REUSE

```
SHARED_CORE_REUSABLE_COMPONENTS=
  - calc_tier()（門檻表本身，完全不動）
  - 「actual-paid >= 1000 才列入會員年度累計」規則（_window_total／
    customer_year_total 已經共用同一條 SQL 條件）
  - _window_total()（helper 本身不動，只是呼叫端傳不同的 window_start）
  - _add_years()（效期計算）
  - get_past_max_single()（全時段單筆最高，維持現狀，void 場景本來就該
    看全時段，不是視窗內）

VOID_SPECIFIC_ORCHESTRATION=
  - 新增一個小 helper：往回找「目前這一級」的前一級 tier_upgrades 列，
    決定該用哪個 window_start（找不到就落入 Scenario F fail-safe）
  - 觸發交易本身被 void（Scenario B/D）時的降級/回退邏輯，仍然透過同一套
    "用新 window_start 重算 calc_tier" 來源，不需要另外的分支——這正是
    hybrid 模型比"revert 回舊版"更完整的地方：不用另外判斷"是不是觸發
    交易"，往回一級的視窗天然就會把它納入或排除在正確的位置
  - Scenario F 的 review_flags 寫入（沿用既有 `_is_backdated_entry` 那種
    fail-safe 寫法的精神，不是新發明一套機制）
```

## EXISTING_TEST 評估

```
TEST_BUSINESS_EXPECTATION_CORRECT=PARTIAL
```

「作廢一筆跟目前等級建立無關的重複單，不該讓顧客失去已經合法賺到的等級」
這個**意圖**是對的，值得保留、值得測。但這個 fixture 的「P級」本身從未
合法建立（見上方 ROOT_CAUSE 附帶確認），所以它現在斷言的「應該維持P」
驗證的其實是一個錯誤的目標值。**保留 incident intent，重寫成正確
business-level expectation** 的具體做法（本輪只設計，不動手）：把 fixture
的觸發交易換成真正合法門檻（例如把 07-04 那筆從 NT$8,700 改成
NT$12,000，讓它單筆真的達到 P 級門檻），其餘結構不變，斷言改成「新
hybrid 模型應該維持 P 級」——這樣測試才是在驗證一個真實、合法的 edge
case，而不是驗證一個被誤保留的錯誤狀態。

```
PII_EXPOSURE=NO
```

檢查過 fixture：`name='作廢稽核'`（描述性佔位字串，非真實姓名）、
`birthday='1990-01-01'`（明顯合成佔位日期）、無 phone、customer_id 是
測試當下自動產生的 lastrowid，不是 301。金額／日期數字雖然精確對應
真實事件，但單獨這些數字不構成可識別真人的 PII。

## MINIMAL_FIX_OPTIONS

| | Option 1: Revert 回 `_window_total` | Option 2: 完整 historical replay | Option 3（建議）: 往回一級 anchor reconstruction |
|---|---|---|---|
| CORRECTNESS | Scenario A 系統性錯誤（重演6097b09原始問題） | 全部6個scenario正確，by construction | Scenario A/B/C/D/E正確；Scenario F需fail-safe分支；理論上限：若「前一級」本身也建立在錯誤anchor上會繼承那個錯誤（風險已因7位remediation大幅降低） |
| IMPLEMENTATION_SIZE | 極小（單行revert） | 最大（需把/entry的副作用抽成純函式才能安全重放） | 小-中（一個新helper+改一處呼叫，複用既有_window_total／tier_upgrades，無schema變更） |
| REGRESSION_RISK | 低（code層面）／中（行為層面，重演舊bug） | 高（觸碰最多路徑，驗證面最大） | 低-中（單一函式內部，跟entry()／_project_tier_state完全隔離） |
| HISTORICAL_DATA_DEPENDENCY | 無新增 | 高（依賴完整、正確排序的交易史與補登語意） | 中（依賴tier_upgrades紀錄完整性；查無紀錄時fail-safe，不強行猜） |
| TESTABILITY | 容易（但既有測試fixture需要先修數字才會綠得有意義） | 最難（路徑最多） | 中等（比Option1多幾個情境，比Option2少很多） |

```
RECOMMENDED_MINIMAL_FIX=Option 3（往回一級 anchor reconstruction / hybrid）
EXPECTED_CHANGED_FILES=app.py, tests/test_contracts.py
EXPECTED_CHANGED_SYMBOLS=
  reevaluate_tier_after_void()（改寫內部 window_start 決定邏輯）
  新增：_prior_tier_window_start(db, customer_id, current_tier_effective_date)
    （或等效名稱；純查詢 tier_upgrades，無副作用）
  test_void_recalculation_keeps_tier_when_calendar_year_threshold_remains
    （修正 fixture 數字，保留意圖，見上）
```

## TARGETED_TEST_MATRIX（設計，本輪不建立可執行 test code）

依 §13「若新增 RED tests 會讓 main CI 故意失敗，不要直接 push 到 main」，
本輪選擇「test design in evidence」，不在 `tests/test_contracts.py` 新增
任何程式碼（`RED_TESTS_CREATED=NO`，`MAIN_CI_INTENTIONALLY_BROKEN=NO`，
main 完全未觸碰）。以下是未來實作時應涵蓋的 8 個情境設計：

| # | 情境 | 情境設計 | 目前(6097b09)行為 | Hybrid 應有行為 |
|---|---|---|---|---|
| 1 | customer-301-equivalent（合法版） | 合法≥12,000單筆觸發P，之後一筆無關重複單被void | 維持P（巧合正確，理由錯：日曆年） | 維持P（理由正確：觸發交易落在新視窗內） |
| 2 | 普通void不該動tier | void一筆遠低於任何門檻、對累計無實質影響的交易 | 早退（idx_recomputed>=idx_current） | 相同，早退 |
| 3 | void觸發交易本身→合法降級 | 被void的交易本身就是trigger_txn_id | 日曆年可能錯誤地不降級 | 正確降級到往回一級查到的等級 |
| 4 | void觸發交易→更早的合法交易成為新anchor | 觸發交易被void，但再往前還有一筆本來就夠格的交易 | 未处理此細節 | 新視窗自然涵蓋該更早交易，正確得出對應等級 |
| 5 | void落在目前會員年度視窗內 | 一筆視窗內、非觸發、對「年度累計」型觸發有貢獻的交易被void | 依日曆年，可能不降級 | 依新視窗重算，可能正確降級 |
| 6 | 日曆年高但視窗內不足 | 顧客日曆年總額很高（含很久以前的消費），但往回一級的視窗總額不足 | 錯誤地不降級（日曆年掩蓋問題） | 正確降級 |
| 7 | 同一顧客重複void/reevaluate冪等 | 對同一顧客連續兩次void（各自對應不同交易） | 未特別測過 | 兩次都應該各自產生正確、獨立、可追溯的結果，不互相污染 |
| 8 | 補登交易／source不完整→fail-safe | 顧客的目前等級查無對應tier_upgrades列（336/402型），或涉及merge搬移 | 直接日曆年硬算 | 落入Scenario F，寫入review_flags，不自動判定 |

## FUTURE_EXPOSURE

```
TRIGGER_CONDITION=未來任何一筆 entry_mode='normal' 交易被作廢
  （delete_transaction route），且該顧客目前等級不是一般會員
POSSIBLE_WRONG_OUTCOME=
  (a) 現行(6097b09後)行為：日曆年可能「過度保留」——顧客可能被保留在一個
      實際上已經不再合格的等級（因為日曆年把已經用去賺更早等級的錢也
      算進來），本質上是額外給予不該有的折扣/回饋率，屬於營運面持續
      的小額成本，不是一次性帳務錯誤
  (b) 若貿然單純revert（Option 1，未經hybrid設計）：會重演6097b09原本
      想解決的問題——合法賺到等級的顧客，因為無關的重複單被作廢，被
      不當降級
SEVERITY=MEDIUM
```

考量既有 5 個真實案例（302/301/319/533/464）：發生頻率低（Production
史上至今 5 位、跨約 6 週）；錯誤狀態存續時間短（3 位靠後續真實交易自然
覆蓋校正，301／302 兩位的落差已被 7 位 remediation／既有既定 evidence
完全涵蓋，沒有一位是「錯誤狀態長期未被發現」）；staff／既有稽核機制
（本 session 建立的 Audit Governance＋本次一連串 review）已證明有能力
偵測並解釋這類 drift；完全不碰點數/coin_balance/transactions，不是
財務錯誤；多數案例會被後續正常交易自然校正。

```
URGENT_HOTFIX_REQUIRED=NO
```

## ECONOMIC_GATE

```
OWNER_DECISION_RECOMMENDATION=SCHEDULE_NEXT_MAINTENANCE
RATIONALE=Bug 真實存在且已精確定義、fix 方案具體且小（單一函式＋一個
  純查詢helper，複用既有欄位，無schema異動），regression risk可控，但
  歷史損害輕微、非財務、大多能自我修復或已被既有機制涵蓋——不構成中斷
  其他工作立即處理的急迫性；也不建議無限期擱置（ACCEPT_RISK_
  TEMPORARILY），因為修法已經想清楚、成本不高，適合排進下一次維護窗口
  用本文件當實作依據直接動工，不需要再重新設計。
```

## RECOMMENDED_NEXT_STEP

Owner／ChatGPT review 本設計；核准後另開一輪實作 Gate（RED→GREEN→
disposable-copy驗證→dry-run→Production write，比照 7 位 remediation 走過
的同一套紀律），屆時才真正修改 `reevaluate_tier_after_void()` 與新增
測試。本輪到此為止。

---

## Required Final Report

```
STATE=VOID_TIER_FIX_PLAN_READY

HEAD=a8e07f3f07889b6adc29aba630bce3aae961c4f5
EVIDENCE_COMMIT=（本文件 commit 後填入）

# Root Cause
ROOT_CAUSE_COMMIT=6097b09
BEFORE_6097B09_BEHAVIOR=視窗制（_window_total，從目前tier_effective_date算）
AFTER_6097B09_BEHAVIOR=一律日曆年（customer_year_total）
BUSINESS_JUSTIFICATION_FOR_CALENDAR_YEAR=NOT_FOUND

# Customer 301
CUSTOMER_301_EDGE_CASE=Scenario A——被void的交易(1013)既非觸發交易也不在
  合法建立目前等級的基礎內；疊加既有Group A根因（P級本身從未合法建立，
  已由7位remediation修正）
WHY_SIMPLE_WINDOW_LOGIC_FAILED=_window_total()的排除規則是為「偵測下一級」
  設計（排除已用於目前等級的錢），被誤用在「維持目前等級」這個不同問題
  上，把目前等級自己的合法觸發交易也排除掉

# Correct Semantics
CORRECT_VOID_REEVALUATION_MODEL=HYBRID
SHARED_CORE_REUSABLE_COMPONENTS=calc_tier／_window_total／
  get_past_max_single／_add_years／>=1000累計規則，全部不動
VOID_SPECIFIC_ORCHESTRATION=往回一級查tier_upgrades決定window_start的
  新helper；查無紀錄時Scenario F fail-safe寫review_flags

# Existing Test
TEST_BUSINESS_EXPECTATION_CORRECT=PARTIAL（意圖對，fixture建立在從未合法
  成立的P級之上，需修正fixture數字保留意圖）
PII_EXPOSURE=NO

# Minimal Fix
RECOMMENDED_MINIMAL_FIX=Option 3（anchor reconstruction / hybrid）
EXPECTED_CHANGED_FILES=app.py, tests/test_contracts.py
EXPECTED_CHANGED_SYMBOLS=reevaluate_tier_after_void()，新增
  _prior_tier_window_start()，重寫既有void回歸測試fixture

# Tests
TARGETED_TEST_SCENARIOS=8（見TARGETED_TEST_MATRIX）
RED_TESTS_CREATED=NO
MAIN_CI_INTENTIONALLY_BROKEN=NO

# Risk
TRIGGER_CONDITION=未來任何 entry_mode='normal' 交易被作廢，且顧客當時
  非一般會員
POSSIBLE_WRONG_OUTCOME=現行行為可能過度保留不該有的等級（非財務、營運面
  小額成本）；若貿然單純revert會重演6097b09原始問題
SEVERITY=MEDIUM
URGENT_HOTFIX_REQUIRED=NO

# Economic Gate
OWNER_DECISION_RECOMMENDATION=SCHEDULE_NEXT_MAINTENANCE
RATIONALE=方案具體、風險可控，但歷史損害輕微/非財務/多能自我修復，不急迫
  但也不建議無限期擱置

# Safety
PRODUCTION_DB_WRITES=0
PRODUCTION_DEPLOY=0
PRODUCTION_BEHAVIOR_CHANGES=0

# Next Gate
NEXT_GATE=Owner/ChatGPT review本設計→核准後另開實作Gate（RED→GREEN→
  disposable驗證→dry-run→Production write）
BLOCKERS=待Owner核准是否/何時排入下一次維護窗口
```
