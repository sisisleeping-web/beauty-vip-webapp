# Live Void Tier Reevaluation — Focused Review

READ-ONLY。回答：`reevaluate_tier_after_void()` 這條現行（非 deprecated）程式碼
路徑是否使用了跟 V3 正式視窗制不一致的語意、是否已造成傷害、是否阻擋 7 位
Tier Current-State Remediation 進入 Owner write approval。

```
STATE=VOID_TIER_PATH_DECISION_READY
```

## EXECUTIVE_SUMMARY

`reevaluate_tier_after_void()` 的累計計算語意確認跟正式 `/entry` 升等偵測
（已建立視窗後）**不一致**——但這不是憑空存在的分叉：`git log` 找到
**commit `6097b09`（2026-09-10 14:13:20）** 把它從「視窗制」**刻意改成**
「日曆年累計」，理由寫在程式碼註解裡；同一顆 commit 新增的測試
`test_void_recalculation_keeps_tier_when_calendar_year_threshold_remains`
的 fixture 資料（6000/1899/6750/4999/8700/3500，日期 01-10/02-28/04-11/
06-20/07-04/08-01）跟 `tier_effective_date=2026-07-05` **逐項精確對應
顧客 301 的真實交易史**——這支測試幾乎可以確定是照她 2026-08-10 那次真實
事件反推寫出來的 same-day-ish 補救 patch（她的真實事件早一個月，
2026-08-10；patch 在 2026-09-10）。

Production 全庫掃描：23 筆 `entry_mode='normal'` 的作廢交易，只有 5 筆真正
觸發了 tier 重判（其餘因為降級門檻仍夠撐住現有等級，函式提早 return）。
5 位受影響顧客中，**3 位目前狀態完全正確**（318/533/464，函式當下算錯或
算出中間態，但都被後續真實交易自然覆蓋校正回來）；**2 位存在落差**，但
兩位都是**已知、已在既有稽核鏈裡處理過的案例**：顧客 301 已在 7 位
remediation 名單內（Group A）；顧客 302 的落差是既有、獨立的
`tier_effective_date` 問題（`backdated-entry-review-2026-09-25.md` 已記錄），
跟這次的 void 事件無關（void 觸發的降級也沒有真的留存下來）。

**沒有發現任何新的、尚未被涵蓋的財務或等級傷害。** 7 位 remediation 的
write approval **不被這條 live path 阻擋**（CASE A + 部分 CASE D 混合，見
§14）。但這條 live path 本身確實是一個**未修的既有 bug**，會在未來任意
顧客身上重演不一致的降級判斷——記錄為獨立 FOLLOW_UP，本輪不修。

## 1. CODE_PATH_COMPARISON

完整讀過 `reevaluate_tier_after_void()`（app.py:854-911）、唯一呼叫端
`delete_transaction()`（app.py:2786-2825，只有 `entry_mode=='normal'` 才會
呼叫）、以及所有 helper（`_project_tier_state`／`_window_total`／
`customer_year_total`／`get_past_max_single`／`calc_tier`／`_add_years`）。

```
ENTRY_TIER_CALCULATION_PATH=entry() 內建 upgrade-detection 區塊（app.py
  ~2290-2349），呼叫 effective_tier_for_transaction() 取得「這筆交易當下」
  的等級，用 calc_tier(new_max_single, accum_total) 判斷是否達到更高等級。
VOID_TIER_CALCULATION_PATH=reevaluate_tier_after_void()：先呼叫
  reevaluate_and_persist_tier()（正規化 pending／效期屆滿重判），再另外
  用 calc_tier(get_past_max_single(), customer_year_total()) **獨立**重算
  一次，只會降級不會升級（idx_recomputed >= idx_current 就提早 return）。

ENTRY_ACCUMULATION_WINDOW=
  - 顧客尚無 tier_effective_date（第一次達標）：customer_year_total()（日曆年）
  - 顧客已有 tier_effective_date（window_start 已存在）：_window_total()
    （視窗制，從 tier_effective_date 到 tier_expires_date）
VOID_ACCUMULATION_WINDOW=customer_year_total()（日曆年）——**無論顧客是否
  已有既存視窗，一律用日曆年**，這是跟 entry() 分岔的地方。

ENTRY_TIER_EFFECTIVE_RULE=次日生效（pending_tier／pending_effective_date，
  達標當筆本身仍用舊費率，隔天才正式生效）
VOID_TIER_EFFECTIVE_RULE=即時生效（as_of，也就是作廢動作發生的當天，直接寫
  customers.tier_effective_date=as_of，沒有 pending 機制）

ENTRY_VALID_UNTIL_RULE=生效日 + 1 年（_add_years 套用在正式生效的那天）
VOID_VALID_UNTIL_RULE=_add_years(as_of, 1)——從「作廢動作當天」起算 1 年，
  不是從任何交易日期起算

SEMANTICS_IDENTICAL=NO
```

**追的是實際 SQL／Python 邏輯，不是函式名稱或註解推論**——三處實測數字
（顧客 301／302／319 的作廢事件 log 裡各自記錄的累計金額）都跟上面兩條
公式手算結果逐位元組吻合，交叉驗證過，不是憑空推斷。

## 2. BUSINESS_RULE_COMPARISON

以現行 V3 正式規則（`_project_tier_state` 讀取／`_window_total` 視窗制）
作為 authoritative specification。

```
問：歷史 transaction 被 void 後，是否有任何已核准的 business rule，要求
    會員等級重新判斷時改用日曆年累計？

搜尋範圍：app.py 全文、tests/test_contracts.py、docs/、CLAUDE.md、
  rules.json、既有 audit 文件

找到：commit 6097b09 的程式碼註解本身（工程判斷，非外部核准的商業規則文件）：
  「升等門檻的『年度累計』是曆年累計，不是目前會員效期開始後的累計。先前
  使用 member_tier 的效期窗口，會在作廢重複交易時遺失會員升等前已累積的
  消費，讓仍符合年度門檻的會員被錯誤降級。」

這段話描述的問題是真實存在的（見下方"顧客 319 案例還原"），但它提出的
「一律改用日曆年」是過度修正——它解決了「視窗制會不當忽略視窗前已合格的
消費」這個真問題，卻引入了「日曆年也會不當納入已經『用過一次』的消費」
這個新問題，兩者都不是 V3 §3 正式定義的「本會員年度累計」語意（該語意
明確是視窗制，entry() 對已有視窗的顧客就是這樣做的）。

BUSINESS_JUSTIFICATION_FOR_DIFFERENT_VOID_SEMANTICS=NOT_FOUND
```

（找到的是同一位工程師/session 當天寫的程式碼註解與對應測試，不是 Owner
或任何文件核准的獨立商業規則——不採信為「刻意設計的不同語意」，採信為
「未被抓到的 regression」。）

## 3. PRODUCTION_EXPOSURE（READ-ONLY，2026-09-25 全庫掃描）

```
VOID_TRANSACTIONS_TOTAL=23（entry_mode='normal' 且 voided_at 非空）
VOID_TRANSACTIONS_WITH_TIER_REEVALUATION=5（觸發交易：1008/1013/1015/1034/1120；
  其餘 18 筆因為降級門檻仍撐得住現有等級，函式內 idx_recomputed>=idx_current
  提早 return，沒有寫入任何 tier_upgrades 列）
AFFECTED_CUSTOMERS=5（302, 301, 319, 533, 464）
```

用目前正式 shared Calculation Core（`/entry` route 完整 replay，voided
交易視同從未發生，不另外實作第二套 oracle）逐位重放，比較
`STORED_CURRENT_STATE` vs `EXPECTED_CURRENT_STATE`：

| 分類 | 人數 | 顧客 |
|---|---|---|
| MATCH | 3 | 319, 533, 464 |
| WRONG_CURRENT_TIER | 1 | 301 |
| WRONG_EFFECTIVE_DATE | 1 | 302（跟 void 無關，見 §5） |
| WRONG_DOWNGRADE／WRONG_VALID_UNTIL／HISTORICAL_ONLY／SOURCE_INCOMPLETE | 0 | — |

逐戶明細：`docs/audit/void-tier-reevaluation-exposure-2026-09-25.csv`。

**關鍵觀察**：5 筆觸發降級的事件裡，有 4 筆（302/301/319/533）發生在
commit 6097b09（2026-09-10 14:13）**之前**，用的是**舊版視窗制**程式碼；
只有 1 筆（464，2026-09-13）發生在修正**之後**，用的是新版日曆年程式碼，
而且剛好算對（她是首次評等顧客，視窗制跟日曆年在「尚無既存視窗」時本來
就等價）。也就是說：這次審查抓到的 4 個舊 bug 案例，是**新 patch 想解決
卻還沒真的被線上流量驗證過**的問題；新 patch 上線後唯一一次實際執行，
結果剛好正確（但那是因為那個案例的性質剛好讓兩種算法殊途同歸，不是新
patch 被證明對）。

## 4. CUSTOMER_301

```
VOID_TRANSACTION_ID=1013（2026-08-01，NT$3,500，跟 1012 同金額同日期，
  重複輸入後 24 秒內被作廢）
PRE_VOID_TIER=P級美咖（本身就是 Group A 根因錯誤——2026-07-05 生效日已經
  是誤算結果，不是這次 void 造成的）
STORED_POST_VOID_TIER=P級美咖（目前正式站現況——void 當下產生的降級並未
  留存下來）
EXPECTED_POST_VOID_TIER=S級美咖（跟既有 7 位 remediation 的 Fact Sheet／
  dry-run 完全一致）
STORED_EVENT=tier_upgrades id=364，P→S，2026-08-10，
  「作廢交易 id=1013 後累計降為 3,500 元」
EXPECTED_EVENT=若從乾淨重放（void 交易視同從未發生）看，她應該從
  2026-06-21 起持續是 S級美咖，07-04／07-05 那次 P 級升等本身就不該發生
  （Group A 根因），void 事件對一位「本來就該是 S」的顧客而言，本質上
  不該產生任何 tier_upgrades 事件——1013 只是 1012 的即時重複輸入，兩者
  淨影響為零
CALENDAR_YEAR_RESULT=P級美咖（新版程式碼若在此刻執行：日曆年累計扣掉
  1013 之後仍有 NT$31,848（407+481+566+857+891+1012），≥24,000，
  idx_recomputed(P) >= idx_current(P)，提早 return，不降級——這正是本輪
  稍早 Production dry-run 對 301 的觀察：她的 BEFORE 狀態穩定，沒有被
  void path 進一步影響）
WINDOW_SCOPED_RESULT=S級美咖（舊版程式碼實際執行的結果：視窗制從她
  （錯誤的）07-05 生效日起算，扣掉剛作廢的 1013，只剩 1012 這筆 NT$3,500，
  加上 all-time 單筆最高 NT$8,700（≥8,000 S 級單筆門檻），算出 S 級——
  跟記錄的 log 完全吻合）
ANOMALY_EXPLAINED_BY_LIVE_VOID_PATH=PARTIAL——void path（舊版）精確解釋了
  log 裡那個「累計降為 3,500 元」數字從何而來；但**沒有解釋**為什麼這筆
  已經寫入的 P→S 降級，最後沒有留在她目前的正式資料裡（她現在仍是 P，
  不是 S）。這代表 08-10 之後、09-25 之前，一定還有**另一次**未留下
  tier_upgrades 紀錄的寫入把她從 S 改回了 P——這跟 336／402 那種「沒有
  對應 tier_upgrades 紀錄的直接覆寫」是同一種特徵，但無法從現有資料精確
  指認是哪一次操作、哪支程式碼做的。
MERGE_HISTORY_IMPACT=NONE（已查證：`customers.merged_into_customer_id`
  沒有任何列指向或來自顧客 301，她既不是 merge 來源也不是 merge 目標）
```

**不影響既有結論**：她的 EXPECTED（S級美咖／2026-06-21）在 void 事件、
Group A 根因、以及這次的乾淨重放三個獨立角度都得到同一個答案，7 位
remediation 名單裡她的 BEFORE/AFTER 不需要修改。

## 5. CUSTOMER_302

```
VOID_TRANSACTION_ID=1008（2026-08-08，NT$22,999；同一天有一筆金額完全
  相同的 1009 被重新輸入且維持有效——典型的「重複輸入後作廢重打一次」）
PRE_VOID_TIER=A級美咖
STORED_POST_VOID_TIER=A級美咖（目前正式站現況，void 觸發的降級同樣沒有
  留存）
EXPECTED_POST_VOID_TIER=A級美咖（乾淨重放結果跟現況一致，tier 本身沒有
  問題）
CALENDAR_YEAR_RESULT=A級美咖（日曆年累計本來就遠超過 60,000，無論用哪種
  算法都會是 A 級，這位顧客的日曆年總額不是有鑑別力的比較點）
WINDOW_SCOPED_RESULT=P級美咖（舊版程式碼實際執行的結果：視窗制從她
  （同樣錯誤的）04-12 生效日起算，扣掉剛作廢的 1008，剩 714+785+834+1009
  ＝NT$46,998，<60,000，觸發 A→P 降級——log「累計降為 46,998 元」精確
  吻合）
VOID_DOWNGRADE_CORRECT=NO——這次降級本身不對（正確答案是維持 A級美咖），
  但這個「不對」的降級也沒有真的留存到現在：她目前正式資料是 A，不是 P，
  代表後續真實交易（1075／1100，皆晚於 void）透過正常 /entry 升等偵測，
  自然把她重新推回 A 級（她的真實消費量本來就遠遠支撐得起 A 級，跟這次
  void path 的公式選擇無關）。
```

`STORED_A_EFFECTIVE=2026-04-12` 與 `EXPECTED_A_EFFECTIVE=2026-05-31` 這組
落差**跟這次 void 事件無關**——不要混成同一個問題：

- void 觸發的降級把 `tier_effective_date` 改寫成 `2026-08-10`（`as_of`，
  作廢動作當天），這個值也沒有留存下來；她目前的 `2026-04-12` 是這次
  void 事件發生**之前**就已經存在的舊值，這次 void 的寫入被後續交易完全
  覆蓋掉，`2026-04-12` 這個數字本身是獨立、更早的問題（已記錄於
  `backdated-entry-review-2026-09-25.md`），不是這次 focused review 的
  新發現，也沒有被 void path 加重或掩蓋。

## 6. FUTURE_SAFETY

```
VOID_PATH_BUG_CONFIRMED=YES（commit 6097b09 把視窗制改成日曆年，跟 entry()
  已建立視窗後的語意不一致，且找不到獨立商業規則佐證這是刻意設計）
FUTURE_REDRIFT_RISK=
  對本輪 7 位 remediation 而言：LOW——void path 只會降級不會升級
  （idx_recomputed>=idx_current 才提早 return），而日曆年累計恆
  ≥ 視窗制累計（視窗是日曆年的子集合），所以「日曆年版」的 void path
  最多只會比「視窗制版」更不容易誤降級，不會產生新的、比視窗制更嚴重的
  向上誤判——修正這 7 位之後，就算未來他們有交易被作廢，這條 live path
  也不會把他們重新推回本次要修正的錯誤高等級。
  對系統整體而言：MEDIUM——這條 path 本身的降級判斷基準跟視窗制不一致
  是獨立、持續存在的問題，未來任何顧客的交易被作廢，都可能得到一個跟
  V3 正式規則不一致的降級/不降級判斷（本輪 5 個真實案例裡，3 個靠「後續
  真實交易自然覆蓋」僥倖躲過，不是這條 path 本身被驗證為正確）。
```

## 7. SHARED_CORE_FEASIBILITY

```
CAN_REUSE_SHARED_CORE=PARTIAL
```

- 最小修改點（若未來要修）：把 `customer_year_total(db, customer_id,
  as_of.strftime("%Y"))` 改回 `state["tier_effective_date"]` 存在時用
  `_window_total(db, customer_id, state["tier_effective_date"],
  state["tier_expires_date"])`——這是 6097b09 之前就有、已存在的既有
  helper，純粹是「改回去」，不是新寫一套邏輯，風險低。
- 但這個最小修改**不能完全避免** 6097b09 想解決的原始問題（視窗制在
  「作廢視窗前已合格的消費」這個邊界情境下確實會算得不合理，見顧客 319
  案例：她本來就靠視窗前的消費合格，視窗制重判卻只看視窗後的殘餘消費，
  對她也算出了不合理的降級）——要完全正確，理論上需要「排除掉剛作廢的
  這筆交易後，把全部歷史交易重新 replay 一次」（這正是本次 review 拿來
  當 oracle 用的方法），而不是任何單一 snapshot 公式。這是更大的架構
  改動（void replay 需要：exclusion set、完整交易排序重放、backdated
  交易語意、merge history provenance 一起考慮），不是本輪「minimal fix」
  範圍，也不是本輪要執行的事。
- 不建議為了「單一 Calculation Core」硬套 `_window_total` 而忽略 319 那類
  邊界情況——minimal fix 能把「跟 entry() 語意不一致」這個確定的問題解決，
  但不會讓 void path 變成完美，這點需要如實留給 Owner 判斷。

## 8. REMEDIATION_AUDIT_ROW_SAFETY

檢查 `scripts/remediate_tier_current_state_20260925.py` 新增的稽核列：

```
REMEDIATION_AUDIT_ROW_TABLE=tier_upgrades
REMEDIATION_AUDIT_ROW_EVENT_TYPE='remediation'
REMEDIATION_AUDIT_ROW_TRIGGER_REASON=完整記錄修正前後 tier/日期、根因、
  被取代（保留不刪）的既有 tier_upgrades id 清單、稽核文件路徑
  （範例格式見 scripts/remediate_tier_current_state_20260925.py 內
  REASON 組裝邏輯）
RUNTIME_READS_THIS_ROW_FOR_TIER_CALCULATION=NO
RUNTIME_READS_THIS_ROW_FOR_MEMBER_YEAR_ANCHOR=NO
RUNTIME_READS_THIS_ROW_FOR_EXPIRY=NO
```

逐一核對 app.py 全部 8 處讀取 `tier_upgrades` 的地方（gift-pending 儀表板
`app.py:1611`、刪除顧客防呆 COUNT `app.py:3167`、顧客合併搬移
`app.py:3566/3648`、Manager 升等禮報表 `app.py:3714/3726/3743`、顧客詳情頁
歷史列表 `app.py:4039`）——全部是**顯示／統計／搬移**用途，沒有任何一處
把 `tier_upgrades` 的內容拿來**計算**目前等級、會員年度起訖日、或到期日。
唯一的計算來源自始至終是 `customers.member_tier`／`tier_effective_date`／
`tier_expires_date`／`pending_tier`／`pending_effective_date`（跟上一輪
Gate C 的結論一致）。

新增列的 `gift_status='skipped'`，不會出現在 gift-pending 儀表板或
「已發放禮品」統計；會出現在顧客詳情頁的歷史列表裡，但 `trigger_reason`
明確標註 `TIER-REMEDIATION-20260925` 字樣，管理者一眼可辨識是修正紀錄，
不會被誤認成真正的業務升等/降級事件。

```
REMEDIATION_AUDIT_ROW_SAFE=YES
REMEDIATION_SCRIPT_STATUS=NOT_BLOCKED
```

## 9. RECOMMENDED_NEXT_GATE

```
CASE=A（主要）+ D（次要，僅限 void path 本身的修復時機）

7_CUSTOMER_REMEDIATION：CASE A——VOID_PATH_CORRECT 對這 7 位不成立（bug 
  確實存在），但 7_CUSTOMER_REMEDIATION_BLOCKED_BY_VOID_PATH=NO，因為
  這條 path 只會降級、且日曆年版本比視窗制版本更不容易誤降級，不會讓
  這 7 位重新漂移回本次要修正的錯誤狀態。可以重新進 Owner write approval。

VOID_PATH_ITSELF：CASE D——是否要修 reevaluate_tier_after_void()、要修成
  什麼樣子（改回視窗制 vs. 做完整 replay vs. 維持現狀但加告警），涉及
  §7 提到的邊界情境取捨（顧客 319 那類案例），建議交給 Owner／ChatGPT
  決定下一步，不由本輪自行拍板。
```

## Required Final Report

```
STATE=VOID_TIER_PATH_DECISION_READY

HEAD_BEFORE=09aab0c783793345d803b7eca87f8f997aea2af1
HEAD_AFTER=（本文件 commit 後）
EVIDENCE_COMMIT=（見 git log，本次 commit）

# Code Path
ENTRY_ACCUMULATION_WINDOW=視窗制（已有 tier_effective_date 時）／日曆年
  （首次評等時）
VOID_ACCUMULATION_WINDOW=一律日曆年（customer_year_total），無論是否已有
  既存視窗
SEMANTICS_IDENTICAL=NO
BUSINESS_JUSTIFICATION_FOR_DIFFERENT_VOID_SEMANTICS=NOT_FOUND（找到的是
  commit 6097b09 的工程判斷註解，非外部核准商業規則）

# Production Exposure
VOID_TRANSACTIONS_TOTAL=23
VOID_TRANSACTIONS_WITH_TIER_REEVALUATION=5（觸發交易數，對應 5 位顧客）
AFFECTED_CUSTOMERS=5（302, 301, 319, 533, 464）
MATCH=3（319, 533, 464）
MISMATCH=2（301=WRONG_CURRENT_TIER；302=WRONG_EFFECTIVE_DATE，與此 void
  事件無關的既有問題）
SOURCE_INCOMPLETE=0

# Customer 301
CUSTOMER_301_VOID_RESULT=PARTIAL——void path 精確解釋了 log 裡的異常數字，
  但不能解釋她之後為何又回到 P 級（另有未留痕的寫入）；她的 EXPECTED
  （S級美咖）在三個獨立角度下一致，既有 7 位 remediation 的 BEFORE/AFTER
  不需要更動
CUSTOMER_301_ROOT_CAUSE=主要根因仍是 Group A（tier_effective_date 本身
  誤算，見既有 Fact Sheet）；void path 是次要的、疊加的既有 bug，不是她
  異常的主因

# Customer 302
CUSTOMER_302_VOID_RESULT=NO——這次降級判斷本身算錯（應維持 A 級卻算出
  P 級），但沒有留存到目前狀態，對她現在的資料沒有實質影響
CUSTOMER_302_ROOT_CAUSE=tier_effective_date 落差（04-12 vs 05-31）是獨立
  於此次 void 事件的既有問題，早於 void 事件就存在，void 事件的寫入本身
  也被後續交易覆蓋掉，兩者不是同一個問題

# Future Safety
VOID_PATH_BUG_CONFIRMED=YES
FUTURE_REDRIFT_RISK=對 7 位 remediation：LOW（void path 只降級不升級，
  且日曆年公式恆≥視窗制公式，不會把他們推回本次要修正的錯誤高等級）；
  對系統整體：MEDIUM（獨立、持續存在的既有 bug，未來任何人被作廢交易都
  可能受影響，非本輪 7 位專屬）
CAN_REUSE_SHARED_CORE=PARTIAL（`_window_total` 可直接重用做最小修正，但
  無法完全複現本次拿來當 oracle 用的「排除該筆後完整重放」精確度，尤其
  在視窗前已合格消費的邊界情境）

# 7-Customer Remediation Script
REMEDIATION_AUDIT_ROW_TABLE=tier_upgrades
REMEDIATION_AUDIT_ROW_SAFE=YES
REMEDIATION_SCRIPT_STATUS=NOT_BLOCKED

# Safety
CODE_CHANGES=0
PRODUCTION_FINANCIAL_WRITES=0
PRODUCTION_TIER_METADATA_WRITES=0

# Decision
CASE=A（7位remediation）+ D（void path本身修復時機，交由Owner決定）
RECOMMENDED_NEXT_GATE=7 位 remediation 可重新提交 Owner write approval
  （不受此 path 阻擋）；void path 本身是否/何時修復，另待 Owner 決定，
  本輪不執行任何程式碼修改
BLOCKERS=無（對 7 位 remediation 而言）；void path 本身的既有 bug 仍待
  Owner 排入後續 Gate
```

`PRODUCTION_FINANCIAL_WRITES=0`、`PRODUCTION_TIER_METADATA_WRITES=0`、
`CODE_CHANGES=0`。本輪純查核，未修改任何程式碼或 Production 資料。
