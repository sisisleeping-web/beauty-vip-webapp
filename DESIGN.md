# DESIGN.md — 美咖美容 VIP 管理系統

> 給 AI coding agent 用的設計規範。改任何頁面前先看這份文件，不要臨時發明顏色/圓角/陰影數值。
> 手法參考自 [家庭記帳本](../family-accounting/DESIGN.md) 的軟體工藝（等寬字體標金額、膠囊徽章、
> 分層陰影、九章節結構），但**配色保留美咖既有的紫色品牌**，不套用記帳本的米白奶茶色系——
> 兩個產品調性不同（本站是桌面版店家後台工具，不是手機隨身記帳app）。

## 1. Visual Theme & Mood

專業、活力的美容業後台系統。紫色→粉紅漸層品牌色 + 淺藍灰底色，卡片邊緣圓潤、陰影柔和有層次。
金額與點數數字用等寬字體強調「這是要仔細核對的數字」，狀態（VIP等級、預約狀態、贈禮進度）
一律用膠囊徽章而不是純色文字，一眼能分辨。桌面優先（店員用電腦/平板操作），非手機隨身工具。

## 2. Color Palette & Roles

| Token | 值 | 角色 |
|---|---|---|
| `--primary` | `#6c3fc5` | 品牌主色（既有紫色，不變）—— 標題、主要按鈕、連結 |
| `--primary-light` | `#a855f7` | 品牌漸層中段 —— 按鈕漸層、focus 外環 |
| `--accent` | `#ec4899` | 品牌漸層尾段（粉紅）—— 只用在 header/首頁漸層，不單獨用 |
| `--primary-bg` | `#ede9fe` | 品牌淡底 —— 選中的篩選 chip、次要按鈕背景 |
| `--primary-bg-soft` | `#f8f6ff` | 更淡的品牌底 —— 表頭、hover 底色 |
| `--bg-color` | `#f4f6fb` | 頁面底色（淺藍灰，襯托紫色品牌） |
| `--card-bg` | `#ffffff` | 卡片/表面 |
| `--border-color` | `#ede9ff` | 卡片邊框、表格格線 |
| `--border-strong` | `#d4c9f0` | 輸入框邊框 |
| `--text-color` | `#1a1a2e` | 主要文字 |
| `--text-heading` | `#2d1b69` | 標題文字（h3） |
| `--text-muted` | `#6b7280` | 次要文字/標籤 —— **統一取代**原本 `#888`/`#555`/`#666`/`#999`/`#aaa`/`#7f8c8d` 等混用的灰階 |
| `--success` | `#059669` | 語意：好消息（已確認、已送達、獲得點數、營收） |
| `--danger` | `#dc2626` | 語意：警示（刪除、已取消、扣除點數） |
| `--warning` | `#d97706` | 語意：提醒（待處理、壽星充值、新客） |
| `--tier-s` / `--tier-s-bg` | `#1d4ed8` / `#dbeafe` | S級美咖徽章專用色 |
| `--tier-p` / `--tier-p-bg` | `#b45309` / `#fef3c7` | P級美咖徽章專用色 |
| `--tier-a-text` / `--tier-a-bg` | `#78350f` / 漸層`#fde68a→#fbbf24` | A級美咖徽章專用色（唯一用漸層底的等級） |

**語意色使用鐵則**：紅綠不是固定「正面=綠負面=紅」，要看指標本身的好壞方向判斷（例如「本月營收」
增加是好事用 success；「扣除點數」不管多少都用 danger 因為那是負向動作，不是壞消息本身）。

**已修正的實際不一致**：改版前 VIP 等級徽章在 `report.html`／`contacts.html`／`my.html` 三處
各自寫了不同的顏色組合（同一個「S級美咖」在報表頁跟通訊名單頁顯示不同顏色），現在統一用
`.tier-badge`／`.tier-{{ tier名稱 }}` class，只在 `base.html`（或 `my.html`，它不 extend
base.html，見第9節）維護一份。

## 3. Typography

- 主要字體：`Noto Sans TC`（既有，維持）
- 金額／點數數字專用：新增 `JetBrains Mono`（等寬），透過 `.font-mono` class 套用，只用在
  **金額/點數/營收數字**，不要用在一般文字或標籤

| 用途 | 字級 | 字重 | 字體 |
|---|---|---|---|
| 卡片內大金額（如點數餘額） | `1.05–1.35rem` | 700 | JetBrains Mono |
| 表格內金額 | 表格預設 `14px` | 700（`.font-mono` 內建 `font-weight:700`） | JetBrains Mono |
| 卡片標題 | `1.15rem`（h3） | 700 | Noto Sans TC |
| 標籤/次要文字 | `0.85rem` | 500 | Noto Sans TC |

## 4. Component Styles

**Card（`.card`）**
- 圓角 `16px`（`--radius-lg`）
- 陰影：`0 4px 20px rgba(108,63,197,.07), 0 1px 3px rgba(108,63,197,.08)`（`--shadow-card`）
- 邊框：`1px solid var(--border-color)`

**Button（裸 `<button>`）**
- 圓角 `8px`（`--radius-sm`）
- 預設陰影：`--shadow-btn`
- Active（按下）：內陰影 `inset 0 2px 4px rgba(0,0,0,.12)`，取代單純 `opacity`，按下要有觸感回饋
- 小型行內操作按鈕（表格內「修改」「刪除」等）：加 `box-shadow:none; min-height:unset;` 蓋掉
  全域按鈕的陰影跟高度，避免表格被撐開

**Input / Select**
- 內陰影：`inset 0 2px 4px rgba(108,63,197,.04)`
- Focus：邊框轉 `var(--primary-light)` + 外環 `0 0 0 3px rgba(168,85,247,.16)`

**Badge Pill（`.badge-pill` + 修飾字 class，新元件，取代各頁各自寫的內嵌樣式）**
- 膠囊形（`border-radius: 999px`）
- `.badge-pill-success` / `-danger` / `-warning` / `-neutral`：背景用語意色 12% 透明度、文字用
  該語意色本身
- VIP 等級另外用 `.tier-badge` + `.tier-一般會員`／`.tier-S級美咖`／`.tier-P級美咖`／`.tier-A級美咖`
  （四個等級各自固定色碼，見第2節）

## 5. Layout Principles

- 桌面優先，容器 `max-width: 1100px` 置中（既有，不變，跟記帳本的手機版 `600px` 不同）
- 頂部固定橫向導覽列（既有 `.site-header`/`nav`，不是底部導覽）
- 卡片之間垂直間距 `14px`
- 篩選表單用 `grid`/`flex` 自動換行，不手動排版

## 6. Depth System（陰影分級）

由淺到深，對應「越會被操作/越重要」陰影越明顯：
1. 靜態卡片（資訊展示）—— 最淺，`--shadow-card`
2. 可點擊按鈕（預設狀態）—— 中等，`--shadow-btn`
3. 按鈕按下（active）—— 內陰影，製造「壓下去」的觸覺回饋
4. 表格內小型行內按鈕 —— 無陰影（`box-shadow:none`），避免密集表格看起來雜亂

## 7. Design Guidelines（禁忌）

- 不換掉既有紫色品牌（`--primary`/`--primary-light`/`--accent`），這是刻意保留的識別色，不是
  「AI 預設紫色」要避免的那種情況
- 首頁功能磚塊（`home.html`）的多彩漸層是刻意的視覺導航設計（每個功能一個顏色方便辨識），
  不要把它們也統一成單一品牌色
- 圓角統一走 `--radius-sm/md/lg/pill` 四級，不要臨時寫 `10px`/`14px`/`20px` 這類散落數值
- 金額一律套 `.font-mono`，不要跟一般文字混用字體造成不一致
- 狀態一律用 `.badge-pill`/`.tier-badge`，不要再用純色文字＋emoji 表示狀態（改版前
  `spa_admin.html` 的預約狀態就是這樣，已修正成 badge-pill）

## 8. Responsive Behavior

- 斷點沿用既有 `600px`（`.main-content` padding 收窄、`.card` padding 收窄、input 寬度轉 100%）
- 這是桌面優先工具，手機斷點只做「不要爆版」的最低限度調整，不重新設計版面

## 9. 已知的特例／技術債

- `templates/my.html`（顧客自助查詢頁）**不 extend `base.html`**，是獨立的 `<html>` 頁面
  （公開網址，未登入也能訪問，不套用內部系統的頂部導覽）。它的 `<style>` 區塊自己重複定義了一份
  `:root` token（跟 base.html 數值必須保持一致），改 base.html 的色票時記得同步改這裡。
- `templates/spa_booking.html`（顧客端預約日曆）有大量互動式行事曆專屬樣式（可選/已滿/選中時段
  的顏色狀態），這次改版**沒有深入處理**，只確保它繼承 base.html 的卡片/按鈕基礎樣式；行事曆
  本身的顏色系統之後要動再另外處理。

## 10. AI Prompt Guide

改版任何頁面時：
1. 顏色、圓角、陰影數值一律查本文件第 2、4、6 節，不要臨時決定新數值
2. 金額/點數數字套 `.font-mono`；一般文字維持 `Noto Sans TC`
3. 狀態顯示一律用 `.badge-pill`（一般狀態）或 `.tier-badge`（VIP 等級），不要寫新的內嵌顏色判斷
4. 新元件優先重用 `.card`/裸 `button`/`.badge-pill`/`.tier-badge`，不要另開一套新 class
5. `my.html` 是特例（見第9節），改動色票時兩邊都要改
6. 保留品牌紫色與首頁多彩導航磚塊，這兩個是刻意設計不是要清除的「AI 預設色」
