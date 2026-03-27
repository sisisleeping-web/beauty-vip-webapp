# Beauty VIP WebApp (v0.1)

兩家分店使用的美容客戶消費管理系統（網站版）。

## 功能
- 前台快速輸入：姓名、生日、本日消費、分店
- 自動計算：
  - 壽星優惠（預設 9 折）
  - 月累計消費
  - VIP 等級、回饋金、點數
- 報表查詢：日明細 / 月消費 / 年消費
- 主管儀表板：跨店彙總

## 啟動
```bash
cd projects/beauty-vip-webapp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

開啟： http://localhost:5090

## 規則設定
編輯 `rules.json`：
- `birthday_offer.discount_rate`：壽星折扣（0.1 = 9折）
- `vip_tiers`：各等級門檻與回饋比率

## 備註
- 目前為 v0.1，尚未加登入權限（主管頁需後續加帳密/角色控管）
- 資料儲存在 `data/beauty_vip.db`
