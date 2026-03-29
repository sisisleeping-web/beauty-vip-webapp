#!/usr/bin/env python3
"""
pa_migrate.py — 在 PythonAnywhere 執行 DB 資料修復/遷移

上傳此腳本到 PA 後執行：
  python3 pa_migrate.py

已封裝的操作（依 TODO 解注解後執行）：
  - fix_birthday_discount: 回補折扣記錄
  - recalc_all_coins: 重算所有顧客點數
  - check_integrity: 完整性驗算
"""
import sqlite3, json
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "data" / "beauty_vip.db"
RULES_PATH = BASE / "rules.json"

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

with open(RULES_PATH) as f:
    rules = json.load(f)

def tier_name(max_single, year_total):
    if max_single >= 30000 or year_total >= 60000: return "A級美咖"
    if max_single >= 12000 or year_total >= 24000: return "P級美咖"
    if max_single >= 8000 or year_total >= 15000: return "S級美咖"
    return "一般會員"

def tier_rates(name):
    for t in rules["vip_tiers"]:
        if t["name"] == name:
            return t["cashback_rate"], t["points_rate"]
    return 0.0, 0.0

def recalc_customer_coins(cust_id):
    """重算單一顧客所有 normal 交易的 coins_earned/cashback，並更新 coin_balance"""
    txns = conn.execute("""
        SELECT id, txn_date, final_amount FROM transactions
        WHERE customer_id=? AND entry_mode='normal'
        ORDER BY txn_date ASC, id ASC
    """, (cust_id,)).fetchall()

    running_max, year_totals = 0.0, {}
    for txn in txns:
        yr = txn["txn_date"][:4]
        yt = year_totals.get(yr, 0.0)
        tier = tier_name(running_max, yt)
        cr, pr = tier_rates(tier)
        coins = int(txn["final_amount"] * pr)
        cashback = round(txn["final_amount"] * cr, 2)
        conn.execute("UPDATE transactions SET coins_earned=?, cashback=? WHERE id=?", (coins, cashback, txn["id"]))
        running_max = max(running_max, txn["final_amount"])
        year_totals[yr] = yt + txn["final_amount"]

    row = conn.execute("""
        SELECT
          COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END),0) AS earned,
          COALESCE(SUM(CASE WHEN entry_mode='coin_deduct' THEN coins_redeemed ELSE 0 END),0) AS redeemed
        FROM transactions WHERE customer_id=?
    """, (cust_id,)).fetchone()
    correct = int(row["earned"]) - int(row["redeemed"])
    conn.execute("UPDATE customers SET coin_balance=? WHERE id=?", (correct, cust_id))
    return correct


def check_integrity():
    ok = True
    disc = conn.execute("SELECT COUNT(*) FROM transactions WHERE birthday_discount_applied=1").fetchone()[0]
    mm = conn.execute("SELECT COUNT(*) FROM transactions WHERE entry_mode='normal' AND ABS(final_amount-amount)>0.01").fetchone()[0]
    custs = conn.execute("SELECT id, name, coin_balance FROM customers").fetchall()
    bad = 0
    for c in custs:
        row = conn.execute("""
            SELECT COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END),0) AS e,
                   COALESCE(SUM(CASE WHEN entry_mode='coin_deduct' THEN coins_redeemed ELSE 0 END),0) AS r
            FROM transactions WHERE customer_id=?
        """, (c["id"],)).fetchone()
        if int(row["e"]) - int(row["r"]) != c["coin_balance"]:
            bad += 1
    print(f"birthday_discount=1: {disc} (expect 0)")
    print(f"final_amount≠amount: {mm} (expect 0)")
    print(f"coin_balance mismatch: {bad}/{len(custs)} (expect 0)")
    return disc == 0 and mm == 0 and bad == 0


# ─── 執行選項（解注解要執行的區塊）────────────────────────────────────────

# 1. 修復折扣記錄
# affected = [r["customer_id"] for r in conn.execute("SELECT DISTINCT customer_id FROM transactions WHERE birthday_discount_applied=1").fetchall()]
# conn.execute("UPDATE transactions SET final_amount=amount, birthday_discount_applied=0 WHERE birthday_discount_applied=1")
# for cid in affected:
#     recalc_customer_coins(cid)
# conn.commit()
# print(f"Fixed {len(affected)} customers")

# 2. 重算全部顧客點數（謹慎使用）
# all_ids = [r[0] for r in conn.execute("SELECT id FROM customers").fetchall()]
# for cid in all_ids:
#     recalc_customer_coins(cid)
# conn.commit()
# print(f"Recalculated {len(all_ids)} customers")

# 3. 完整性驗算（永遠安全，只讀）
print("=== Integrity Check ===")
ok = check_integrity()
print("✓ All OK" if ok else "✗ Issues found")

conn.close()
