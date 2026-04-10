#!/usr/bin/env python3
"""
Recalculate ALL normal transactions using the CORRECT business rule:
  - Tier is determined solely by PAST transactions (txn_date < current txn)
  - A big single purchase earns points at the CURRENT tier (before upgrade)
  - Tier upgrade takes effect from the NEXT transaction

Uses txn_date ordering (not id) to handle data imported out of DB-id order.

This script corrects the previous over-crediting caused by fix_coins_history.py
which incorrectly included current_amount in tier calculation.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path.home() / "beauty-vip-webapp" / "data" / "beauty_vip.db"

RATE_MAP = {
    "A級美咖": 0.08,
    "P級美咖": 0.05,
    "S級美咖": 0.03,
    "一般會員": 0.02,
}


def get_tier(past_max_single: float, year_total_before: float) -> str:
    """Tier based on PAST transactions only — no current_amount included."""
    if past_max_single >= 30000 or year_total_before >= 60000:
        return "A級美咖"
    if past_max_single >= 12000 or year_total_before >= 24000:
        return "P級美咖"
    if past_max_single >= 8000 or year_total_before >= 15000:
        return "S級美咖"
    return "一般會員"


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Fetch all normal transactions ordered by customer + txn_date
rows = cur.execute(
    """
    SELECT t.id, t.customer_id, t.final_amount, t.coins_earned, t.cashback, t.txn_date
    FROM transactions t
    WHERE t.entry_mode = 'normal' AND t.final_amount > 0
    ORDER BY t.customer_id, t.txn_date ASC, t.id ASC
    """
).fetchall()

updates = []
coin_deltas: dict[int, int] = {}

for r in rows:
    txn_id = r["id"]
    cust_id = r["customer_id"]
    amount = float(r["final_amount"])
    old_coins = int(r["coins_earned"] or 0)
    old_cashback = float(r["cashback"] or 0)
    txn_date = r["txn_date"]
    year_str = txn_date[:4]

    # Use txn_date ordering — tier determined by PAST txns only
    past_max = float(
        cur.execute(
            """SELECT COALESCE(MAX(final_amount),0) FROM transactions
               WHERE customer_id=? AND txn_date < ? AND entry_mode='normal'""",
            (cust_id, txn_date),
        ).fetchone()[0]
        or 0
    )
    year_before = float(
        cur.execute(
            """SELECT COALESCE(SUM(final_amount),0) FROM transactions
               WHERE customer_id=? AND substr(txn_date,1,4)=?
               AND txn_date < ? AND entry_mode='normal'""",
            (cust_id, year_str, txn_date),
        ).fetchone()[0]
        or 0
    )

    tier = get_tier(past_max, year_before)
    rate = RATE_MAP[tier]
    new_coins = int(amount * rate)
    new_cashback = round(amount * rate, 2)

    if new_coins != old_coins or abs(new_cashback - old_cashback) > 0.01:
        delta = new_coins - old_coins
        updates.append((new_coins, new_cashback, txn_id, cust_id, tier, old_coins, delta))
        coin_deltas[cust_id] = coin_deltas.get(cust_id, 0) + delta
        cur.execute(
            "UPDATE transactions SET coins_earned=?, cashback=? WHERE id=?",
            (new_coins, new_cashback, txn_id),
        )

print(f"Found {len(updates)} transactions needing correction:\n")
print(f"{'TxnID':>6} {'等級':<8} {'舊點數':>8} {'新點數':>8} {'差額':>7}")
print("-" * 50)
for new_coins, new_cashback, txn_id, cust_id, tier, old_coins, delta in updates:
    cname = cur.execute("SELECT name FROM customers WHERE id=?", (cust_id,)).fetchone()["name"]
    print(f"{txn_id:>6} {cname:<10} {tier:<8} {old_coins:>8} {new_coins:>8} {delta:>+7}")

print(f"\nUpdating {len(coin_deltas)} customer coin balances...")
for cust_id, delta in coin_deltas.items():
    if delta != 0:
        # Recalculate from scratch for safety
        true_balance = cur.execute(
            """SELECT
                 COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END), 0)
                 - COALESCE(SUM(coins_redeemed), 0) AS net
               FROM transactions WHERE customer_id=?""",
            (cust_id,)
        ).fetchone()["net"]
        row = cur.execute("SELECT name, coin_balance FROM customers WHERE id=?", (cust_id,)).fetchone()
        print(f"  {row['name']}: {row['coin_balance']} → {true_balance} (delta: {delta:+d})")
        cur.execute("UPDATE customers SET coin_balance=? WHERE id=?", (true_balance, cust_id))

conn.commit()
conn.close()
print("\n✅ Done! All corrections committed to DB.")
