#!/usr/bin/env python3
"""Fix historical coins_earned and cashback for transactions where the
current-txn amount itself should have triggered a higher tier.

Bug: calc_tier() previously only considered past_max_single (before this txn),
so a large first-time purchase (e.g. 20000) was calculated at 一般會員 rate
instead of P級 rate. Fixed by using max(past_max_single, current_amount).
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "beauty_vip.db"

RATE_MAP = {
    "A級美咖": 0.08,
    "P級美咖": 0.05,
    "S級美咖": 0.03,
    "一般會員": 0.02,
}


def get_tier(effective_single: float, year_total: float) -> str:
    if effective_single >= 30000 or year_total >= 60000:
        return "A級美咖"
    if effective_single >= 12000 or year_total >= 24000:
        return "P級美咖"
    if effective_single >= 8000 or year_total >= 15000:
        return "S級美咖"
    return "一般會員"


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Fetch all normal transactions ordered by id (chronological per customer)
rows = cur.execute(
    """
    SELECT t.id, t.customer_id, t.final_amount, t.coins_earned, t.cashback, t.txn_date
    FROM transactions t
    WHERE t.entry_mode = 'normal' AND t.final_amount > 0
    ORDER BY t.id
    """
).fetchall()

updates = []  # (new_coins, new_cashback, old_coins, old_cashback, txn_id, cust_id, tier)

for r in rows:
    txn_id = r["id"]
    cust_id = r["customer_id"]
    amount = float(r["final_amount"])
    old_coins = int(r["coins_earned"] or 0)
    old_cashback = float(r["cashback"] or 0)
    year_str = r["txn_date"][:4]

    past_max = float(
        cur.execute(
            "SELECT COALESCE(MAX(final_amount),0) FROM transactions WHERE customer_id=? AND id < ?",
            (cust_id, txn_id),
        ).fetchone()[0]
        or 0
    )
    year_before = float(
        cur.execute(
            "SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=? AND id < ?",
            (cust_id, year_str, txn_id),
        ).fetchone()[0]
        or 0
    )

    # Corrected: use max(past_max, current amount) as effective single
    effective_single = max(past_max, amount)
    tier = get_tier(effective_single, year_before)
    rate = RATE_MAP[tier]

    new_coins = int(amount * rate)
    new_cashback = round(amount * rate, 2)

    if new_coins != old_coins or abs(new_cashback - old_cashback) > 0.01:
        updates.append((new_coins, new_cashback, old_coins, old_cashback, txn_id, cust_id, tier))

print(f"Found {len(updates)} transactions needing correction:\n")
print(f"{'TxnID':>6} {'姓名':<10} {'等級':<8} {'舊點數':>8} {'新點數':>8} {'差額':>7}")
print("-" * 60)

coin_deltas: dict[int, int] = {}  # customer_id -> net coin delta

for new_coins, new_cashback, old_coins, old_cashback, txn_id, cust_id, tier in updates:
    diff = new_coins - old_coins
    cname = cur.execute("SELECT name FROM customers WHERE id=?", (cust_id,)).fetchone()["name"]
    print(f"{txn_id:>6} {cname:<10} {tier:<8} {old_coins:>8} {new_coins:>8} {diff:>+7}")
    coin_deltas[cust_id] = coin_deltas.get(cust_id, 0) + diff
    cur.execute(
        "UPDATE transactions SET coins_earned=?, cashback=? WHERE id=?",
        (new_coins, new_cashback, txn_id),
    )

print(f"\nUpdating {len(coin_deltas)} customer coin balances...")
for cust_id, delta in coin_deltas.items():
    if delta != 0:
        row = cur.execute("SELECT name, coin_balance FROM customers WHERE id=?", (cust_id,)).fetchone()
        old_bal = int(row["coin_balance"] or 0)
        print(f"  {row['name']}: {old_bal} → {old_bal + delta} (delta: {delta:+d})")
        cur.execute(
            "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
            (delta, cust_id),
        )

conn.commit()
conn.close()
print("\n✅ Done! All corrections committed to DB.")
