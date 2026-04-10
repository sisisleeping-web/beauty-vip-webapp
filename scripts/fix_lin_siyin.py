#!/usr/bin/env python3
"""
Fix: 林思吟 has two customer records with slightly different birthdays:
  - Correct: birthday 1982-03-29  (has 3/9 txn, 20000, P級)
  - Wrong:   birthday 1982-03-30  (has 3/30 txn, 8799, calculated at S級 by mistake)

Steps:
1. Find both customer records
2. Move the 3/30 transaction to the correct customer (1982-03-29)
3. Recalculate coins for the 3/30 txn at P級 rate (since max_single from 3/9 = 20000)
4. Update coin_balance on the correct customer
5. Delete the wrong customer record (1982-03-30)

Also fixes the fix_coins_history.py approach: now uses txn_date for ordering
so that transactions entered out of DB ID order still get the right tier.
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

# ── Step 1: Find both records ───────────────────────────────────────────────
records = cur.execute(
    "SELECT id, name, birthday, coin_balance FROM customers WHERE name='林思吟' ORDER BY birthday"
).fetchall()

print("=== 林思吟 customer records found ===")
for r in records:
    print(f"  id={r['id']}, birthday={r['birthday']}, coin_balance={r['coin_balance']}")

if len(records) < 2:
    print("\n⚠️  Less than 2 records found. Checking if merge already done.")
    # Even if one record, still re-check the 3/30 txn coins
    pass

# Identify correct vs wrong record
correct = None
wrong = None
for r in records:
    if r["birthday"] == "1982-03-29":
        correct = r
    elif r["birthday"] == "1982-03-30":
        wrong = r

if correct is None:
    print("ERROR: Cannot find correct customer (1982-03-29). Aborting.")
    conn.close()
    exit(1)

print(f"\nCorrect customer: id={correct['id']} birthday={correct['birthday']}")

# ── Step 2: Move wrong customer's transactions to correct customer ──────────
if wrong:
    print(f"Wrong customer:  id={wrong['id']} birthday={wrong['birthday']}")
    
    wrong_txns = cur.execute(
        "SELECT id, txn_date, final_amount, coins_earned FROM transactions WHERE customer_id=?",
        (wrong["id"],)
    ).fetchall()
    print(f"\nMoving {len(wrong_txns)} transactions from wrong → correct customer:")
    for t in wrong_txns:
        print(f"  txn_id={t['id']} date={t['txn_date']} amount={t['final_amount']} coins={t['coins_earned']}")
    
    cur.execute(
        "UPDATE transactions SET customer_id=? WHERE customer_id=?",
        (correct["id"], wrong["id"])
    )
    
    # Revert wrong customer's coin_balance impact (those coins will be re-added via correct customer)
    # The wrong customer's coin_balance represents coins already counted - don't double count
    # We'll recalculate properly below, so just merge balances
    merged_coins = int(correct["coin_balance"] or 0) + int(wrong["coin_balance"] or 0)
    cur.execute(
        "UPDATE customers SET coin_balance=? WHERE id=?",
        (merged_coins, correct["id"])
    )
    
    cur.execute("DELETE FROM customers WHERE id=?", (wrong["id"],))
    print(f"\n✅ Merged: deleted wrong customer id={wrong['id']}")
    print(f"   Temporary coin_balance: {merged_coins}")

# ── Step 3: Re-calculate ALL transactions for correct customer ────────────
# Use txn_date ordering (not id) to correctly determine "past" state
print(f"\n=== Recalculating all transactions for customer id={correct['id']} ===")

all_txns = cur.execute(
    """
    SELECT id, txn_date, final_amount, coins_earned, cashback, entry_mode
    FROM transactions
    WHERE customer_id=? AND entry_mode='normal' AND final_amount > 0
    ORDER BY txn_date ASC, id ASC
    """,
    (correct["id"],)
).fetchall()

total_coin_delta = 0
recalculated = []

for txn in all_txns:
    txn_id = txn["id"]
    amount = float(txn["final_amount"])
    old_coins = int(txn["coins_earned"] or 0)
    old_cashback = float(txn["cashback"] or 0)
    year_str = txn["txn_date"][:4]
    txn_date = txn["txn_date"]

    # Use txn_date to find past state (chronologically correct)
    past_max = float(
        cur.execute(
            """SELECT COALESCE(MAX(final_amount),0)
               FROM transactions
               WHERE customer_id=? AND txn_date < ? AND entry_mode='normal'""",
            (correct["id"], txn_date),
        ).fetchone()[0] or 0
    )
    year_before = float(
        cur.execute(
            """SELECT COALESCE(SUM(final_amount),0)
               FROM transactions
               WHERE customer_id=? AND substr(txn_date,1,4)=?
               AND txn_date < ? AND entry_mode='normal'""",
            (correct["id"], year_str, txn_date),
        ).fetchone()[0] or 0
    )

    effective_single = max(past_max, amount)
    tier = get_tier(effective_single, year_before)
    rate = RATE_MAP[tier]

    new_coins = int(amount * rate)
    new_cashback = round(amount * rate, 2)

    print(f"  txn_id={txn_id} date={txn['txn_date']} amount={amount}")
    print(f"    past_max={past_max} year_before={year_before} → {tier}")
    print(f"    coins: {old_coins} → {new_coins} ({new_coins-old_coins:+d})")

    if new_coins != old_coins or abs(new_cashback - old_cashback) > 0.01:
        total_coin_delta += (new_coins - old_coins)
        recalculated.append((new_coins, new_cashback, txn_id))
        cur.execute(
            "UPDATE transactions SET coins_earned=?, cashback=? WHERE id=?",
            (new_coins, new_cashback, txn_id)
        )

# ── Step 4: Fix coin_balance on correct customer ─────────────────────────
# Recalculate from scratch to be safe
cur_balance = cur.execute(
    "SELECT coin_balance FROM customers WHERE id=?", (correct["id"],)
).fetchone()["coin_balance"]

# Calculate what coin_balance SHOULD be:
# sum of all coins_earned (normal + recharge) minus sum of all coins_redeemed
true_balance = cur.execute(
    """SELECT
         COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END), 0)
         - COALESCE(SUM(coins_redeemed), 0) AS net
       FROM transactions WHERE customer_id=?""",
    (correct["id"],)
).fetchone()["net"]

# Note: coins_earned was just updated above, so this reflects new values
true_balance_after = cur.execute(
    """SELECT
         COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END), 0)
         - COALESCE(SUM(coins_redeemed), 0) AS net
       FROM transactions WHERE customer_id=?""",
    (correct["id"],)
).fetchone()["net"]

print(f"\n=== Fixing coin_balance ===")
print(f"  Current stored balance: {cur_balance}")
print(f"  True balance (from transactions): {true_balance_after}")

cur.execute(
    "UPDATE customers SET coin_balance=? WHERE id=?",
    (true_balance_after, correct["id"])
)

conn.commit()
conn.close()

print(f"\n✅ Done!")
print(f"  林思吟 coin_balance: {cur_balance} → {true_balance_after}")
print(f"  Updated {len(recalculated)} transactions")
