"""Backfill tier_upgrades table from historical transaction data.

Scans all transactions chronologically per customer, detects tier changes,
and inserts upgrade records with gift_status='skipped' (historical, not to be delivered).

Usage:
    python scripts/backfill_tier_upgrades.py [--deliver]

    --deliver: Set gift_status to 'pending' instead of 'skipped' (if you want to retroactively deliver).
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "beauty_vip.db"
RULES_PATH = BASE_DIR / "rules.json"

TIER_ORDER = ["一般會員", "S級美咖", "P級美咖", "A級美咖"]


def tier_name_from_totals(max_single: float, year_total: float) -> str:
    if max_single >= 30000 or year_total >= 60000:
        return "A級美咖"
    if max_single >= 12000 or year_total >= 24000:
        return "P級美咖"
    if max_single >= 8000 or year_total >= 15000:
        return "S級美咖"
    return "一般會員"


def main():
    deliver_mode = "--deliver" in sys.argv
    default_status = "pending" if deliver_mode else "skipped"

    with open(RULES_PATH, "r", encoding="utf-8") as f:
        rules = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Check if tier_upgrades table exists
    table_check = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='tier_upgrades'"
    ).fetchone()
    if not table_check:
        print("❌ tier_upgrades table does not exist. Run the app first to create it.")
        return

    # Get all customers
    customers = cur.execute("SELECT id, name FROM customers").fetchall()
    print(f"Processing {len(customers)} customers...")

    total_upgrades = 0

    for cust in customers:
        cid = cust["id"]
        cname = cust["name"]

        # Get all normal transactions for this customer, ordered chronologically
        txns = cur.execute(
            """
            SELECT id, txn_date, final_amount
            FROM transactions
            WHERE customer_id = ? AND entry_mode = 'normal' AND final_amount > 0
            ORDER BY txn_date ASC, id ASC
            """,
            (cid,),
        ).fetchall()

        if not txns:
            continue

        current_tier = "一般會員"
        running_max_single = 0.0
        year_totals: dict[str, float] = {}  # year_str -> cumulative

        for txn in txns:
            fa = float(txn["final_amount"])
            year_str = txn["txn_date"][:4]

            # Update running totals
            running_max_single = max(running_max_single, fa)
            year_totals[year_str] = year_totals.get(year_str, 0.0) + fa
            year_total = year_totals[year_str]

            # Determine tier AFTER this transaction
            new_tier = tier_name_from_totals(running_max_single, year_total)

            if new_tier != current_tier:
                idx_before = TIER_ORDER.index(current_tier) if current_tier in TIER_ORDER else 0
                idx_after = TIER_ORDER.index(new_tier) if new_tier in TIER_ORDER else 0

                if idx_after > idx_before:
                    # Determine reason
                    if fa >= running_max_single:
                        reason = f"單筆消費 {int(running_max_single):,} 元達標"
                    else:
                        reason = f"年度累計 {int(year_total):,} 元達標"

                    now_str = datetime.now().isoformat(timespec="seconds")

                    for step in range(idx_before + 1, idx_after + 1):
                        step_from = TIER_ORDER[step - 1]
                        step_to = TIER_ORDER[step]
                        gift_name = f"{step_to} 升級禮"

                        # Check if this upgrade already exists
                        existing = cur.execute(
                            """
                            SELECT id FROM tier_upgrades
                            WHERE customer_id = ? AND tier_before = ? AND tier_after = ? AND trigger_txn_id = ?
                            """,
                            (cid, step_from, step_to, txn["id"]),
                        ).fetchone()

                        if not existing:
                            cur.execute(
                                """
                                INSERT INTO tier_upgrades(
                                    customer_id, upgrade_date, tier_before, tier_after,
                                    trigger_txn_id, trigger_reason, gift_name,
                                    gift_status, created_at
                                ) VALUES(?,?,?,?,?,?,?,?,?)
                                """,
                                (
                                    cid, txn["txn_date"],
                                    step_from, step_to,
                                    txn["id"], reason, gift_name,
                                    default_status, now_str,
                                ),
                            )
                            total_upgrades += 1
                            print(f"  {cname}: {step_from} → {step_to} on {txn['txn_date']} ({reason}) [{default_status}]")

                current_tier = new_tier

    conn.commit()
    conn.close()
    print(f"\n✅ Done! Inserted {total_upgrades} upgrade records (status: {default_status}).")


if __name__ == "__main__":
    main()
