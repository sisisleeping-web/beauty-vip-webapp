#!/usr/bin/env python3
"""Repair the member tier of a customer already merged before merge reconciliation existed.

Run this on PythonAnywhere, where the production DB is the source of truth:
    python3 scripts/repair_merged_customer_tier.py --customer-id 302 --expect-tier 'A級美咖'

Use --dry-run first to print the intended result and roll it back.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import app as beauty  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer-id", type=int, required=True)
    parser.add_argument("--expect-tier", default="")
    parser.add_argument("--db", type=Path, default=beauty.DB_PATH, help="Override DB path for offline verification")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    customer = db.execute(
        "SELECT id,name,member_tier,tier_effective_date,tier_expires_date FROM customers WHERE id=?",
        (args.customer_id,),
    ).fetchone()
    if not customer:
        print(f"customer_id={args.customer_id} not found", file=sys.stderr)
        return 1

    before = dict(customer)
    prior_state, _ = beauty._project_tier_state(db, args.customer_id, date.today().isoformat())
    after = beauty._reconcile_tier_after_merge(db, args.customer_id, [prior_state], date.today())
    year_total = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) FROM transactions "
        "WHERE customer_id=? AND substr(txn_date,1,4)=? AND final_amount>=1000 AND voided_at IS NULL",
        (args.customer_id, str(date.today().year)),
    ).fetchone()[0]

    print(f"customer={before['name']} id={args.customer_id}")
    print(f"before={before['member_tier']} {before['tier_effective_date']}~{before['tier_expires_date']}")
    print(f"after={after['member_tier']} {after['tier_effective_date']}~{after['tier_expires_date']}")
    print(f"current_year_qualified_spend={float(year_total):.0f}")

    if args.expect_tier and after["member_tier"] != args.expect_tier:
        db.rollback()
        print(f"expected={args.expect_tier}, got={after['member_tier']}", file=sys.stderr)
        return 2
    if args.dry_run:
        db.rollback()
        print("dry-run: rolled back")
    else:
        db.commit()
        print("committed")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
