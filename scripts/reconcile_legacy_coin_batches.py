#!/usr/bin/env python3
"""安全收斂舊制轉入批次與後補來源批次的重複入帳。

Phase 3 導入時，舊餘額被轉成 ``is_legacy=1`` 批次。若該快照後又補建一筆
「快照前已存在交易」的來源批次，會讓同一筆點數同時存在於兩個批次。本工具只處理
可機器驗證、且尚未被使用的候選；其他情況 fail-closed，不修改資料。
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "beauty_vip.db"


def find_candidates(db: sqlite3.Connection) -> list[sqlite3.Row]:
    return db.execute(
        """
        WITH legacy AS (
            SELECT customer_id, MAX(created_at) AS legacy_created
            FROM coin_batches
            WHERE is_legacy=1
            GROUP BY customer_id
        )
        SELECT b.id AS batch_id, b.customer_id, c.name, b.earned_amount,
               b.remaining_amount, b.created_at AS batch_created,
               t.id AS txn_id, t.created_at AS txn_created, t.txn_date
        FROM coin_batches b
        JOIN legacy l ON l.customer_id=b.customer_id
        JOIN customers c ON c.id=b.customer_id
        JOIN transactions t ON t.id=b.source_txn_id
        LEFT JOIN coin_redemptions r ON r.batch_id=b.id
        WHERE b.is_legacy=0
          AND b.status='active'
          AND b.source_txn_id IS NOT NULL
          AND b.created_at > l.legacy_created
          AND t.created_at < l.legacy_created
          AND t.voided_at IS NULL
          AND b.remaining_amount=b.earned_amount
          AND r.id IS NULL
        ORDER BY b.customer_id, b.id
        """
    ).fetchall()


def main() -> int:
    parser = argparse.ArgumentParser(description="稽核／收斂舊制轉入造成的重複點數批次")
    parser.add_argument("--apply", action="store_true", help="確認候選後寫入 superseded_by_legacy 狀態")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="目標 SQLite DB")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        candidates = find_candidates(db)
        print(f"candidates={len(candidates)} mode={'apply' if args.apply else 'dry-run'}")
        for row in candidates:
            print(
                f"batch={row['batch_id']} customer={row['customer_id']} {row['name']} "
                f"points={row['remaining_amount']} txn={row['txn_id']} date={row['txn_date']}"
            )
        if not args.apply or not candidates:
            return 0

        for row in candidates:
            db.execute(
                "UPDATE coin_batches SET status='superseded_by_legacy' WHERE id=? AND status='active'",
                (row["batch_id"],),
            )
        db.commit()

        # Import only after the status change; this uses the production application's canonical balance rule.
        import sys
        sys.path.insert(0, str(ROOT))
        import app as beauty  # noqa: PLC0415

        for customer_id in {int(row["customer_id"]) for row in candidates}:
            beauty.sync_coin_balance(db, customer_id)
        db.commit()
        print(f"reconciled={len(candidates)}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
