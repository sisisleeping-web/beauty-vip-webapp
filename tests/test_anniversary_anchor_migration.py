"""B1-01 isolated migration contracts; no production DB access."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import app
from scripts.migrate_tier_anniversary_anchor import apply, verify


class AnniversaryAnchorMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "isolated.db"
        self.original_path = app.DB_PATH
        app.DB_PATH = self.db_path
        app.init_db()

    def tearDown(self):
        app.DB_PATH = self.original_path
        self.tmp.cleanup()

    def test_additive_and_idempotent_without_backfill(self):
        # Emulate a pre-B1-01 schema; fresh init_db already includes the column.
        with sqlite3.connect(self.db_path) as db:
            db.execute("DROP TABLE customers")
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, birthday TEXT, created_at TEXT)")
            db.execute("INSERT INTO customers(name,birthday,created_at) VALUES('舊會員','1990-01-01','2026-01-01')")
            db.commit()
            with self.assertRaises(RuntimeError):
                verify(db)
            apply(db)
            verify(db)
            apply(db)
            cols = [row[1] for row in db.execute("PRAGMA table_info(customers)")]
            self.assertEqual(cols.count("tier_anniversary_anchor"), 1)
            self.assertIsNone(db.execute("SELECT tier_anniversary_anchor FROM customers").fetchone()[0])

    def test_missing_customers_fails_closed(self):
        with sqlite3.connect(":memory:") as db:
            with self.assertRaises(RuntimeError):
                apply(db)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 0)

    def test_fresh_database_has_nullable_anchor(self):
        with sqlite3.connect(self.db_path) as db:
            verify(db)
            db.execute("INSERT INTO customers(name,birthday,created_at) VALUES('新會員','1991-01-01','2026-01-01')")
            db.commit()
            self.assertIsNone(db.execute("SELECT tier_anniversary_anchor FROM customers").fetchone()[0])

    def test_legacy_undo_snapshot_with_current_anchor_fails_before_ledger_moves(self):
        with app.app.app_context():
            db = app.get_db()
            keep_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,tier_anniversary_anchor) "
                "VALUES('舊合併主會員','1990-01-01','2026-01-01','2024-02-29')"
            ).lastrowid
            absorb_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,merged_into_customer_id) "
                "VALUES('舊合併副會員','1991-01-01','2026-01-01',?)", (keep_id,)
            ).lastrowid
            txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,created_at) "
                "VALUES(?,'store_a','2026-01-01','2026-01',100,100,'2026-01-01')", (keep_id,)
            ).lastrowid
            import json
            snapshots = [dict(db.execute("SELECT * FROM customers WHERE id=?", (cid,)).fetchone())
                         for cid in (keep_id, absorb_id)]
            for snapshot in snapshots:
                snapshot.pop("tier_anniversary_anchor")
            op_id = db.execute(
                "INSERT INTO customer_merge_operations("
                "keep_customer_id,absorbed_customer_id,reason,keep_before_json,absorbed_before_json,"
                "moved_ids_json,status,created_at) VALUES(?,?,?,?,?,?,'merged','2026-01-02')",
                (keep_id, absorb_id, '舊版合併', json.dumps(snapshots[0]),
                 json.dumps(snapshots[1]), json.dumps({"transactions": [txn_id]}))
            ).lastrowid
            db.commit()
        client = app.app.test_client()
        with client.session_transaction() as session:
            session["manager_authed"] = True
        response = client.post(
            f"/api/customer-merges/{op_id}/undo",
            data={"reason": "隔離測試", "confirmed": "1"}
        )
        self.assertEqual(response.status_code, 409)
        with app.app.app_context():
            db = app.get_db()
            self.assertEqual(
                db.execute("SELECT customer_id FROM transactions WHERE id=?", (txn_id,)).fetchone()[0],
                keep_id
            )
            self.assertEqual(
                db.execute("SELECT status FROM customer_merge_operations WHERE id=?", (op_id,)).fetchone()[0],
                "merged"
            )

    def test_conflicting_anchors_block_merge_without_moving_transactions(self):
        with app.app.app_context():
            db = app.get_db()
            first = db.execute("INSERT INTO customers(name,birthday,created_at,tier_anniversary_anchor) VALUES('甲','1990-01-01','2026-01-01','2024-02-29')").lastrowid
            second = db.execute("INSERT INTO customers(name,birthday,created_at,tier_anniversary_anchor) VALUES('乙','1991-01-01','2026-01-01','2024-03-01')").lastrowid
            db.commit()
        client = app.app.test_client()
        with client.session_transaction() as session:
            session["manager_authed"] = True
        response = client.post("/api/customers/merge", data={
            "keep_id": str(first), "absorb_id": str(second),
            "confirmed": "1", "reason": "測試衝突錨點"
        })
        self.assertEqual(response.status_code, 409)
        with app.app.app_context():
            db = app.get_db()
            self.assertIsNone(db.execute("SELECT merged_into_customer_id FROM customers WHERE id=?", (second,)).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
