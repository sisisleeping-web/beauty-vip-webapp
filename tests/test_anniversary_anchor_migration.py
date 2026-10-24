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
