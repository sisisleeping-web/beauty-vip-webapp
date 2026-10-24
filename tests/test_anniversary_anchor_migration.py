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
        with sqlite3.connect(self.db_path) as db:
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


if __name__ == "__main__":
    unittest.main()
