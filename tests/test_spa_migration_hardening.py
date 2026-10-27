from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import migrate_spa_bookings_updated_at as migrate

LEGACY = (
    "CREATE TABLE spa_bookings (id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL,"
    " booking_date TEXT NOT NULL, booking_time TEXT NOT NULL, customer_name TEXT NOT NULL,"
    " customer_phone TEXT NOT NULL, customer_type TEXT NOT NULL, service_type TEXT NOT NULL,"
    " note TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL)"
)
ROW = "INSERT INTO spa_bookings (store_id,booking_date,booking_time,customer_name,customer_phone,customer_type,service_type,created_at) VALUES ('store_a','2030-01-01','09:00',?, '0912345678','new','body','t')"


class MigrationHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "m.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _seed(self, wal: bool = False, rows: int = 3) -> None:
        conn = sqlite3.connect(self.db)
        if wal:
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(LEGACY)
        for i in range(rows):
            conn.execute(ROW, (f"客{i}",))
        conn.commit()
        conn.close()

    def _names(self) -> list[str]:
        conn = sqlite3.connect(self.db)
        try:
            return [r[0] for r in conn.execute("SELECT customer_name FROM spa_bookings ORDER BY id")]
        finally:
            conn.close()

    def _has_col(self) -> bool:
        conn = sqlite3.connect(self.db)
        try:
            return "updated_at" in {r[1] for r in conn.execute("PRAGMA table_info(spa_bookings)")}
        finally:
            conn.close()

    def _backups(self) -> list[Path]:
        return sorted(self.dir.glob("m.db.bak_*"))

    def test_wal_db_backup_is_consistent_and_includes_uncheckpointed_rows(self) -> None:
        self._seed(wal=True)
        holder = sqlite3.connect(self.db)  # 保持 WAL 連線，確保資料仍在 -wal 檔而非主檔
        holder.execute(ROW, ("WAL新增",))
        holder.commit()
        self.assertTrue((self.dir / "m.db-wal").exists())
        self.assertEqual(migrate.main(["--apply", "--db", str(self.db)]), 0)
        holder.close()
        backup = sqlite3.connect(self.backups_first())
        names = [r[0] for r in backup.execute("SELECT customer_name FROM spa_bookings ORDER BY id")]
        self.assertEqual(backup.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        backup.close()
        self.assertEqual(names, ["客0", "客1", "客2", "WAL新增"])
        self.assertEqual(self._names(), names)

    def backups_first(self) -> Path:
        return self._backups()[0]

    def test_locked_db_fails_closed_without_changes(self) -> None:
        self._seed()
        other = sqlite3.connect(self.db, isolation_level=None)
        other.execute("BEGIN IMMEDIATE")
        with mock.patch.object(migrate, "LOCK_TIMEOUT_SEC", 0.2):
            with self.assertRaises(SystemExit):
                migrate.apply(self.db)
        other.execute("ROLLBACK")
        other.close()
        self.assertFalse(self._has_col())
        self.assertEqual(self._backups(), [])
        self.assertEqual(len(self._names()), 3)

    def test_backup_failure_blocks_alter_and_leaves_no_partial_file(self) -> None:
        self._seed()
        real_connect = sqlite3.connect

        class BadBackup(sqlite3.Connection):
            def backup(self, *a, **kw):
                raise sqlite3.OperationalError("disk I/O")

        def connect(*a, **kw):
            kw.setdefault("factory", BadBackup)
            return real_connect(*a, **kw)

        with mock.patch.object(migrate.sqlite3, "connect", side_effect=connect):
            with self.assertRaises(sqlite3.OperationalError):
                migrate.apply(self.db)
        self.assertFalse(self._has_col())
        self.assertEqual(self._backups(), [])

    def test_backup_verification_failure_blocks_alter_and_removes_backup(self) -> None:
        self._seed()
        with mock.patch.object(migrate, "_verify_snapshot", side_effect=SystemExit("備份驗證失敗")):
            with self.assertRaises(SystemExit):
                migrate.apply(self.db)
        self.assertFalse(self._has_col())
        self.assertEqual(self._backups(), [])

    def test_insufficient_disk_space_blocks_before_any_change(self) -> None:
        self._seed()
        fake = mock.Mock(free=1)
        with mock.patch.object(migrate.shutil, "disk_usage", return_value=fake):
            with self.assertRaises(SystemExit):
                migrate.apply(self.db)
        self.assertFalse(self._has_col())
        self.assertEqual(self._backups(), [])

    def test_existing_backup_target_is_never_overwritten(self) -> None:
        self._seed()
        fixed = migrate.datetime(2030, 1, 1, 0, 0, 0)
        (self.dir / "m.db.bak_20300101_000000").write_text("keep me")
        with mock.patch.object(migrate, "datetime", mock.Mock(now=lambda: fixed)):
            with self.assertRaises(SystemExit):
                migrate.apply(self.db)
        self.assertEqual((self.dir / "m.db.bak_20300101_000000").read_text(), "keep me")
        self.assertFalse(self._has_col())

    def test_alter_failure_rolls_back_and_backup_is_openable(self) -> None:
        self._seed()
        real_connect = sqlite3.connect

        class Boom(sqlite3.Connection):
            def execute(self, sql, *a):
                if sql.startswith("ALTER TABLE"):
                    raise sqlite3.OperationalError("simulated alter failure")
                return super().execute(sql, *a)

        def connect(*a, **kw):
            if kw.get("isolation_level", "x") is None:
                kw["factory"] = Boom
            return real_connect(*a, **kw)

        with mock.patch.object(migrate.sqlite3, "connect", side_effect=connect):
            with self.assertRaises(sqlite3.OperationalError):
                migrate.apply(self.db)
        self.assertFalse(self._has_col())
        self.assertEqual(len(self._names()), 3)
        self.assertEqual(len(self._backups()), 1)
        bk = sqlite3.connect(self._backups()[0])
        self.assertEqual(bk.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(bk.execute("SELECT COUNT(*) FROM spa_bookings").fetchone()[0], 3)
        bk.close()

    def test_apply_preserves_all_rows_and_second_run_changes_nothing(self) -> None:
        self._seed(rows=5)
        migrate.main(["--apply", "--db", str(self.db)])
        names = self._names()
        snapshot = self.db.read_bytes()
        migrate.main(["--apply", "--db", str(self.db)])
        self.assertEqual(self._names(), names)
        self.assertEqual(self.db.read_bytes(), snapshot)
        self.assertEqual(len(self._backups()), 1)

    def test_check_does_not_modify_main_db_file_on_wal_db(self) -> None:
        """唯讀＝主檔位元組不變、無備份檔；WAL 讀取可能留下空的 -wal/-shm（SQLite 正常行為）。"""
        self._seed(wal=True)
        before = self.db.read_bytes()
        self.assertEqual(migrate.main(["--check", "--db", str(self.db)]), 1)
        self.assertEqual(self.db.read_bytes(), before)
        extras = {p.name for p in self.dir.iterdir()} - {"m.db"}
        self.assertLessEqual(extras, {"m.db-wal", "m.db-shm"})
        self.assertFalse(self._has_col())

if __name__ == "__main__":
    unittest.main()
