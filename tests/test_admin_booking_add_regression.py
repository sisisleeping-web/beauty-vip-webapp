from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty
from scripts import migrate_spa_bookings_updated_at as migrate

LEGACY_SPA_BOOKINGS = """
CREATE TABLE spa_bookings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, booking_date TEXT NOT NULL,
    booking_time TEXT NOT NULL, customer_name TEXT NOT NULL, customer_phone TEXT NOT NULL,
    customer_type TEXT NOT NULL, service_type TEXT NOT NULL, note TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
)
"""
URL = "/api/spa/admin/book"


class AdminBookingAddRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        beauty.DB_PATH = Path(self.tmp.name) / "beauty_vip.db"
        beauty.app.config.update(TESTING=False, PROPAGATE_EXCEPTIONS=False, SESSION_COOKIE_SECURE=False)
        beauty._auth_failures.clear()
        beauty.init_db()
        self.client = beauty.app.test_client()
        self.day = self._open_day()
        self.slot = beauty.get_spa_slots("store_a", date.fromisoformat(self.day))[0]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _login(self) -> None:
        with self.client.session_transaction() as s:
            s["main_authed"] = True

    @staticmethod
    def _open_day() -> str:
        for offset in range(3, 12):
            d = date.today() + timedelta(days=offset)
            if beauty.get_spa_slots("store_a", d):
                return d.isoformat()
        raise AssertionError("no open day")

    def _post(self, **kw: str):
        data = {
            "store_id": "store_a", "booking_date": self.day, "booking_time": self.slot,
            "customer_name": "測試顧客", "customer_phone": "0912345678", "customer_type": "new",
            "service_type": "facial", "note": "", "pax": "1",
        }
        data.update(kw)
        return self.client.post(URL, data=data)

    def _count(self) -> int:
        conn = sqlite3.connect(beauty.DB_PATH)
        try:
            return conn.execute("SELECT COUNT(*) FROM spa_bookings").fetchone()[0]
        finally:
            conn.close()

    def _make_legacy_schema(self) -> None:
        conn = sqlite3.connect(beauty.DB_PATH)
        conn.executescript("ALTER TABLE spa_bookings RENAME TO spa_bookings_bak;" + LEGACY_SPA_BOOKINGS)
        conn.commit()
        conn.close()

    def test_one_pax_creates_one_row(self) -> None:
        self._login()
        r = self._post(pax="1")
        self.assertEqual((r.status_code, r.get_json()), (200, {"status": "success"}))
        self.assertEqual(self._count(), 1)

    def test_two_pax_creates_two_rows(self) -> None:
        self._login()
        self.assertEqual(self._post(pax="2").status_code, 200)
        self.assertEqual(self._count(), 2)

    def test_full_slot_rejected_with_json_error(self) -> None:
        self._login()
        self.assertEqual(self._post(customer_name="甲", pax="2").status_code, 200)
        r = self._post(customer_name="乙", pax="1")
        self.assertEqual(r.status_code, 400)
        self.assertIn("error", r.get_json())
        self.assertEqual(self._count(), 2)

    def test_duplicate_same_slot_rejected(self) -> None:
        self._login()
        self.assertEqual(self._post().status_code, 200)
        r = self._post()
        self.assertEqual(r.status_code, 400)
        self.assertIn("已有預約", r.get_json()["error"])
        self.assertEqual(self._count(), 1)

    def test_no_permission_returns_json_401_and_writes_nothing(self) -> None:
        r = self._post()
        self.assertEqual(r.status_code, 401)
        self.assertTrue(r.is_json)
        self.assertIn("error", r.get_json())
        self.assertEqual(self._count(), 0)

    def test_authed_but_not_manager_route_still_checks_role(self) -> None:
        """通過 PIN 鎖後，後台端點自身的權限檢查仍在（防止只靠全域鎖）。"""
        with self.client.session_transaction() as s:
            s["main_authed"] = False
            s["manager_authed"] = False
        self.assertIn(self._post().status_code, (401, 403))
        self.assertEqual(self._count(), 0)

    def test_legacy_schema_returns_json_500_and_app_does_not_alter_schema(self) -> None:
        """Root cause 情境：缺 updated_at 時不得自動改 schema，且必須回 JSON 500 而非 HTML。"""
        self._make_legacy_schema()
        self._login()
        r = self._post()
        self.assertEqual(r.status_code, 500)
        self.assertTrue(r.is_json)
        self.assertIn("error", r.get_json())
        conn = sqlite3.connect(beauty.DB_PATH)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(spa_bookings)")}
        conn.close()
        self.assertNotIn("updated_at", cols)
        self.assertEqual(self._count(), 0)

    def test_after_migration_booking_succeeds(self) -> None:
        self._make_legacy_schema()
        self.assertEqual(migrate.main(["--apply", "--db", str(beauty.DB_PATH)]), 0)
        self._login()
        slots = beauty.get_spa_slots("store_a", date.fromisoformat(self.day))
        for (pax, expected), slot in zip((("1", 1), ("2", 3)), slots):
            r = self._post(pax=pax, booking_time=slot, customer_name=f"客{pax}")
            self.assertEqual(r.status_code, 200, r.data[:200])
            self.assertEqual(self._count(), expected)

    def test_unexpected_error_returns_json_500_not_html(self) -> None:
        self._login()
        original = beauty._process_spa_booking

        def boom(**_kw):
            raise RuntimeError("simulated")

        beauty._process_spa_booking = boom
        try:
            r = self._post()
        finally:
            beauty._process_spa_booking = original
        self.assertEqual(r.status_code, 500)
        self.assertTrue(r.is_json)
        self.assertIn("error", r.get_json())
        self.assertNotIn("simulated", r.get_json()["error"])

    def test_frontend_checks_http_status_and_non_json(self) -> None:
        html = (Path(__file__).parent.parent / "templates" / "spa_admin.html").read_text(encoding="utf-8")
        body = html[html.index("function submitAddBooking"):]
        body = body[:body.index("</script>")]
        self.assertIn("res.ok", body)
        self.assertIn("data.status === 'success'", body)
        self.assertNotIn("|| !data.error", body)


class MigrationScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "m.db"
        conn = sqlite3.connect(self.db)
        conn.execute(LEGACY_SPA_BOOKINGS)
        conn.execute(
            "INSERT INTO spa_bookings (store_id, booking_date, booking_time, customer_name, customer_phone,"
            " customer_type, service_type, created_at) VALUES ('store_a','2030-01-01','09:00','甲','0912345678','new','body','t')"
        )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _cols(self) -> set[str]:
        conn = sqlite3.connect(self.db)
        try:
            return {r[1] for r in conn.execute("PRAGMA table_info(spa_bookings)")}
        finally:
            conn.close()

    def _backups(self) -> list[Path]:
        return list(Path(self.tmp.name).glob("m.db.bak_*"))

    def test_check_is_read_only_and_exit_1_when_missing(self) -> None:
        before = self.db.read_bytes()
        self.assertEqual(migrate.main(["--check", "--db", str(self.db)]), 1)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(self._backups(), [])

    def test_requires_explicit_mode(self) -> None:
        with self.assertRaises(SystemExit):
            migrate.main(["--db", str(self.db)])

    def test_apply_backs_up_adds_column_preserves_rows(self) -> None:
        self.assertEqual(migrate.main(["--apply", "--db", str(self.db)]), 0)
        self.assertIn("updated_at", self._cols())
        self.assertEqual(len(self._backups()), 1)
        backup_cols = {r[1] for r in sqlite3.connect(self._backups()[0]).execute("PRAGMA table_info(spa_bookings)")}
        self.assertNotIn("updated_at", backup_cols)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT customer_name, updated_at FROM spa_bookings").fetchall(), [("甲", None)])
        conn.close()
        self.assertEqual(migrate.main(["--check", "--db", str(self.db)]), 0)

    def test_apply_twice_is_noop(self) -> None:
        migrate.main(["--apply", "--db", str(self.db)])
        after_first = self.db.read_bytes()
        self.assertEqual(migrate.main(["--apply", "--db", str(self.db)]), 0)
        self.assertEqual(len(self._backups()), 1)
        self.assertEqual(self.db.read_bytes(), after_first)

    def test_missing_db_or_table_fails_fast(self) -> None:
        with self.assertRaises(SystemExit):
            migrate.main(["--check", "--db", str(Path(self.tmp.name) / "nope.db")])
        empty = Path(self.tmp.name) / "e.db"
        sqlite3.connect(empty).close()
        with self.assertRaises(SystemExit):
            migrate.main(["--apply", "--db", str(empty)])


if __name__ == "__main__":
    unittest.main()
