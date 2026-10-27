from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty

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
        beauty._SPA_SCHEMA_CHECKED.clear()
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
        beauty._SPA_SCHEMA_CHECKED.clear()

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

    def test_legacy_schema_without_updated_at_self_heals(self) -> None:
        """Root cause：正式 DB 缺 updated_at 時，新增預約曾回 HTML 500。"""
        self._make_legacy_schema()
        self._login()
        slots = beauty.get_spa_slots("store_a", date.fromisoformat(self.day))
        for (pax, expected), slot in zip((("1", 1), ("2", 3)), slots):
            r = self._post(pax=pax, booking_time=slot, customer_name=f"客{pax}")
            self.assertEqual(r.status_code, 200, r.data[:200])
            self.assertEqual(r.get_json(), {"status": "success"})
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


if __name__ == "__main__":
    unittest.main()
