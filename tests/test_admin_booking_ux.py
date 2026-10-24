from __future__ import annotations

import re
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty

STORES = ("store_a", "store_b")
ADMIN_PAGES = ("/", "/actions", "/entry", "/report", "/contacts", "/review", "/upgrades", "/spa/admin/bookings")


class AdminBookingUxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        beauty.DB_PATH = Path(self.tmp.name) / "beauty_vip.db"
        beauty.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        beauty._auth_failures.clear()
        beauty.init_db()
        self.client = beauty.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _login(self, manager: bool = False) -> None:
        with self.client.session_transaction() as session:
            session["manager_authed" if manager else "main_authed"] = True

    def _open_day(self, store_id: str = "store_a") -> str:
        for offset in range(3, 12):
            day = date.today() + timedelta(days=offset)
            if beauty.get_spa_slots(store_id, day):
                return day.isoformat()
        raise AssertionError("no open day found")

    def _payload(self, **overrides: str) -> dict[str, str]:
        day = self._open_day()
        payload = {
            "store_id": "store_a",
            "booking_date": day,
            "booking_time": beauty.get_spa_slots("store_a", date.fromisoformat(day))[0],
            "customer_name": "測試顧客",
            "customer_phone": "0912345678",
            "customer_type": "new",
            "service_type": "facial",
            "note": "",
            "pax": "1",
        }
        payload.update(overrides)
        return payload

    def _row_count(self) -> int:
        with beauty.app.app_context():
            return beauty.get_db().execute("SELECT COUNT(*) FROM spa_bookings").fetchone()[0]

    # --- 時段：單一權威 ---
    def test_slots_endpoint_matches_backend_policy_for_every_weekday(self) -> None:
        self._login()
        start = date.today() + timedelta(days=3)
        for store_id in STORES:
            for offset in range(7):
                day = start + timedelta(days=offset)
                response = self.client.get(f"/api/spa/slots?store_id={store_id}&date={day.isoformat()}")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(tuple(response.get_json()["slots"]), beauty.get_spa_slots(store_id, day))

    def test_slots_endpoint_rejects_bad_input_and_requires_auth(self) -> None:
        locked = self.client.get("/api/spa/slots?store_id=store_a&date=2030-01-01")
        self.assertFalse(locked.is_json)
        self.assertNotIn("slots", locked.get_data(as_text=True))
        self._login()
        for query in ("", "store_id=store_a", "store_id=nope&date=2030-01-01", "store_id=store_a&date=bad"):
            self.assertEqual(self.client.get(f"/api/spa/slots?{query}").status_code, 400, query)

    def test_admin_add_form_uses_slot_select_not_free_time_and_no_all_day(self) -> None:
        self._login()
        html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        form = html[html.index('id="addBookingForm"'):html.index("</form>", html.index('id="addBookingForm"'))]
        self.assertRegex(form, r'<select[^>]*name="booking_time"')
        self.assertNotIn('type="time"', form)
        self.assertNotIn("全時段", form)

    def test_admin_non_slot_time_is_rejected_even_for_admin(self) -> None:
        self._login(manager=True)
        for bad in ("10:00", "00:00", "全時段", ""):
            self.assertEqual(self.client.post("/api/spa/book", data=self._payload(booking_time=bad)).status_code, 400, bad)
        self.assertEqual(self._row_count(), 0)

    # --- 人數 ---
    def test_admin_add_form_has_pax_select_one_or_two_default_one(self) -> None:
        self._login()
        html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        form = html[html.index('id="addBookingForm"'):html.index("</form>", html.index('id="addBookingForm"'))]
        match = re.search(r'<select[^>]*name="pax"[^>]*>(.*?)</select>', form, re.S)
        self.assertIsNotNone(match)
        options = re.findall(r'<option value="([^"]*)"([^>]*)>(.*?)</option>', match.group(1), re.S)
        self.assertEqual([o[0] for o in options], ["1", "2"])
        self.assertEqual([o[2].strip() for o in options], ["1 人", "2 人"])
        self.assertIn("selected", options[0][1])
        self.assertNotIn("selected", options[1][1])

    def test_pax_one_and_two_are_saved_as_that_many_rows(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/book", data=self._payload(pax="1")).status_code, 200)
        self.assertEqual(self._row_count(), 1)
        other = self._payload(pax="2", customer_name="雙人", customer_phone="0922222222", booking_time=beauty.get_spa_slots("store_a", date.fromisoformat(self._open_day()))[1])
        self.assertEqual(self.client.post("/api/spa/book", data=other).status_code, 200)
        self.assertEqual(self._row_count(), 3)

    def test_pax_absent_defaults_to_one(self) -> None:
        self._login()
        payload = self._payload()
        del payload["pax"]
        self.assertEqual(self.client.post("/api/spa/book", data=payload).status_code, 200)
        self.assertEqual(self._row_count(), 1)

    def test_admin_list_hides_cancelled_by_default_but_keeps_it_filterable(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/admin/book", data=self._payload(customer_name="進行中顧客")).status_code, 200)
        self.assertEqual(self.client.post("/api/spa/admin/book", data=self._payload(
            customer_name="已取消顧客",
            customer_phone="0922222222",
            booking_time=beauty.get_spa_slots("store_a", date.fromisoformat(self._open_day()))[1],
        )).status_code, 200)
        with beauty.app.app_context():
            cancelled_id = beauty.get_db().execute(
                "SELECT id FROM spa_bookings WHERE customer_name='已取消顧客'"
            ).fetchone()[0]
        self.assertEqual(self.client.post(f"/api/spa/bookings/{cancelled_id}/delete").status_code, 200)

        default_html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        cancelled_html = self.client.get("/spa/admin/bookings?status=cancelled").get_data(as_text=True)
        all_html = self.client.get("/spa/admin/bookings?status=all").get_data(as_text=True)
        self.assertIn("進行中顧客", default_html)
        self.assertNotIn("已取消顧客", default_html)
        self.assertIn("已取消顧客", cancelled_html)
        self.assertIn("已取消顧客", all_html)

    def test_booking_audit_timestamps_are_recorded_and_displayed(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/admin/book", data=self._payload()).status_code, 200)
        with beauty.app.app_context():
            db = beauty.get_db()
            booking = db.execute("SELECT id, created_at, updated_at FROM spa_bookings").fetchone()
            self.assertTrue(booking["created_at"])
            self.assertEqual(booking["updated_at"], booking["created_at"])
            db.execute("UPDATE spa_bookings SET created_at='2000-01-01T00:00:00' WHERE id=?", (booking["id"],))
            db.commit()

        self.assertEqual(self.client.post(f"/api/spa/bookings/{booking['id']}/confirm").status_code, 200)
        with beauty.app.app_context():
            updated = beauty.get_db().execute(
                "SELECT created_at, updated_at FROM spa_bookings WHERE id=?", (booking["id"],)
            ).fetchone()
        self.assertEqual(updated["created_at"], "2000-01-01T00:00:00")
        self.assertNotEqual(updated["updated_at"], updated["created_at"])

        html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        self.assertIn("建立／最後修改", html)
        self.assertIn("建：2000-01-01T00:00:00", html)
        self.assertIn("改：", html)

    def test_booking_list_uses_compact_columns_and_keeps_notes_available(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/admin/book", data=self._payload(note="請預留停車位")).status_code, 200)

        html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        self.assertIn("預約／時段", html)
        self.assertIn("服務／客群", html)
        self.assertIn('class="booking-table"', html)
        self.assertIn('data-label="建立／最後修改"', html)
        self.assertIn("查看備註", html)
        self.assertIn("請預留停車位", html)

    def test_invalid_pax_is_rejected_without_writing(self) -> None:
        self._login(manager=True)
        for bad in ("3", "0", "-1", "abc", "", "1.5", "99"):
            response = self.client.post("/api/spa/book", data=self._payload(pax=bad))
            self.assertEqual(response.status_code, 400, bad)
        self.assertEqual(self._row_count(), 0)

    def test_pax_two_rejected_when_only_one_seat_left(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/book", data=self._payload(customer_name="甲", customer_phone="0911111111")).status_code, 200)
        response = self.client.post("/api/spa/book", data=self._payload(pax="2", customer_name="乙", customer_phone="0922222222"))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self._row_count(), 1)

    def test_pax_two_allowed_when_two_seats_free_and_blocked_after(self) -> None:
        self._login()
        self.assertEqual(self.client.post("/api/spa/book", data=self._payload(pax="2")).status_code, 200)
        again = self.client.post("/api/spa/book", data=self._payload(customer_name="丙", customer_phone="0933333333"))
        self.assertEqual(again.status_code, 400)
        self.assertEqual(self._row_count(), 2)

    def test_concurrent_pax_two_requests_cannot_overbook(self) -> None:
        self._login()
        statuses: list[int] = []
        barrier = threading.Barrier(2)

        def submit(name: str, phone: str) -> None:
            client = beauty.app.test_client()
            with client.session_transaction() as session:
                session["main_authed"] = True
            barrier.wait()
            statuses.append(client.post("/api/spa/book", data=self._payload(pax="2", customer_name=name, customer_phone=phone)).status_code)

        threads = [threading.Thread(target=submit, args=(f"併發{i}", f"09000000{i}0")) for i in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(statuses), [200, 400])
        self.assertEqual(self._row_count(), 2)

    # --- 顧客端不退化 ---
    def test_customer_page_still_offers_pax_one_two(self) -> None:
        html = self.client.get("/spa/booking").get_data(as_text=True)
        self.assertIn('<option value="1">1位</option>', html)
        self.assertIn('<option value="2">2位</option>', html)

    # --- 版本 ---
    def test_every_page_shows_the_single_authoritative_version(self) -> None:
        self._login(manager=True)
        for path in ADMIN_PAGES:
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, path)
            badges = re.findall(r'<span class="badge">(v[^<]*)</span>', response.get_data(as_text=True))
            self.assertEqual(badges, [f"v{beauty.APP_VERSION}"], path)

    def test_customer_booking_page_has_no_stale_version_label(self) -> None:
        html = self.client.get("/spa/booking").get_data(as_text=True)
        self.assertNotIn('class="badge"', html)
        self.assertNotIn("v1.1", html)


if __name__ == "__main__":
    unittest.main()
