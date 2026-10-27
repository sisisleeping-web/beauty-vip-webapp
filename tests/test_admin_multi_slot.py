from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty

SAME_DAY_MSG = "同一天只能預約一個時段"


class AdminMultiSlotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        beauty.DB_PATH = Path(self.tmp.name) / "beauty_vip.db"
        beauty.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        beauty._auth_failures.clear()
        beauty.init_db()
        self.client = beauty.app.test_client()
        self.day = self._open_day()
        self.slots = beauty.get_spa_slots("store_a", date.fromisoformat(self.day))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _open_day(self) -> str:
        for offset in range(4, 12):
            d = date.today() + timedelta(days=offset)
            if len(beauty.get_spa_slots("store_a", d)) >= 3:
                return d.isoformat()
        raise AssertionError("no open day")

    def _login(self, manager: bool = False) -> None:
        with self.client.session_transaction() as s:
            s["manager_authed" if manager else "main_authed"] = True

    def _book(self, slot_idx: int, client=None, path: str = "/api/spa/admin/book", **kw):
        payload = {
            "store_id": "store_a", "booking_date": self.day, "booking_time": self.slots[slot_idx],
            "customer_name": "王小美", "customer_phone": "0912345678", "customer_type": "existing",
            "service_type": "body", "note": "", "pax": "1",
        }
        payload.update(kw)
        return (client or self.client).post(path, data=payload)

    def _cust(self, slot_idx: int, **kw):
        return self._book(slot_idx, path="/api/spa/book", **kw)

    def _active(self) -> int:
        with beauty.app.app_context():
            return beauty.get_db().execute("SELECT COUNT(*) FROM spa_bookings WHERE status!='cancelled'").fetchone()[0]

    def _ids(self) -> list[int]:
        with beauty.app.app_context():
            return [r[0] for r in beauty.get_db().execute("SELECT id FROM spa_bookings ORDER BY id")]

    # 1/2 admin 新增
    def test_admin_same_customer_same_day_different_slot_succeeds(self) -> None:
        self._login()
        self.assertEqual(self._book(0).status_code, 200)
        self.assertEqual(self._book(1).status_code, 200)
        self.assertEqual(self._active(), 2)

    def test_admin_same_customer_same_day_same_slot_rejected(self) -> None:
        self._login(manager=True)
        self.assertEqual(self._book(0).status_code, 200)
        r = self._book(0)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self._active(), 1)

    # 3 容量
    def test_admin_different_customers_capacity_respected_including_pax_two(self) -> None:
        self._login()
        self.assertEqual(self._book(0, customer_name="甲", customer_phone="0911111111").status_code, 200)
        self.assertEqual(self._book(0, customer_name="乙", customer_phone="0922222222").status_code, 200)
        self.assertEqual(self._book(0, customer_name="丙", customer_phone="0933333333").status_code, 400)
        self.assertEqual(self._book(1, customer_name="丁", customer_phone="0944444444").status_code, 200)
        self.assertEqual(self._book(1, customer_name="戊", customer_phone="0955555555", pax="2").status_code, 400)
        self.assertEqual(self._active(), 3)

    def test_admin_multi_slot_does_not_bypass_capacity(self) -> None:
        self._login()
        for n, p in (("甲", "0911111111"), ("乙", "0922222222")):
            self.assertEqual(self._book(1, customer_name=n, customer_phone=p).status_code, 200)
        self.assertEqual(self._book(0).status_code, 200)
        self.assertEqual(self._book(1).status_code, 400)  # 時段滿，同顧客也不能超額

    # 4 顧客端不放寬（含已登入管理員使用顧客端點）
    def test_customer_same_day_second_slot_still_rejected(self) -> None:
        self.assertEqual(self._cust(0).status_code, 200)
        r = self._cust(1)
        self.assertEqual(r.status_code, 400)
        self.assertIn(SAME_DAY_MSG, r.get_json()["error"])
        self.assertEqual(self._active(), 1)

    def _assert_customer_endpoint_keeps_same_day_limit(self) -> None:
        self.assertEqual(self._cust(0).status_code, 200)
        r = self._cust(1)
        self.assertEqual(r.status_code, 400)
        self.assertIn(SAME_DAY_MSG, r.get_json()["error"])
        self.assertEqual(self._active(), 1)

    def test_main_pin_admin_using_customer_endpoint_keeps_same_day_limit(self) -> None:
        self._login(manager=False)
        self._assert_customer_endpoint_keeps_same_day_limit()

    def test_manager_pin_admin_using_customer_endpoint_keeps_same_day_limit(self) -> None:
        self._login(manager=True)
        self._assert_customer_endpoint_keeps_same_day_limit()

    def _assert_flags_never_grant_exception(self) -> None:
        self.assertEqual(self._cust(0).status_code, 200)
        for extra in ({"source": "admin"}, {"is_admin": "1"}, {"admin": "true"}):
            with self.subTest(extra=extra):
                self.assertEqual(self._cust(1, **extra).status_code, 400)
        self.assertEqual(self._active(), 1)

    def test_client_flags_never_grant_exception_when_logged_out(self) -> None:
        self._assert_flags_never_grant_exception()

    def test_client_flags_never_grant_exception_when_logged_in(self) -> None:
        self._login()
        self._assert_flags_never_grant_exception()

    def test_admin_endpoint_requires_server_side_admin_session(self) -> None:
        self.assertEqual(self._book(0).status_code, 401)  # 未登入被鎖定，回 JSON 401
        self.assertEqual(self._active(), 0)
        self.assertEqual(self._cust(0).status_code, 200)
        self.assertEqual(self._book(1, source="admin", is_admin="1").status_code, 401)
        self.assertEqual(self._active(), 1)

    def test_admin_logout_restores_restrictions(self) -> None:
        self._login()
        self.assertEqual(self._book(0).status_code, 200)
        self.client.get("/main_logout")
        self.assertEqual(self._cust(1).status_code, 400)
        self.assertEqual(self._book(1).status_code, 401)
        self.assertEqual(self._active(), 1)

    def test_ui_wiring_admin_form_uses_admin_endpoint_customer_page_does_not(self) -> None:
        self._login()
        admin_html = self.client.get("/spa/admin/bookings").get_data(as_text=True)
        self.assertIn("fetch('/api/spa/admin/book'", admin_html)
        self.assertNotIn("fetch('/api/spa/book'", admin_html)
        customer_html = self.client.get("/spa/booking").get_data(as_text=True)
        self.assertIn("fetch('/api/spa/book'", customer_html)
        self.assertNotIn("/api/spa/admin/book", customer_html)

    # 5 改期
    def test_admin_move_onto_own_occupied_slot_rejected(self) -> None:
        self._login()
        self._book(0)
        self._book(1)
        a, b = self._ids()
        r = self.client.post(f"/api/spa/bookings/{b}/update_time", data={"booking_date": self.day, "booking_time": self.slots[0]})
        self.assertEqual(r.status_code, 409)
        with beauty.app.app_context():
            self.assertEqual(beauty.get_db().execute("SELECT booking_time FROM spa_bookings WHERE id=?", (b,)).fetchone()[0], self.slots[1])

    def test_admin_move_to_free_different_slot_succeeds(self) -> None:
        self._login()
        self._book(0)
        self._book(1)
        _, b = self._ids()
        r = self.client.post(f"/api/spa/bookings/{b}/update_time", data={"booking_date": self.day, "booking_time": self.slots[2]})
        self.assertEqual(r.status_code, 200)

    def test_admin_move_cannot_overbook_multi_slot_customer(self) -> None:
        self._login()
        for n, p in (("甲", "0911111111"), ("乙", "0922222222")):
            self._book(2, customer_name=n, customer_phone=p)
        self._book(0)
        self._book(1)
        target = self._ids()[-1]
        r = self.client.post(f"/api/spa/bookings/{target}/update_time", data={"booking_date": self.day, "booking_time": self.slots[2]})
        self.assertEqual(r.status_code, 409)

    # 6 取消
    def test_cancelled_booking_does_not_block_same_slot_rebooking(self) -> None:
        self._login()
        self._book(0)
        first = self._ids()[0]
        self.assertEqual(self.client.post(f"/api/spa/bookings/{first}/delete").status_code, 200)
        self.assertEqual(self._book(0).status_code, 200)
        self.assertEqual(self._active(), 1)

    def test_cancelled_booking_not_counted_as_move_conflict(self) -> None:
        self._login()
        self._book(0)
        self._book(1)
        a, b = self._ids()
        self.client.post(f"/api/spa/bookings/{a}/delete")
        r = self.client.post(f"/api/spa/bookings/{b}/update_time", data={"booking_date": self.day, "booking_time": self.slots[0]})
        self.assertEqual(r.status_code, 200)

    # 7 權限
    def test_unauthenticated_move_is_blocked(self) -> None:
        self._login()
        self._book(0)
        self.client.get("/main_logout")
        self.client.post("/api/spa/bookings/1/update_time", data={"booking_date": self.day, "booking_time": self.slots[1]})
        with beauty.app.app_context():
            self.assertEqual(beauty.get_db().execute("SELECT booking_time FROM spa_bookings WHERE id=1").fetchone()[0], self.slots[0])

    # 8 併發
    def test_concurrent_same_customer_same_slot_only_one_succeeds(self) -> None:
        statuses: list[int] = []
        barrier = threading.Barrier(2)

        def go() -> None:
            c = beauty.app.test_client()
            with c.session_transaction() as s:
                s["main_authed"] = True
            barrier.wait()
            statuses.append(self._book(0, client=c).status_code)

        ts = [threading.Thread(target=go) for _ in range(2)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(sorted(statuses), [200, 400])
        self.assertEqual(self._active(), 1)


if __name__ == "__main__":
    unittest.main()
