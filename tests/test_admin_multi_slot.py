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

    def _book(self, slot_idx: int, client=None, **kw):
        payload = {
            "store_id": "store_a", "booking_date": self.day, "booking_time": self.slots[slot_idx],
            "customer_name": "王小美", "customer_phone": "0912345678", "customer_type": "existing",
            "service_type": "body", "note": "", "pax": "1",
        }
        payload.update(kw)
        return (client or self.client).post("/api/spa/book", data=payload)

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

    # 4 顧客端不放寬
    def test_customer_same_day_second_slot_still_rejected(self) -> None:
        self.assertEqual(self._book(0).status_code, 200)
        r = self._book(1)
        self.assertEqual(r.status_code, 400)
        self.assertIn(SAME_DAY_MSG, r.get_json()["error"])
        self.assertEqual(self._active(), 1)

    def test_customer_cannot_spoof_admin_via_params(self) -> None:
        self.assertEqual(self._book(0).status_code, 200)
        for extra in ({"source": "admin"}, {"is_admin": "1"}, {"admin": "true"}):
            self.assertEqual(self._book(1, **extra).status_code, 400, extra)
        self.assertEqual(self._active(), 1)

    def test_logged_out_after_admin_session_is_restricted_again(self) -> None:
        self._login()
        self.assertEqual(self._book(0).status_code, 200)
        self.client.get("/main_logout")
        self.assertEqual(self._book(1).status_code, 400)

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
    def test_unauthenticated_move_and_book_gain_no_admin_exception(self) -> None:
        self._login()
        self._book(0)
        self.client.get("/main_logout")
        self.assertEqual(self._book(1).status_code, 400)
        self.assertEqual(self.client.post(f"/api/spa/bookings/{self._ids()[0]}/update_time", data={"booking_date": self.day, "booking_time": self.slots[1]}).status_code, 200)  # 鎖定頁
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
