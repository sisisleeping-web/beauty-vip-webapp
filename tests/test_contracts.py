from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty


class BeautyVipContractTests(unittest.TestCase):
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

    def _future_day(self, offset: int = 3) -> str:
        return (date.today() + timedelta(days=offset)).isoformat()

    def _booking_payload(self, **overrides: str) -> dict[str, str]:
        payload = {
            "store_id": "store_a",
            "booking_date": self._future_day(),
            "booking_time": "09:00",
            "customer_name": "測試顧客",
            "customer_phone": "0912345678",
            "customer_type": "new",
            "service_type": "facial",
            "note": "",
            "pax": "1",
        }
        payload.update(overrides)
        return payload

    def test_security_headers_and_cross_site_rejection(self) -> None:
        response = self.client.get("/spa/booking")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])

        rejected = self.client.post(
            "/api/spa/book",
            data=self._booking_payload(),
            headers={"Origin": "https://attacker.example"},
        )
        self.assertEqual(rejected.status_code, 403)
        allowed = self.client.post(
            "/api/spa/book",
            data=self._booking_payload(),
            headers={"Origin": "https://localhost"},
        )
        self.assertEqual(allowed.status_code, 200)

    def test_pin_login_is_rate_limited(self) -> None:
        for _ in range(beauty.AUTH_MAX_FAILURES):
            response = self.client.post("/main_unlock", data={"pin": "wrong"})
            self.assertEqual(response.status_code, 200)
        limited = self.client.post("/main_unlock", data={"pin": beauty.MAIN_PIN})
        self.assertEqual(limited.status_code, 429)

    def test_customer_delete_requires_manager_and_cleans_ledgers(self) -> None:
        self._login()
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('甲','1990-01-01','2026-01-01')"
            ).lastrowid
            adjustment_id = db.execute(
                "INSERT INTO point_adjustments(customer_id,points,reason,operator,created_at) VALUES(?,10,'test','staff','2026-01-01')",
                (customer_id,),
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,source_adjustment_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,?,10,10,'2026-01-01','2026-07-01','active',0,'2026-01-01')",
                (customer_id, adjustment_id),
            )
            db.commit()

        forbidden = self.client.post(f"/api/customers/{customer_id}/delete")
        self.assertEqual(forbidden.status_code, 403)
        self._login(manager=True)
        blocked = self.client.post(
            f"/api/customers/{customer_id}/delete", data={"reason": "誤建帳號", "confirmed": "1"}
        )
        self.assertEqual(blocked.status_code, 409)
        with beauty.app.app_context():
            db = beauty.get_db()
            self.assertEqual(db.execute("SELECT COUNT(*) FROM customers").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM point_adjustments").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM coin_batches").fetchone()[0], 1)
            empty_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('空白','1991-01-01','2026-01-01')"
            ).lastrowid
            db.commit()
        missing_confirmation = self.client.post(f"/api/customers/{empty_id}/delete")
        self.assertEqual(missing_confirmation.status_code, 400)
        deleted = self.client.post(
            f"/api/customers/{empty_id}/delete", data={"reason": "重複空白建檔", "confirmed": "1"}
        )
        self.assertEqual(deleted.status_code, 302)
        with beauty.app.app_context():
            operation = beauty.get_db().execute(
                "SELECT action,reason FROM customer_operation_log WHERE customer_id=?", (empty_id,)
            ).fetchone()
            self.assertEqual(dict(operation), {"action": "delete_empty_customer", "reason": "重複空白建檔"})

    def test_customer_merge_keeps_highest_tier_and_all_customer_ledgers(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            keep_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,member_tier,tier_effective_date,tier_expires_date) "
                "VALUES('合併測試','1988-01-01','2026-01-01','P級美咖','2026-08-10','2027-08-10')"
            ).lastrowid
            absorb_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,member_tier,tier_effective_date,tier_expires_date) "
                "VALUES('合併測試','1988-01-02','2026-01-01','A級美咖','2026-04-12','2027-04-12')"
            ).lastrowid
            txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a','2026-04-11','2026-04',60000,60000,0,'2026-04-11','normal')",
                (absorb_id,),
            ).lastrowid
            db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,trigger_txn_id,created_at) "
                "VALUES(?, '2026-04-11','P級美咖','A級美咖',?,'2026-04-11')",
                (absorb_id, txn_id),
            )
            db.execute(
                "INSERT INTO point_adjustments(customer_id,points,reason,operator,created_at) VALUES(?,100,'test','staff','2026-04-11')",
                (absorb_id,),
            )
            db.commit()

        preview = self.client.get(f"/api/customers/merge/preview?keep_id={keep_id}&absorb_id={absorb_id}")
        self.assertEqual(preview.status_code, 200)
        response = self.client.post("/api/customers/merge", data={"keep_id": keep_id, "absorb_id": absorb_id, "reason": "生日輸入錯誤", "confirmed": "1"})
        self.assertEqual(response.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            customer = db.execute(
                "SELECT member_tier,tier_effective_date,tier_expires_date FROM customers WHERE id=?", (keep_id,)
            ).fetchone()
            self.assertEqual(dict(customer), {
                "member_tier": "A級美咖", "tier_effective_date": "2026-04-12", "tier_expires_date": "2027-04-12",
            })
            self.assertEqual(db.execute("SELECT merged_into_customer_id FROM customers WHERE id=?", (absorb_id,)).fetchone()[0], keep_id)
            self.assertEqual(db.execute("SELECT customer_id FROM transactions WHERE id=?", (txn_id,)).fetchone()[0], keep_id)
            self.assertEqual(db.execute("SELECT customer_id FROM tier_upgrades WHERE trigger_txn_id=?", (txn_id,)).fetchone()[0], keep_id)
            self.assertEqual(db.execute("SELECT customer_id FROM point_adjustments").fetchone()[0], keep_id)

    def test_customer_merge_rebuilds_tier_from_combined_current_year_spend(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            keep_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('累積測試','1988-01-01','2026-01-01')"
            ).lastrowid
            absorb_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('累積測試','1988-01-02','2026-01-01')"
            ).lastrowid
            for customer_id, txn_date, amount in [
                (keep_id, '2026-04-01', 20000), (keep_id, '2026-04-05', 20000),
                (absorb_id, '2026-04-11', 20000),
            ]:
                db.execute(
                    "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,cashback,created_at,entry_mode) "
                    "VALUES(?, 'store_a', ?, '2026-04', ?, ?, 0, ?, 'normal')",
                    (customer_id, txn_date, amount, amount, txn_date),
                )
            db.commit()

        response = self.client.post("/api/customers/merge", data={"keep_id": keep_id, "absorb_id": absorb_id, "reason": "重複建檔", "confirmed": "1"})
        self.assertEqual(response.status_code, 302)
        with beauty.app.app_context():
            row = beauty.get_db().execute(
                "SELECT member_tier,tier_effective_date,tier_expires_date FROM customers WHERE id=?", (keep_id,)
            ).fetchone()
            self.assertEqual(dict(row), {
                "member_tier": "A級美咖", "tier_effective_date": "2026-04-12", "tier_expires_date": "2027-04-12",
            })

    def test_void_recalculation_keeps_tier_when_calendar_year_threshold_remains(self) -> None:
        """作廢重複單後，仍要用整個曆年累計判斷，不可只看會員效期後的消費。"""
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,member_tier,tier_effective_date,tier_expires_date) "
                "VALUES('作廢稽核','1990-01-01','2026-01-01','P級美咖','2026-07-05','2027-07-05')"
            ).lastrowid
            for txn_date, amount in [
                ('2026-01-10', 6000), ('2026-02-28', 1899), ('2026-04-11', 6750),
                ('2026-06-20', 4999), ('2026-07-04', 8700), ('2026-08-01', 3500),
            ]:
                db.execute(
                    "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,cashback,created_at,entry_mode) "
                    "VALUES(?, 'store_a', ?, substr(?,1,7), ?, ?, 0, ?, 'normal')",
                    (customer_id, txn_date, txn_date, amount, amount, txn_date),
                )
            void_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,cashback,created_at,entry_mode,voided_at) "
                "VALUES(?, 'store_a','2026-08-01','2026-08',3500,3500,0,'2026-08-01','normal','2026-08-10T00:00:00')",
                (customer_id,),
            ).lastrowid
            beauty.reevaluate_tier_after_void(
                db, customer_id, beauty.load_rules(), date(2026, 8, 10), void_id, '重複交易'
            )
            row = db.execute("SELECT member_tier FROM customers WHERE id=?", (customer_id,)).fetchone()
            self.assertEqual(row['member_tier'], 'P級美咖')

    def test_merge_preview_and_undo_restore_every_moved_ledger(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            keep_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('還原測試','1988-01-01','2026-01-01')"
            ).lastrowid
            absorb_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at,member_tier,tier_effective_date,tier_expires_date) "
                "VALUES('還原測試','1988-01-02','2026-01-01','A級美咖','2026-04-12','2027-04-12')"
            ).lastrowid
            txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a','2026-04-11','2026-04',60000,60000,0,'2026-04-11','normal')",
                (absorb_id,),
            ).lastrowid
            adjustment_id = db.execute(
                "INSERT INTO point_adjustments(customer_id,points,reason,operator,created_at) VALUES(?,100,'test','staff','2026-04-11')",
                (absorb_id,),
            ).lastrowid
            db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,created_at) "
                "VALUES(?, '2026-04-12','P級美咖','A級美咖','2026-04-12')",
                (absorb_id,),
            )
            db.commit()

        preview = self.client.get(f"/api/customers/merge/preview?keep_id={keep_id}&absorb_id={absorb_id}")
        self.assertEqual(preview.status_code, 200)
        self.assertIn("A級美咖".encode(), preview.data)
        rejected = self.client.post("/api/customers/merge", data={"keep_id": keep_id, "absorb_id": absorb_id})
        self.assertEqual(rejected.status_code, 400)
        self.assertEqual(
            self.client.post(
                "/api/customers/merge",
                data={"keep_id": keep_id, "absorb_id": absorb_id, "reason": "生日誤植", "confirmed": "1"},
            ).status_code,
            302,
        )
        with beauty.app.app_context():
            db = beauty.get_db()
            op_id = db.execute("SELECT id FROM customer_merge_operations").fetchone()[0]
            self.assertEqual(db.execute("SELECT customer_id FROM transactions WHERE id=?", (txn_id,)).fetchone()[0], keep_id)
        restored = self.client.post(
            f"/api/customer-merges/{op_id}/undo", data={"reason": "確認誤合併", "confirmed": "1"}
        )
        self.assertEqual(restored.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            source = db.execute("SELECT merged_into_customer_id,member_tier FROM customers WHERE id=?", (absorb_id,)).fetchone()
            self.assertEqual(dict(source), {"merged_into_customer_id": None, "member_tier": "A級美咖"})
            self.assertEqual(db.execute("SELECT customer_id FROM transactions WHERE id=?", (txn_id,)).fetchone()[0], absorb_id)
            self.assertEqual(db.execute("SELECT customer_id FROM point_adjustments WHERE id=?", (adjustment_id,)).fetchone()[0], absorb_id)
            self.assertEqual(db.execute("SELECT status FROM customer_merge_operations WHERE id=?", (op_id,)).fetchone()[0], "undone")

    def test_birthday_change_and_duplicate_candidates_are_guarded_and_audited(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,phone,birthday,created_at) VALUES('重複候選','0912000000','2000-01-01','2026-01-01')"
            ).lastrowid
            db.commit()
        candidates = self.client.get("/api/customers/duplicate-candidates?name=重複候選&phone=0912000000")
        self.assertEqual(candidates.status_code, 200)
        self.assertEqual(candidates.json["customers"][0]["id"], customer_id)
        missing_confirmation = self.client.post(
            f"/review/update_birthday/{customer_id}", data={"birthday": "1990-02-02"}
        )
        self.assertEqual(missing_confirmation.status_code, 400)
        changed = self.client.post(
            f"/review/update_birthday/{customer_id}",
            data={"birthday": "1990-02-02", "reason": "更正證件生日", "confirmed": "1"},
        )
        self.assertEqual(changed.status_code, 302)
        with beauty.app.app_context():
            row = beauty.get_db().execute(
                "SELECT action,reason FROM customer_operation_log WHERE customer_id=? ORDER BY id DESC", (customer_id,)
            ).fetchone()
            self.assertEqual(dict(row), {"action": "update_birthday", "reason": "更正證件生日"})

    def test_booking_validates_input_and_capacity(self) -> None:
        invalid = self.client.post("/api/spa/book", data=self._booking_payload(store_id="unknown"))
        self.assertEqual(invalid.status_code, 400)
        invalid_phone = self.client.post(
            "/api/spa/book", data=self._booking_payload(customer_phone="<script>")
        )
        self.assertEqual(invalid_phone.status_code, 400)

        first = self.client.post("/api/spa/book", data=self._booking_payload(customer_name="甲", customer_phone="0911111111"))
        second = self.client.post("/api/spa/book", data=self._booking_payload(customer_name="乙", customer_phone="0922222222"))
        full = self.client.post("/api/spa/book", data=self._booking_payload(customer_name="丙", customer_phone="0933333333"))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(full.status_code, 400)

    def test_admin_move_cannot_overbook(self) -> None:
        for name, phone in [("甲", "0911111111"), ("乙", "0922222222")]:
            self.assertEqual(
                self.client.post("/api/spa/book", data=self._booking_payload(customer_name=name, customer_phone=phone)).status_code,
                200,
            )
        self.assertEqual(
            self.client.post(
                "/api/spa/book",
                data=self._booking_payload(customer_name="丙", customer_phone="0933333333", booking_time="14:00"),
            ).status_code,
            200,
        )
        self._login()
        moved = self.client.post(
            "/api/spa/bookings/3/update_time",
            data={"booking_date": self._future_day(), "booking_time": "09:00"},
        )
        self.assertEqual(moved.status_code, 409)

    def test_capacity_cannot_drop_below_existing_bookings(self) -> None:
        self.assertEqual(self.client.post("/api/spa/book", data=self._booking_payload()).status_code, 200)
        self._login()
        response = self.client.post(
            "/api/spa/capacity/update",
            data={
                "store_id": "store_a",
                "override_date": self._future_day(),
                "override_time": "09:00",
                "capacity": "0",
            },
        )
        self.assertEqual(response.status_code, 409)

    def test_booking_cancel_is_auditable_soft_delete(self) -> None:
        self.assertEqual(self.client.post("/api/spa/book", data=self._booking_payload()).status_code, 200)
        self._login()
        response = self.client.post("/api/spa/bookings/1/delete")
        self.assertEqual(response.status_code, 200)
        with beauty.app.app_context():
            row = beauty.get_db().execute("SELECT status FROM spa_bookings WHERE id=1").fetchone()
            self.assertEqual(row["status"], "cancelled")

    def test_availability_rejects_invalid_month_and_store(self) -> None:
        self.assertEqual(
            self.client.get("/api/spa/availability?store_id=store_a&month=2026-99").status_code,
            400,
        )
        self.assertEqual(
            self.client.get("/api/spa/availability?store_id=unknown&month=2026-08").status_code,
            400,
        )


if __name__ == "__main__":
    unittest.main()
