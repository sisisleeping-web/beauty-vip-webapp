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

    def test_any_application_request_refreshes_coin_balance_cache(self) -> None:
        """日期驅動的批次到期不可讓快取在下一次使用前仍停在舊值。"""
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,phone,birthday,created_at,coin_balance) "
                "VALUES('點數快取','0912345678','1990-01-01','2026-01-01',0)"
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,500,500,'2026-01-01','2030-01-01','active',0,'2026-01-01')",
                (customer_id,),
            )
            db.commit()

        response = self.client.get("/spa/booking")
        self.assertEqual(response.status_code, 200)
        with beauty.app.app_context():
            row = beauty.get_db().execute("SELECT coin_balance FROM customers WHERE id=?", (customer_id,)).fetchone()
            self.assertEqual(row["coin_balance"], 500)

    def test_report_separates_member_point_summary_from_transaction_status(self) -> None:
        """日明細不可重複放會員總計，且待入帳點數須能直接追到來源交易。"""
        today = date.today()
        future_credit = beauty._add_one_month(today)
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('點數摘要測試','1990-01-01',?)",
                (today.isoformat(),),
            ).lastrowid
            pending_txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_earned,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a', ?, ?, 3799,3799,303,0,?,'normal')",
                (customer_id, today.isoformat(), today.strftime("%Y-%m"), today.isoformat()),
            ).lastrowid
            usable_txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_earned,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a', ?, ?, 1250,1250,100,0,?,'normal')",
                (customer_id, today.isoformat(), today.strftime("%Y-%m"), today.isoformat()),
            ).lastrowid
            for txn_id, amount, credit_day in (
                (pending_txn_id, 303, future_credit),
                (usable_txn_id, 100, today),
            ):
                db.execute(
                    "INSERT INTO coin_batches(customer_id,source_txn_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (customer_id, txn_id, amount, amount, credit_day.isoformat(), beauty._add_years(credit_day.isoformat(), 1), "active", 0, today.isoformat()),
                )
            beauty.sync_coin_balance(db, customer_id, today)
            db.commit()

            rows = beauty._build_report_detail_rows(db, {
                "month_from": today.strftime("%Y-%m"), "month_to": today.strftime("%Y-%m"),
                "year": today.strftime("%Y"), "store_id": "", "q": "點數摘要測試",
                "start_date": "", "end_date": "", "birthday_month": "", "vip_tier": "",
            })
            pending_row = next(row for row in rows if row["id"] == pending_txn_id)
            usable_row = next(row for row in rows if row["id"] == usable_txn_id)
            self.assertEqual(pending_row["coin_status"], "待入帳")
            self.assertEqual(pending_row["coin_available_date"], future_credit.isoformat())
            self.assertEqual(usable_row["coin_status"], "已入帳")
            self.assertEqual(pending_row["cust_total_earned"], 403)
            self.assertEqual(pending_row["cust_pending_coins"], 303)
            self.assertEqual(pending_row["cust_coin_balance"], 100)

        self._login(manager=True)
        query = f"/report?month_from={today:%Y-%m}&month_to={today:%Y-%m}&q=%E9%BB%9E%E6%95%B8%E6%91%98%E8%A6%81%E6%B8%AC%E8%A9%A6"
        page = self.client.get(query).get_data(as_text=True)
        self.assertIn("會員點數摘要", page)
        self.assertIn("待入帳點數", page)
        self.assertIn("歷史累計使用", page)
        self.assertIn("待入帳", page)
        self.assertIn(f"{future_credit.isoformat()} 可用", page)
        self.assertNotIn("累計折抵", page)

        export = self.client.get(query.replace("/report?", "/report/export.csv?"))
        export_text = export.data.decode("utf-8-sig")
        self.assertIn("入帳狀態,可用日期,歷史累計獲得,歷史累計使用,待入帳點數,目前可用點數", export_text)

    def test_customer_self_service_uses_canonical_point_ledger(self) -> None:
        """會員頁摘要與流水必須同源，且待入帳與扣點都可追溯。"""
        today = date.today()
        prior_day = today - timedelta(days=2)
        with beauty.app.app_context():
            db = beauty.get_db()
            customer_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('會員點數頁測試','1990-01-01',?)",
                (today.isoformat(),),
            ).lastrowid
            posted_txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_earned,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a', ?, ?, 12500,12500,1000,0,?,'normal')",
                (customer_id, prior_day.isoformat(), prior_day.strftime("%Y-%m"), prior_day.isoformat()),
            ).lastrowid
            pending_txn_id = db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_earned,cashback,created_at,entry_mode) "
                "VALUES(?, 'store_a', ?, ?, 3799,3799,303,0,?,'normal')",
                (customer_id, today.isoformat(), today.strftime("%Y-%m"), today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_redeemed,cashback,created_at,entry_mode,note) "
                "VALUES(?, 'store_a', ?, ?, 0,0,500,0,?,'coin_deduct','消費折抵')",
                (customer_id, today.isoformat(), today.strftime("%Y-%m"), today.isoformat()),
            )
            for txn_id, earned, remaining, credit_day in (
                (posted_txn_id, 1000, 500, prior_day),
                (pending_txn_id, 303, 303, beauty._add_one_month(today)),
            ):
                db.execute(
                    "INSERT INTO coin_batches(customer_id,source_txn_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (customer_id, txn_id, earned, remaining, credit_day.isoformat(), beauty._add_years(credit_day.isoformat(), 1), "active", 0, today.isoformat()),
                )
            beauty.sync_coin_balance(db, customer_id, today)
            db.commit()

            result = beauty._build_customer_result(customer_id)
            self.assertIsNotNone(result)
            customer = result["customer"]
            self.assertEqual(customer["coin_balance"], 500)
            self.assertEqual(customer["pending_coins"], 303)
            self.assertEqual(customer["total_earned"], 1303)
            self.assertEqual(customer["total_used"], 500)
            self.assertEqual(customer["next_pending_date"], beauty._add_one_month(today).isoformat())
            self.assertIn({
                "date": today.isoformat(), "points_change": 303, "kind": "一般消費",
                "detail": "消費 $3,799", "status": "待入帳",
                "available_date": beauty._add_one_month(today).isoformat(), "timestamp": today.isoformat(),
            }, result["point_history"])
            self.assertTrue(any(item["points_change"] == -500 and item["status"] == "已使用" for item in result["point_history"]))

        page = self.client.get(f"/my?cid={customer_id}").get_data(as_text=True)
        self.assertIn("目前可用點數", page)
        self.assertIn("待入帳點數", page)
        self.assertIn("歷史累計獲得", page)
        self.assertIn("歷史累計使用", page)
        self.assertIn("點數紀錄", page)
        self.assertIn("等級紀錄", page)
        self.assertIn("待入帳", page)
        self.assertIn(f"{beauty._add_one_month(today).isoformat()} 可用", page)
        self.assertIn("-500 點", page)
        self.assertNotIn("目前點數餘額", page)

    def test_credit_date_uses_next_month_same_day_or_month_end(self) -> None:
        self.assertEqual(beauty._add_one_month(date(2026, 1, 31)), date(2026, 2, 28))
        self.assertEqual(beauty._add_one_month(date(2028, 1, 31)), date(2028, 2, 29))

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
