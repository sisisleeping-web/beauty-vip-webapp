from __future__ import annotations

import re
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

    def _birthday_far_from_today(self) -> str:
        """給不測生日邏輯、只是需要一個「顧客」的測試用：保證落在 Action Board
        today/missed/upcoming 判定窗口之外（跟今天差 6 個月），避免測試結果隨執行日期漂移。"""
        today = date.today()
        safe_month = ((today.month + 5) % 12) + 1
        return f"1990-{safe_month:02d}-15"

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

    def test_action_board_summary_reads_existing_sources_without_mutation(self) -> None:
        """登入提示只讀既有 ledger/workflow，且點數到期用 coin_batches 邊界判斷。"""
        # 用固定 anchor（而非 date.today()）讓生日判定不隨執行日期漂移。
        today = date(2026, 6, 15)
        other_birthday = "1990-01-01"
        current_month_birthday = f"1990-{today.month:02d}-{today.day:02d}"  # == anchor，落在「今日壽星」

        with beauty.app.app_context():
            db = beauty.get_db()
            red_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('紅色到期',?,?)",
                (other_birthday, today.isoformat()),
            ).lastrowid
            orange_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('橘色到期',?,?)",
                (other_birthday, today.isoformat()),
            ).lastrowid
            future_credit_id = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('未入帳不提醒',?,?)",
                (other_birthday, today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('本月壽星',?,?)",
                (current_month_birthday, today.isoformat()),
            )
            for customer_id, remaining, credit_offset, expiry_offset in [
                (red_id, 100, 0, 3),
                (red_id, 300, 0, 20),
                (orange_id, 500, 0, 30),
                (future_credit_id, 999, 1, 5),
            ]:
                db.execute(
                    "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                    "VALUES(?,?,?,?,?,?,0,?)",
                    (
                        customer_id,
                        remaining,
                        remaining,
                        (today + timedelta(days=credit_offset)).isoformat(),
                        (today + timedelta(days=expiry_offset)).isoformat(),
                        "active",
                        today.isoformat(),
                    ),
                )
            db.execute(
                "INSERT INTO review_flags(item_type,item_key,status,note,updated_at) VALUES('coin_refund','batch1','unreviewed','test',?)",
                (today.isoformat(),),
            )
            db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,gift_status,created_at) "
                "VALUES(?,?,'P級美咖','A級美咖','pending',?)",
                (red_id, today.isoformat(), today.isoformat()),
            )
            db.commit()

            before_batches = db.execute("SELECT COUNT(*) FROM coin_batches").fetchone()[0]
            before_reviews = db.execute("SELECT COUNT(*) FROM review_flags").fetchone()[0]
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["red_expiry"]["count"], 1)
            self.assertEqual(summary["categories"]["orange_expiry"]["count"], 1)
            self.assertEqual(summary["categories"]["review"]["count"], 1)
            self.assertEqual(summary["categories"]["gift"]["count"], 1)
            self.assertEqual(summary["categories"]["birthday_today"]["count"], 1)
            self.assertEqual(summary["total_count"], 5)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM coin_batches").fetchone()[0], before_batches)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM review_flags").fetchone()[0], before_reviews)

    def test_action_board_birthdays_sort_by_day_not_birth_year(self) -> None:
        """本月壽星要照生日「日期」排，不能被出生年支配（出生年較晚者不該排到後面）。
        三人生日都選在 anchor(28日) 之前，同屬「本月漏關心」bucket，才能單純驗證排序。"""
        today = date(2026, 6, 28)
        month = today.month
        with beauty.app.app_context():
            db = beauty.get_db()
            # 刻意讓「日期較晚、出生年較早」排在插入順序前面，只有靠出生月-日排序才會正確
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('晚日早年',?,?)",
                (f"1970-{month:02d}-20", today.isoformat()),
            )
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('早日晚年',?,?)",
                (f"2005-{month:02d}-03", today.isoformat()),
            )
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('中日中年',?,?)",
                (f"1990-{month:02d}-15", today.isoformat()),
            )
            db.commit()

            actions = beauty.build_action_board_actions(db, today)
            birthday_names = [a["customer_name"] for a in actions if a["action_type"] == "birthday"]
            self.assertEqual(birthday_names, ["早日晚年", "中日中年", "晚日早年"])

    def test_action_board_modal_shows_once_per_login_session(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            # HTTP route 一律用 server 端真實 date.today()，所以固定用「今天」本身的 MM-DD，
            # 確定落在「今日壽星」bucket，測試結果才不隨執行日期漂移。
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('提示窗壽星',?,?)",
                (f"1990-{today.month:02d}-{today.day:02d}", today.isoformat()),
            )
            db.commit()

        self._login()
        first = self.client.get("/").get_data(as_text=True)
        self.assertIn("今日有 1 件待關注事項", first)
        self.assertIn("今日壽星", first)
        second = self.client.get("/").get_data(as_text=True)
        self.assertNotIn("今日有 1 件待關注事項", second)

    def test_action_board_handle_hides_event_without_touching_coin_ledger(self) -> None:
        today = date.today()
        self._login()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('已提醒測試',?,?)",
                (self._birthday_far_from_today(), today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,300,300,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=5)).isoformat(), today.isoformat()),
            )
            db.commit()
            action = beauty.build_action_board_actions(db, today)[0]
            self.assertEqual(action["action_type"], "red_expiry")
            before_remaining = db.execute("SELECT remaining_amount FROM coin_batches").fetchone()[0]

        response = self.client.post(
            "/actions/handle",
            data={"action_type": action["action_type"], "action_key": action["action_key"], "next": "/actions"},
        )
        self.assertEqual(response.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            self.assertEqual(beauty.build_action_board_summary(db, today)["total_count"], 0)
            self.assertEqual(db.execute("SELECT remaining_amount FROM coin_batches").fetchone()[0], before_remaining)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM customer_action_logs").fetchone()[0], 1)

    # ── Action Board V1.1：actionable / informational 語意 regression ──────────

    def test_action_board_birthday_0001_year_hidden_from_presentation(self) -> None:
        """Production 有 0001-MM-DD 佔位出生年，UI 只顯示 MM/DD，且不回寫資料庫。"""
        self.assertEqual(beauty._format_birthday_display("0001-09-06"), "09/06")
        self.assertEqual(beauty._format_birthday_display("1989-09-15"), "1989-09-15")

        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('佔位年壽星','0001-06-15','2026-01-01')"
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertNotIn("0001", actions[0]["detail"])
            self.assertIn("06/15", actions[0]["detail"])
            row = db.execute("SELECT birthday FROM customers WHERE name='佔位年壽星'").fetchone()
            self.assertEqual(row["birthday"], "0001-06-15")  # 只改 presentation，不回寫原始生日資料

    def test_action_board_birthday_today_unresolved_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('今日壽星測試','1990-06-15','2026-01-01')"
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["birthday_bucket"], "today")
            self.assertTrue(beauty._action_is_actionable(actions[0]))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["birthday_today"]["count"], 1)
            self.assertEqual(summary["total_count"], 1)

    def test_action_board_birthday_today_handled_excluded_from_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('今日壽星已關心','1990-06-15','2026-01-01')"
            ).lastrowid
            action = beauty.build_action_board_actions(db, today)[0]
            db.execute(
                "INSERT INTO customer_action_logs(customer_id,action_type,action_key,status,handled_at,handled_by,created_at) "
                "VALUES(?,?,?,'handled',?,'staff',?)",
                (cid, action["action_type"], action["action_key"], today.isoformat(), today.isoformat()),
            )
            db.commit()
            self.assertEqual(beauty.build_action_board_actions(db, today), [])
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["birthday_today"]["count"], 0)
            self.assertEqual(summary["total_count"], 0)

    def test_action_board_birthday_upcoming_7d_is_informational_not_badge(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('未來壽星','1990-06-18','2026-01-01')"
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["birthday_bucket"], "upcoming")
            self.assertFalse(beauty._action_is_actionable(actions[0]))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["total_count"], 0)  # 不計入主 badge
            self.assertEqual(summary["informational"]["birthday_upcoming"]["count"], 1)

    def test_action_board_birthday_missed_this_month_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('本月漏關心測試','1990-06-05','2026-01-01')"
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["birthday_bucket"], "missed")
            self.assertTrue(beauty._action_is_actionable(actions[0]))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["birthday_missed"]["count"], 1)
            self.assertEqual(summary["total_count"], 1)

    def test_action_board_birthday_missed_handled_excluded_from_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('本月漏關心已處理','1990-06-05','2026-01-01')"
            ).lastrowid
            action = beauty.build_action_board_actions(db, today)[0]
            db.execute(
                "INSERT INTO customer_action_logs(customer_id,action_type,action_key,status,handled_at,handled_by,created_at) "
                "VALUES(?,?,?,'handled',?,'staff',?)",
                (cid, action["action_type"], action["action_key"], today.isoformat(), today.isoformat()),
            )
            db.commit()
            self.assertEqual(beauty.build_action_board_actions(db, today), [])
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["birthday_missed"]["count"], 0)
            self.assertEqual(summary["total_count"], 0)

    def test_action_board_pending_gift_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('待發升等禮','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,gift_status,created_at) "
                "VALUES(?,?,'S級美咖','P級美咖','pending',?)",
                (cid, today.isoformat(), today.isoformat()),
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["action_type"], "gift")
            self.assertTrue(beauty._action_is_actionable(actions[0]))
            self.assertIn("升等日：", actions[0]["detail"])
            self.assertIn("已等待", actions[0]["detail"])

    def test_action_board_delivered_gift_is_not_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('已發升等禮','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,gift_status,created_at) "
                "VALUES(?,?,'S級美咖','P級美咖','delivered',?)",
                (cid, today.isoformat(), today.isoformat()),
            )
            db.commit()
            self.assertEqual(beauty.build_action_board_actions(db, today), [])

    def test_action_board_unresolved_review_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO review_flags(item_type,item_key,status,note,updated_at) VALUES('coin_refund','r1','unreviewed','test',?)",
                (today.isoformat(),),
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["action_type"], "review")
            self.assertTrue(beauty._action_is_actionable(actions[0]))

    def test_action_board_reviewed_review_is_not_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO review_flags(item_type,item_key,status,note,updated_at) VALUES('coin_refund','r2','reviewed','test',?)",
                (today.isoformat(),),
            )
            db.commit()
            self.assertEqual(beauty.build_action_board_actions(db, today), [])

    def test_action_board_red_expiry_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('紅色到期測試','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=3)).isoformat(), today.isoformat()),
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["action_type"], "red_expiry")
            self.assertTrue(beauty._action_is_actionable(actions[0]))

    def test_action_board_orange_expiry_keeps_existing_priority_and_count_semantics(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('橘色到期測試','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=20)).isoformat(), today.isoformat()),
            )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["action_type"], "orange_expiry")
            self.assertTrue(beauty._action_is_actionable(actions[0]))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["orange_expiry"]["count"], 1)
            self.assertEqual(summary["total_count"], 1)

    def test_action_board_same_customer_red_and_orange_not_double_counted(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('紅橘同人測試','1990-01-01','2026-01-01')"
            ).lastrowid
            for remaining, expiry_offset in [(100, 3), (200, 20)]:
                db.execute(
                    "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                    "VALUES(?,?,?,?,?, 'active',0,?)",
                    (cid, remaining, remaining, today.isoformat(), (today + timedelta(days=expiry_offset)).isoformat(), today.isoformat()),
                )
            db.commit()
            actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(actions), 1)  # 同 customer 不會同時出現 red 跟 orange 兩筆
            self.assertEqual(actions[0]["action_type"], "red_expiry")
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["total_count"], 1)

    def test_actions_page_hides_technical_action_key_from_rendered_ui(self) -> None:
        today = date.today()
        self._login()
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('技術key測試',?,?)",
                (f"1990-{today.month:02d}-{today.day:02d}", today.isoformat()),
            )
            db.commit()
        html = self.client.get("/actions").get_data(as_text=True)
        self.assertNotIn("key: ", html)  # 舊版明文顯示的 "key: birthday:..." 之類標籤必須消失
        self.assertIn('data-action-key="birthday:', html)  # 但 data attribute／form 仍保留供前端與稽核使用

    def test_actions_page_button_text_differs_between_pending_and_handled(self) -> None:
        today = date.today()
        self._login()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('文案測試甲','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=3)).isoformat(), today.isoformat()),
            )
            db.commit()
            action = beauty.build_action_board_actions(db, today)[0]
            db.execute(
                "INSERT INTO customer_action_logs(customer_id,action_type,action_key,status,handled_at,handled_by,created_at) "
                "VALUES(?,?,?,'handled',?,'staff',?)",
                (cid, action["action_type"], action["action_key"], today.isoformat(), today.isoformat()),
            )
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('文案測試乙','1990-01-02','2026-01-01')"
            )
            cid2 = db.execute("SELECT id FROM customers WHERE name='文案測試乙'").fetchone()["id"]
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid2, today.isoformat(), (today + timedelta(days=3)).isoformat(), today.isoformat()),
            )
            db.commit()
        html = self.client.get("/actions").get_data(as_text=True)
        self.assertIn("標記已提醒", html)  # 未處理：動作語意
        self.assertIn("✓ 已提醒", html)   # 已處理：狀態語意
        self.assertNotIn("✓ 標記已提醒", html)

    def test_navbar_count_matches_modal_actionable_total(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('Navbar一致性測試',?,?)",
                (f"1990-{today.month:02d}-{today.day:02d}", today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=3)).isoformat(), today.isoformat()),
            )
            db.commit()
        self._login()
        html = self.client.get("/").get_data(as_text=True)
        navbar_match = re.search(r"🔔 待關注 (\d+)", html)
        modal_match = re.search(r"今日有 (\d+) 件待關注事項", html)
        self.assertIsNotNone(navbar_match)
        self.assertIsNotNone(modal_match)
        self.assertEqual(navbar_match.group(1), modal_match.group(1))

    def test_homepage_preview_only_shows_actionable_top_n(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            # 一個 informational 未來壽星 + 一個 actionable red expiry
            upcoming = today + timedelta(days=3)
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('首頁預告壽星',?,?)",
                (f"1990-{upcoming.month:02d}-{upcoming.day:02d}", today.isoformat()),
            )
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('首頁actionable測試','1990-01-01','2026-01-01')"
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=3)).isoformat(), today.isoformat()),
            )
            db.commit()
            preview = beauty._build_action_preview(db)
            self.assertTrue(all(beauty._action_is_actionable(a) for a in preview))
            self.assertTrue(any(a["action_type"] == "red_expiry" for a in preview))
            self.assertFalse(any(a.get("birthday_bucket") == "upcoming" for a in preview))

    def test_actions_page_informational_birthday_does_not_pollute_actionable_total(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('actions頁預告壽星','1990-06-18','2026-01-01')"
            )
            db.commit()
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["total_count"], 0)
            self.assertEqual(summary["informational"]["birthday_upcoming"]["count"], 1)

    def test_dismiss_later_button_does_not_write_customer_action_logs(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('稍後處理測試',?,?)",
                (f"1990-{today.month:02d}-{today.day:02d}", today.isoformat()),
            )
            db.commit()
            before = db.execute("SELECT COUNT(*) FROM customer_action_logs").fetchone()[0]

        self._login()
        html = self.client.get("/").get_data(as_text=True)
        modal_start = html.index('id="actionModalBackdrop"')
        later_idx = html.index("稍後處理", modal_start)
        segment = html[modal_start:later_idx]
        self.assertEqual(segment.count("<form"), segment.count("</form"))  # 稍後處理按鈕不在任何 <form> 內，純前端隱藏 modal

        with beauty.app.app_context():
            db = beauty.get_db()
            after = db.execute("SELECT COUNT(*) FROM customer_action_logs").fetchone()[0]
        self.assertEqual(after, before)

    # ── Audit Governance V1 regression ──────────────────────────────────────

    def _insert_completed_quarterly(
        self, db, completed_at: date, period_key: str,
        critical: int = 0, high: int = 0, medium: int = 0, low: int = 0, info: int = 0,
        result: str = "PASS", report_ref: str = "AUDIT-TEST",
    ) -> None:
        due_date = completed_at.isoformat()
        now = completed_at.isoformat()
        db.execute(
            "INSERT INTO system_audits(audit_type,period_key,due_date,status,completed_at,completed_by,result,"
            "critical_count,high_count,medium_count,low_count,info_count,report_ref,notes,created_at,updated_at) "
            "VALUES('quarterly_deep',?,?, 'completed',?, 'manager',?,?,?,?,?,?,?,?,?,?)",
            (period_key, due_date, now, result, critical, high, medium, low, info, report_ref, "", now, now),
        )
        db.commit()

    def test_quarterly_audit_due_is_actionable(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 3, 10), "2026-Q1")  # +3 months due = 2026-06-10 <= anchor
            actions = beauty.build_action_board_actions(db, today)
            audit_actions = [a for a in actions if a["action_type"] == "system_audit"]
            self.assertEqual(len(audit_actions), 1)
            self.assertTrue(beauty._action_is_actionable(audit_actions[0]))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["categories"]["system_audit"]["count"], 1)
            self.assertEqual(summary["total_count"], 1)

    def test_quarterly_audit_not_due_does_not_enter_badge(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 5, 1), "2026-Q2")  # +3 months due = 2026-08-01 > anchor
            actions = beauty.build_action_board_actions(db, today)
            self.assertFalse(any(a["action_type"] == "system_audit" for a in actions))
            summary = beauty.build_action_board_summary(db, today)
            self.assertEqual(summary["total_count"], 0)

    def test_quarterly_audit_completed_reminder_disappears(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 3, 10), "2026-Q1")
            self.assertTrue(any(a["action_type"] == "system_audit" for a in beauty.build_action_board_actions(db, today)))
            # 補登這一期（due 2026-06-10）的完成結果
            self._insert_completed_quarterly(db, date(2026, 6, 12), "2026-Q2")
            actions_after = beauty.build_action_board_actions(db, today)
            self.assertFalse(any(a["action_type"] == "system_audit" for a in actions_after))

    def test_quarterly_completion_rejected_without_required_evidence(self) -> None:
        self._login(manager=True)
        base_payload = {
            "audit_type": "quarterly_deep", "period_key": "2026-Q9", "due_date": "2026-06-15",
            "result": "PASS", "report_ref": "AUDIT-X",
            "critical_count": "0", "high_count": "0", "medium_count": "0", "low_count": "0", "info_count": "0",
        }
        # 缺 report_ref
        payload = dict(base_payload); payload.pop("report_ref")
        self.assertEqual(self.client.post("/admin/system-audits/complete", data=payload).status_code, 400)
        # 缺 result
        payload = dict(base_payload); payload.pop("result")
        self.assertEqual(self.client.post("/admin/system-audits/complete", data=payload).status_code, 400)
        # 缺 counts
        payload = dict(base_payload); payload.pop("critical_count")
        self.assertEqual(self.client.post("/admin/system-audits/complete", data=payload).status_code, 400)
        with beauty.app.app_context():
            db = beauty.get_db()
            self.assertIsNone(
                db.execute("SELECT 1 FROM system_audits WHERE period_key='2026-Q9'").fetchone()
            )

    def test_beautician_cannot_complete_audit(self) -> None:
        self._login(manager=False)
        payload = {
            "audit_type": "quarterly_deep", "period_key": "2026-Q9", "due_date": "2026-06-15",
            "result": "PASS", "report_ref": "AUDIT-X",
            "critical_count": "0", "high_count": "0", "medium_count": "0", "low_count": "0", "info_count": "0",
        }
        self.assertEqual(self.client.post("/admin/system-audits/complete", data=payload).status_code, 403)

    def test_manager_can_complete_audit(self) -> None:
        self._login(manager=True)
        payload = {
            "audit_type": "quarterly_deep", "period_key": "2026-Q9", "due_date": "2026-06-15",
            "result": "PASS_WITH_FINDINGS", "report_ref": "AUDIT-2026-Q9",
            "critical_count": "0", "high_count": "1", "medium_count": "2", "low_count": "3", "info_count": "4",
            "notes": "測試登記",
        }
        resp = self.client.post("/admin/system-audits/complete", data=payload)
        self.assertEqual(resp.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            row = db.execute("SELECT * FROM system_audits WHERE period_key='2026-Q9'").fetchone()
            self.assertEqual(row["status"], "completed")
            self.assertEqual(row["result"], "PASS_WITH_FINDINGS")
            self.assertEqual(row["report_ref"], "AUDIT-2026-Q9")
            self.assertEqual(row["completed_by"], "manager")

    def test_completed_audit_computes_next_due_plus_3_calendar_months(self) -> None:
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 6, 15), "2026-Q2")
            period_key, due_date = beauty.compute_next_quarterly_due(db, date(2026, 6, 20))
            self.assertEqual(due_date, date(2026, 9, 15))
            self.assertEqual(period_key, "2026-Q3")

    def test_quarterly_due_uses_safe_month_end_arithmetic(self) -> None:
        """baseline 完成日是 1/31 這種月底日，+3 個月落到沒有 31 號的月份要取該月最後一天，不能用固定 90 天。"""
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 1, 31), "2026-Q0")
            period_key, due_date = beauty.compute_next_quarterly_due(db, date(2026, 2, 1))
            self.assertEqual(due_date, date(2026, 4, 30))  # 4月無31日，取月底
            self.assertNotEqual(due_date, date(2026, 1, 31) + timedelta(days=90))  # 不是固定90天

    def test_quarterly_overdue_days_reported(self) -> None:
        today = date(2026, 6, 20)
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 3, 10), "2026-Q1")  # due 2026-06-10，anchor晚10天
            action = beauty.build_system_audit_action(db, today)
            self.assertIsNotNone(action)
            self.assertIn("已逾期 10 天", action["detail"])
            self.assertEqual(action["audit_priority_tier"], "high")  # overdue > 7 天

    def test_quarterly_previous_high_critical_shown_and_forces_high_priority(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            # 上一期有 unresolved High/Critical，即使這期才剛到期（0 天逾期）也要是 high tier
            self._insert_completed_quarterly(db, date(2026, 3, 15), "2026-Q1", critical=1, high=2)
            action = beauty.build_system_audit_action(db, today)
            self.assertIsNotNone(action)
            self.assertEqual(action["audit_priority_tier"], "high")
            self.assertIn("未解決的 High/Critical", action["detail"])

    def test_navbar_modal_homepage_count_due_audit_exactly_once(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, today - timedelta(days=100), "count-once-baseline")
            db.commit()
        self._login()
        home = self.client.get("/").get_data(as_text=True)
        navbar_match = re.search(r"🔔 待關注 (\d+)", home)
        modal_match = re.search(r"今日有 (\d+) 件待關注事項", home)
        self.assertIsNotNone(navbar_match)
        self.assertIsNotNone(modal_match)
        self.assertEqual(navbar_match.group(1), modal_match.group(1))
        with beauty.app.app_context():
            db = beauty.get_db()
            summary = beauty.build_action_board_summary(db)
            self.assertEqual(summary["categories"]["system_audit"]["count"], 1)  # 剛好一次，不重複

    def test_monthly_quick_does_not_pollute_beautician_badge(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            actions = beauty.build_action_board_actions(db, today)
            self.assertFalse(any(a["action_type"] == "monthly_quick" for a in actions))
            summary = beauty.build_action_board_summary(db, today)
            self.assertNotIn("monthly_quick", summary["categories"])
            self.assertNotIn("monthly_quick", summary.get("informational", {}))

    def test_waived_audit_requires_reason(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO system_audits(audit_type,period_key,due_date,status,created_at,updated_at) "
                "VALUES('quarterly_deep','2026-Q-waive','2026-06-01','due','2026-01-01','2026-01-01')"
            )
            db.commit()
        no_reason = self.client.post(
            "/admin/system-audits/waive",
            data={"audit_type": "quarterly_deep", "period_key": "2026-Q-waive", "reason": ""},
        )
        self.assertEqual(no_reason.status_code, 400)
        with_reason = self.client.post(
            "/admin/system-audits/waive",
            data={"audit_type": "quarterly_deep", "period_key": "2026-Q-waive", "reason": "已於其他管道確認無風險"},
        )
        self.assertEqual(with_reason.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            row = db.execute("SELECT status, notes FROM system_audits WHERE period_key='2026-Q-waive'").fetchone()
            self.assertEqual(row["status"], "waived")
            self.assertEqual(row["notes"], "已於其他管道確認無風險")

    def test_audit_history_ordering(self) -> None:
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 1, 1), "hist-2026-Q1")
            self._insert_completed_quarterly(db, date(2026, 4, 1), "hist-2026-Q2")
            self._insert_completed_quarterly(db, date(2026, 2, 1), "hist-2026-Qmid")
            rows = db.execute(
                "SELECT period_key FROM system_audits ORDER BY due_date DESC, id DESC"
            ).fetchall()
            keys = [r["period_key"] for r in rows]
            # due_date 降冪：最近到期在最前面
            self.assertEqual(
                keys.index("hist-2026-Q2") < keys.index("hist-2026-Qmid") < keys.index("hist-2026-Q1"),
                True,
            )

    def test_report_ref_persisted(self) -> None:
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 5, 1), "2026-Q-ref", report_ref="AUDIT-2026-Q2-REF")
            row = db.execute("SELECT report_ref FROM system_audits WHERE period_key='2026-Q-ref'").fetchone()
            self.assertEqual(row["report_ref"], "AUDIT-2026-Q2-REF")

    def test_audit_completion_does_not_change_financial_tables(self) -> None:
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('財務不動測試','1990-01-01','2026-01-01')"
            )
            db.commit()
            before = {
                t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("customers", "transactions", "coin_batches", "coin_redemptions", "tier_upgrades", "review_flags")
            }
        self.client.post("/admin/system-audits/complete", data={
            "audit_type": "quarterly_deep", "period_key": "2026-Q-fin", "due_date": "2026-06-15",
            "result": "PASS", "report_ref": "AUDIT-FIN",
            "critical_count": "0", "high_count": "0", "medium_count": "0", "low_count": "0", "info_count": "0",
        })
        with beauty.app.app_context():
            db = beauty.get_db()
            after = {
                t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("customers", "transactions", "coin_batches", "coin_redemptions", "tier_upgrades", "review_flags")
            }
        self.assertEqual(before, after)

    def test_system_audits_page_requires_login(self) -> None:
        unauth = self.client.get("/admin/system-audits").get_data(as_text=True)
        # 未登入被 require_main_auth 擋下，渲染的是主網密碼鎖定頁，不是稽核頁內容。
        # navbar 本身不分登入狀態都會出現「🛡️ 系統稽核」連結文字，所以用「有沒有頁面標題 h2」
        # 而不是單純字串是否出現來判斷內容有沒有真的被擋下。
        self.assertIn("系統存取驗證", unauth)
        self.assertNotIn("<h2>🛡️ 系統稽核</h2>", unauth)

        self._login()
        authed = self.client.get("/admin/system-audits").get_data(as_text=True)
        self.assertIn("<h2>🛡️ 系統稽核</h2>", authed)

    def test_event_driven_audit_does_not_affect_quarterly_schedule_anchor(self) -> None:
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 3, 10), "2026-Q1")
            before = beauty.compute_next_quarterly_due(db, date(2026, 6, 15))
            db.execute(
                "INSERT INTO system_audits(audit_type,period_key,due_date,status,notes,created_at,updated_at) "
                "VALUES('event_driven','event-test','2026-05-01','due','制度變更','2026-05-01','2026-05-01')"
            )
            db.commit()
            after = beauty.compute_next_quarterly_due(db, date(2026, 6, 15))
        self.assertEqual(before, after)

    def test_duplicate_audit_period_not_recreated(self) -> None:
        self._login(manager=True)
        payload = {"audit_type": "quarterly_deep", "period_key": "2026-Q-dup", "due_date": "2026-06-01"}
        self.client.post("/admin/system-audits/start", data=payload)
        self.client.post("/admin/system-audits/start", data=payload)
        with beauty.app.app_context():
            db = beauty.get_db()
            count = db.execute(
                "SELECT COUNT(*) FROM system_audits WHERE audit_type='quarterly_deep' AND period_key='2026-Q-dup'"
            ).fetchone()[0]
            self.assertEqual(count, 1)

    def test_audit_reminder_action_key_is_deterministic(self) -> None:
        today = date(2026, 6, 15)
        with beauty.app.app_context():
            db = beauty.get_db()
            self._insert_completed_quarterly(db, date(2026, 3, 10), "2026-Q1")
            action1 = beauty.build_system_audit_action(db, today)
            action2 = beauty.build_system_audit_action(db, today)
            self.assertEqual(action1["action_key"], action2["action_key"])
            self.assertEqual(action1["action_key"], "system_audit:quarterly_deep:2026-Q2")

    def test_admin_page_offers_baseline_creation_when_no_history_exists(self) -> None:
        """完全沒有稽核歷史時（真正的第一次），compute_next_quarterly_due 會是 None，
        一般的「啟動本期」按鈕不會出現——管理頁必須另外提供建立 baseline 的入口，
        否則會卡死：永遠沒有第一筆 completed row 可以當排程 anchor。"""
        self._login(manager=True)
        with beauty.app.app_context():
            db = beauty.get_db()
            self.assertIsNone(beauty.compute_next_quarterly_due(db, date.today()))
        admin = self.client.get("/admin/system-audits").get_data(as_text=True)
        self.assertIn("建立第一次 Baseline Quarterly Deep Audit", admin)

        resp = self.client.post("/admin/system-audits/start", data={
            "audit_type": "quarterly_deep", "period_key": "BASELINE_QUARTERLY_DEEP",
            "due_date": date.today().isoformat(),
        })
        self.assertEqual(resp.status_code, 302)
        with beauty.app.app_context():
            db = beauty.get_db()
            row = db.execute(
                "SELECT status FROM system_audits WHERE period_key='BASELINE_QUARTERLY_DEEP'"
            ).fetchone()
            self.assertEqual(row["status"], "in_progress")

    def test_expiry_action_key_is_stable_for_partial_use_and_non_earlier_batch_addition(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('Key穩定',?,?)",
                (self._birthday_far_from_today(), today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,500,500,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=5)).isoformat(), today.isoformat()),
            )
            first_key = beauty.build_action_board_actions(db, today)[0]["action_key"]
            db.execute("UPDATE coin_batches SET remaining_amount=120 WHERE customer_id=?", (cid,))
            self.assertEqual(beauty.build_action_board_actions(db, today)[0]["action_key"], first_key)
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,200,200,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=6)).isoformat(), today.isoformat()),
            )
            self.assertEqual(beauty.build_action_board_actions(db, today)[0]["action_key"], first_key)
            db.execute("UPDATE coin_batches SET remaining_amount=0 WHERE customer_id=?", (cid,))
            self.assertEqual(beauty.build_action_board_actions(db, today), [])

    def test_expiry_next_batch_reopens_after_handled_nearest_batch_is_consumed(self) -> None:
        """Document current V1 semantics: a later expiry date becomes a new reminder."""
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('下一批提醒',?,?)",
                (self._birthday_far_from_today(), today.isoformat()),
            ).lastrowid
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,100,100,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=5)).isoformat(), today.isoformat()),
            )
            db.execute(
                "INSERT INTO coin_batches(customer_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) "
                "VALUES(?,500,500,?,?, 'active',0,?)",
                (cid, today.isoformat(), (today + timedelta(days=20)).isoformat(), today.isoformat()),
            )
            first_action = beauty.build_action_board_actions(db, today)[0]
            self.assertEqual(first_action["action_type"], "red_expiry")
            db.execute(
                "INSERT INTO customer_action_logs(customer_id,action_type,action_key,status,handled_at,handled_by,created_at) "
                "VALUES(?,?,?,'handled',?,'staff',?)",
                (cid, first_action["action_type"], first_action["action_key"], today.isoformat(), today.isoformat()),
            )
            self.assertEqual(beauty.build_action_board_actions(db, today), [])

            db.execute(
                "UPDATE coin_batches SET remaining_amount=0 WHERE customer_id=? AND expires_date=?",
                (cid, (today + timedelta(days=5)).isoformat()),
            )
            next_actions = beauty.build_action_board_actions(db, today)
            self.assertEqual(len(next_actions), 1)
            self.assertEqual(next_actions[0]["action_type"], "orange_expiry")
            self.assertNotEqual(next_actions[0]["action_key"], first_action["action_key"])
            self.assertIn((today + timedelta(days=20)).isoformat(), next_actions[0]["action_key"])

    def test_actions_page_and_nav_badge_use_pending_summary(self) -> None:
        today = date.today()
        self._login()
        with beauty.app.app_context():
            db = beauty.get_db()
            # HTTP route 用 server 真實 date.today()，固定用「今天」MM-DD 確保落在今日壽星 bucket。
            db.execute(
                "INSERT INTO customers(name,birthday,created_at) VALUES('Badge壽星',?,?)",
                (f"1990-{today.month:02d}-{today.day:02d}", today.isoformat()),
            )
            db.commit()
        home = self.client.get("/").get_data(as_text=True)
        self.assertIn("🔔 待關注 1", home)
        self.assertIn("今日待關注 1 件", home)
        actions = self.client.get("/actions").get_data(as_text=True)
        self.assertIn("今日壽星", actions)

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

    def test_portal_v2_batches_expiry_and_tier_progress_are_canonical(self) -> None:
        today = date.today()
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute("INSERT INTO customers(name,birthday,created_at) VALUES('V2會員','1990-01-01',?)", (today.isoformat(),)).lastrowid
            txn = db.execute("INSERT INTO transactions(customer_id,store_id,txn_date,month_key,amount,final_amount,coins_earned,cashback,created_at,entry_mode) VALUES(?, 'store_a', ?, ?, 9000,9000,900,0,?,'normal')", (cid, today.isoformat(), today.strftime('%Y-%m'), today.isoformat())).lastrowid
            for remaining, credit, expiry in [(200, today + timedelta(days=3), today + timedelta(days=40)), (300, today + timedelta(days=7), today + timedelta(days=20))]:
                db.execute("INSERT INTO coin_batches(customer_id,source_txn_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (cid, txn, remaining, remaining, credit.isoformat(), expiry.isoformat(), 'active', 0, today.isoformat()))
            db.execute("INSERT INTO coin_batches(customer_id,source_txn_id,earned_amount,remaining_amount,credit_date,expires_date,status,is_legacy,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (cid, txn, 99, 99, today.isoformat(), (today - timedelta(days=1)).isoformat(), 'active', 0, today.isoformat()))
            db.commit()
            result = beauty._build_customer_result(cid)
            self.assertEqual(result['customer']['pending_coins'], 500)
            self.assertEqual(result['customer']['pending_detail_total'], 500)
            self.assertEqual([row['credit_date'] for row in result['pending_batches']], [(today + timedelta(days=3)).isoformat(), (today + timedelta(days=7)).isoformat()])
            self.assertEqual(result['customer']['expiring_coins'], 300)
            self.assertEqual(result['expiring_batches'][0]['expires_date'], (today + timedelta(days=20)).isoformat())
            self.assertEqual(result['customer']['tier_progress']['next_tier'], 'S級美咖')
        html = self.client.get(f'/my?cid={cid}').get_data(as_text=True)
        self.assertIn('查看明細', html)
        self.assertIn('data-filter="pending"', html)

    def test_portal_events_are_allowlisted_deduplicated_and_analytics_is_manager_only(self) -> None:
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = db.execute("INSERT INTO customers(name,birthday,created_at) VALUES('分析會員','1990-01-01','2026-01-01')").lastrowid
            db.commit()
        token = beauty._issue_portal_event_token(cid)
        payload = {'token': token, 'event_type': 'portal_view', 'metadata': {}}
        self.assertEqual(self.client.post('/api/member-portal/events', json=payload).status_code, 204)
        self.assertEqual(self.client.post('/api/member-portal/events', json=payload).status_code, 204)
        self.assertEqual(self.client.post('/api/member-portal/events', json={'token': token, 'event_type': 'points_filter_change', 'metadata': {'filter': 'pending'}}).status_code, 204)
        self.assertEqual(self.client.post('/api/member-portal/events', json={'token': token, 'event_type': 'bad', 'metadata': {}}).status_code, 400)
        self.assertEqual(self.client.post('/api/member-portal/events', json={'token': token, 'event_type': 'portal_view', 'metadata': {'name': '不得記錄'}}).status_code, 400)
        self.assertEqual(self.client.get('/manager/portal-analytics').status_code, 403)
        self._login(manager=True)
        page = self.client.get('/manager/portal-analytics?days=30').get_data(as_text=True)
        self.assertIn('會員自助查詢使用分析', page)
        self.assertIn('實際使用會員數', page)
        with beauty.app.app_context():
            self.assertEqual(beauty.get_db().execute('SELECT COUNT(*) FROM member_portal_events WHERE event_type="portal_view"').fetchone()[0], 1)

    def test_portal_event_rejects_bad_token_and_oversized_metadata(self) -> None:
        self.assertEqual(self.client.post('/api/member-portal/events', json={'token': 'bad', 'event_type': 'portal_view', 'metadata': {}}).status_code, 401)
        with beauty.app.app_context():
            cid = beauty.get_db().execute("INSERT INTO customers(name,birthday,created_at) VALUES('事件會員','1990-01-01','2026-01-01')").lastrowid
            beauty.get_db().commit()
        token = beauty._issue_portal_event_token(cid)
        self.assertEqual(self.client.post('/api/member-portal/events', data='x' * 600, content_type='application/json').status_code, 413)
        self.assertEqual(self.client.post('/api/member-portal/events', json={'token': token, 'event_type': 'points_filter_change', 'metadata': {'filter': 'wrong'}}).status_code, 400)

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
