from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

import app as beauty

S, P, A = "S級美咖", "P級美咖", "A級美咖"
NORMAL = "一般會員"


class UpgradeGiftEntitlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        beauty.DB_PATH = Path(self.tmp.name) / "beauty_vip.db"
        beauty.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        beauty._auth_failures.clear()
        beauty.init_db()
        self.client = beauty.app.test_client()
        with self.client.session_transaction() as s:
            s["main_authed"] = True
            s["manager_authed"] = True

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # --- helpers ---
    def _entry(self, amount: str, name: str = "禮品測試", phone: str = "0912000111"):
        return self.client.post("/entry", data={
            "store_id": "store_a", "entry_mode": "normal", "name": name, "phone": phone,
            "birthday": "1990-03-15", "amount": amount, "txn_date": date.today().isoformat(),
        })

    def _rows(self, cid: int | None = None) -> list[dict]:
        with beauty.app.app_context():
            sql = "SELECT * FROM tier_upgrades" + (" WHERE customer_id=?" if cid else "") + " ORDER BY id"
            return [dict(r) for r in beauty.get_db().execute(sql, (cid,) if cid else ())]

    def _cid(self, name: str = "禮品測試") -> int:
        with beauty.app.app_context():
            return beauty.get_db().execute("SELECT id FROM customers WHERE name=?", (name,)).fetchone()[0]

    def _seed_row(self, cid: int, tier_before: str, tier_after: str, status: str, txn: int | None = None,
                  event_type: str = "upgrade") -> int:
        with beauty.app.app_context():
            db = beauty.get_db()
            cur = db.execute(
                "INSERT INTO tier_upgrades(customer_id,upgrade_date,tier_before,tier_after,trigger_txn_id,trigger_reason,"
                "gift_name,gift_status,event_type,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (cid, "2026-01-01", tier_before, tier_after, txn, "seed", f"{tier_after} 升級禮", status, event_type,
                 "2026-01-01T00:00:00"),
            )
            db.commit()
            return cur.lastrowid

    def _make_customer(self, name: str) -> int:
        with beauty.app.app_context():
            db = beauty.get_db()
            cid = beauty.get_or_create_customer(db, name, "1990-03-15", "0933000111")
            db.commit()
            return cid

    def _gift_actions(self) -> list[dict]:
        with beauty.app.app_context():
            return [a for a in beauty.build_action_board_actions(beauty.get_db()) if a["action_type"] == "gift"]

    def _pending_count_on_page(self) -> int:
        html = self.client.get("/upgrades").get_data(as_text=True)
        marker = "⏳ 待發放 ("
        start = html.index(marker) + len(marker)
        return int(html[start:html.index(")", start)])

    # --- 1/2 跨級只一筆 ---
    def test_direct_jump_to_p_creates_single_final_tier_event(self) -> None:
        self._entry("12000")
        rows = self._rows()
        self.assertEqual([(r["tier_before"], r["tier_after"], r["gift_status"]) for r in rows], [(NORMAL, P, "pending")])
        self.assertEqual(rows[0]["gift_name"], f"{P} 升級禮")

    def test_direct_jump_to_a_creates_single_final_tier_event(self) -> None:
        self._entry("30000")
        rows = self._rows()
        self.assertEqual([(r["tier_before"], r["tier_after"], r["gift_status"]) for r in rows], [(NORMAL, A, "pending")])

    # --- 3/4 分次逐級 ---
    def test_stepwise_upgrades_each_create_their_own_entitlement(self) -> None:
        self._entry("8000")
        self._entry("12000")
        self._entry("30000")
        rows = self._rows()
        self.assertEqual([(r["tier_before"], r["tier_after"], r["gift_status"]) for r in rows],
                         [(NORMAL, S, "pending"), (S, P, "pending"), (P, A, "pending")])
        self.assertEqual(len({r["trigger_txn_id"] for r in rows}), 3)

    def test_unclaimed_lower_tier_entitlement_survives_later_upgrade(self) -> None:
        self._entry("8000")
        s_id = self._rows()[0]["id"]
        self._entry("12000")
        rows = {r["id"]: r for r in self._rows()}
        self.assertEqual(rows[s_id]["gift_status"], "pending")
        self.assertEqual(len(self._gift_actions()), 2)

    # --- 5 終身一次 ---
    def test_delivered_tier_gift_blocks_second_entitlement_after_downgrade(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, P, "delivered", txn=None)
        self._entry("12000")
        new = [r for r in self._rows(cid) if r["trigger_txn_id"]]
        self.assertEqual(len(new), 1)
        self.assertEqual(new[0]["tier_after"], P)
        self.assertEqual(new[0]["gift_status"], "skipped")
        self.assertEqual(new[0]["gift_name"], "")
        self.assertEqual(self._gift_actions(), [])

    def test_skipped_same_tier_history_does_not_block_new_entitlement(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, S, "skipped")
        self._entry("8000")
        new = [r for r in self._rows(cid) if r["trigger_txn_id"]]
        self.assertEqual([(r["tier_after"], r["gift_status"]) for r in new], [(S, "pending")])

    def test_valid_pending_same_tier_is_not_duplicated(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, S, "pending")
        self._entry("8000")
        new = [r for r in self._rows(cid) if r["trigger_txn_id"]]
        self.assertEqual([(r["tier_after"], r["gift_status"]) for r in new], [(S, "skipped")])
        self.assertEqual(len(self._gift_actions()), 1)

    def test_pending_tied_to_voided_txn_does_not_block_new_entitlement(self) -> None:
        self._entry("8000")
        with beauty.app.app_context():  # 464 型終態：交易已作廢、會員已回一般、禮仍 pending
            db = beauty.get_db()
            db.execute("UPDATE transactions SET voided_at='2026-01-02T00:00:00' WHERE id=?", (self._rows()[0]["trigger_txn_id"],))
            db.execute("UPDATE customers SET pending_tier=NULL, pending_effective_date=NULL")
            db.commit()
        self._entry("9000")
        pend = [r for r in self._rows() if r["trigger_txn_id"] != self._rows()[0]["trigger_txn_id"]]
        self.assertEqual([(r["tier_after"], r["gift_status"]) for r in pend], [(S, "pending")])
        self.assertEqual(len(self._gift_actions()), 1)

    # --- 6 同事件不重複 ---
    def test_same_trigger_txn_is_idempotent(self) -> None:
        self._entry("12000")
        row = self._rows()[0]
        with beauty.app.app_context():
            db = beauty.get_db()
            again = beauty.record_tier_upgrade_event(
                db, row["customer_id"], row["upgrade_date"], NORMAL, P, row["trigger_txn_id"], "again",
                "2026-10-24T00:00:00")
            db.commit()
        self.assertIsNone(again)
        self.assertEqual(len(self._rows()), 1)

    def test_duplicate_http_submission_creates_no_second_entitlement(self) -> None:
        self._entry("12000")
        self._entry("12000")
        self.assertEqual(len(self._rows()), 1)

    # --- 7 作廢 ---
    def test_voiding_trigger_txn_cancels_only_its_own_pending_gift(self) -> None:
        self._entry("8000")
        self._entry("12000")
        s_row, p_row = self._rows()
        self.client.post(f"/api/transactions/{p_row['trigger_txn_id']}/delete")
        by_id = {r["id"]: r for r in self._rows()}
        self.assertEqual(by_id[p_row["id"]]["gift_status"], "skipped")
        self.assertEqual(by_id[s_row["id"]]["gift_status"], "pending")
        self.assertEqual([a["title"] for a in self._gift_actions()], [f"禮品測試｜{S}升級禮"])

    def test_voiding_does_not_touch_delivered_history(self) -> None:
        self._entry("12000")
        row = self._rows()[0]
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute("UPDATE tier_upgrades SET gift_status='delivered' WHERE id=?", (row["id"],))
            db.commit()
        self.client.post(f"/api/transactions/{row['trigger_txn_id']}/delete")
        self.assertEqual({r["id"]: r["gift_status"] for r in self._rows()}[row["id"]], "delivered")

    def test_legacy_pending_on_voided_txn_is_not_valid_entitlement(self) -> None:
        self._entry("8000")
        row = self._rows()[0]
        with beauty.app.app_context():  # 模擬歷史資料：交易已作廢但禮仍 pending（會員 464 型）
            db = beauty.get_db()
            db.execute("UPDATE transactions SET voided_at='2026-01-02T00:00:00' WHERE id=?", (row["trigger_txn_id"],))
            db.commit()
        self.assertEqual(self._gift_actions(), [])
        self.assertEqual(self._pending_count_on_page(), 0)
        html = self.client.get("/upgrades?status=pending").get_data(as_text=True)
        self.assertNotIn(f"/api/upgrades/{row['id']}/deliver", html)
        self.assertEqual(self._rows()[0]["gift_status"], "pending")  # 資料本身不被讀取路徑改動

    # --- 狀態操作守衛 ---
    def test_deliver_and_reopen_are_blocked_when_same_tier_already_delivered(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, P, "delivered")
        dup_pending = self._seed_row(cid, S, P, "pending")
        dup_skipped = self._seed_row(cid, S, P, "skipped")
        self.assertEqual(self.client.post(f"/api/upgrades/{dup_pending}/deliver").status_code, 409)
        self.assertEqual(self.client.post(f"/api/upgrades/{dup_skipped}/reopen").status_code, 409)
        self.assertEqual({r["id"]: r["gift_status"] for r in self._rows()}[dup_pending], "pending")

    def test_normal_deliver_skip_reopen_still_work(self) -> None:
        self._entry("8000")
        rid = self._rows()[0]["id"]
        self.client.post(f"/api/upgrades/{rid}/deliver")
        self.assertEqual(self._rows()[0]["gift_status"], "delivered")
        self.client.post(f"/api/upgrades/{rid}/skip")
        self.client.post(f"/api/upgrades/{rid}/reopen")
        self.assertEqual(self._rows()[0]["gift_status"], "pending")

    # --- 到期重判與續會 ---
    def test_expiry_upgrade_event_uses_shared_entitlement(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, P, "delivered")
        with beauty.app.app_context():
            db = beauty.get_db()
            beauty._log_tier_events(db, cid, [{
                "event_type": "upgrade", "tier_before": S, "tier_after": P, "date": "2026-10-01", "window_total": 24000}])
            db.commit()
        last = self._rows(cid)[-1]
        self.assertEqual((last["tier_after"], last["gift_status"], last["gift_name"]), (P, "skipped", ""))

    def test_renewal_gift_is_unaffected_by_lifetime_rule(self) -> None:
        cid = self._make_customer("禮品測試")
        self._seed_row(cid, NORMAL, A, "delivered")
        with beauty.app.app_context():
            db = beauty.get_db()
            beauty._log_tier_events(db, cid, [{
                "event_type": "renewal", "tier_before": A, "tier_after": A, "date": "2026-10-01", "window_total": 60000}])
            db.commit()
        last = self._rows(cid)[-1]
        self.assertEqual((last["event_type"], last["gift_status"], last["gift_name"]), ("renewal", "pending", "A級續會禮"))

    # --- 首頁／管理頁／統計一致 ---
    def test_reminder_management_count_and_stats_agree(self) -> None:
        self._entry("12000", name="甲", phone="0911000001")
        self._entry("8000", name="乙", phone="0911000002")
        self._entry("30000", name="丙", phone="0911000003")
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute("UPDATE tier_upgrades SET gift_status='skipped' WHERE customer_id=?",
                       (db.execute("SELECT id FROM customers WHERE name='丙'").fetchone()[0],))
            db.commit()
        self.assertEqual(len(self._gift_actions()), 2)
        self.assertEqual(self._pending_count_on_page(), 2)

    def test_management_pending_section_excludes_skipped(self) -> None:
        self._entry("12000", name="待發甲", phone="0911000001")
        self._entry("8000", name="略過乙", phone="0911000002")
        with beauty.app.app_context():
            db = beauty.get_db()
            db.execute("UPDATE tier_upgrades SET gift_status='skipped' WHERE customer_id="
                       "(SELECT id FROM customers WHERE name='略過乙')")
            db.commit()
        html = self.client.get("/upgrades").get_data(as_text=True)
        pending_part = html[html.index("待處理"):html.index("已略過")]
        self.assertIn("待發甲", pending_part)
        self.assertNotIn("略過乙", pending_part)
        self.assertIn("略過乙", html[html.index("已略過"):])

    def test_delivered_stats_count_by_final_tier_only_for_new_events(self) -> None:
        self._entry("30000")
        rid = self._rows()[0]["id"]
        self.client.post(f"/api/upgrades/{rid}/deliver")
        html = self.client.get("/upgrades").get_data(as_text=True)
        self.assertIn(f"{A} <strong", html)
        self.assertNotIn(f"{S} <strong", html)
        self.assertNotIn(f"{P} <strong", html)


if __name__ == "__main__":
    unittest.main()
