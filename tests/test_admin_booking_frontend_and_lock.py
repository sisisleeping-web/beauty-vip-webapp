from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import app as beauty

TEMPLATE = Path(__file__).resolve().parent.parent / "templates" / "spa_admin.html"
NODE_HARNESS = r"""
const src = require('fs').readFileSync(process.argv[2], 'utf8');
const start = src.indexOf('function submitAddBooking');
const body = src.slice(start, src.indexOf('</script>', start));
const cases = JSON.parse(process.argv[3]);
(async () => {
  const out = [];
  for (const c of cases) {
    const log = { alerts: [], reloads: 0, fetches: 0 };
    const btn = { disabled: false };
    const form = { querySelector: () => btn };
    global.alert = m => log.alerts.push(String(m));
    global.location = { reload: () => { log.reloads++; } };
    global.FormData = class { constructor() {} };
    global.URLSearchParams = class { constructor() {} };
    global.fetch = () => { log.fetches++; return c.reject ? Promise.reject(new Error('net'))
      : Promise.resolve({ ok: c.status < 400, status: c.status,
          json: () => c.json === undefined ? Promise.reject(new SyntaxError('not json')) : Promise.resolve(c.json) }); };
    const fn = new Function(body + '; return submitAddBooking;')();
    const ev = { preventDefault() {}, target: form };
    fn(ev);
    if (c.double) fn(ev);
    await new Promise(r => setTimeout(r, 20));
    out.push({ name: c.name, ...log, btnDisabled: btn.disabled });
  }
  console.log(JSON.stringify(out));
})();
"""


@unittest.skipUnless(shutil.which("node"), "node not installed")
class SubmitAddBookingBehaviourTests(unittest.TestCase):
    def _run(self, cases: list[dict]) -> dict[str, dict]:
        proc = subprocess.run(
            ["node", "-e", NODE_HARNESS, "x", str(TEMPLATE), json.dumps(cases)],
            capture_output=True, text=True, timeout=30, check=True,
        )
        return {r["name"]: r for r in json.loads(proc.stdout)}

    def test_all_response_shapes(self) -> None:
        r = self._run([
            {"name": "html500", "status": 500},
            {"name": "json500", "status": 500, "json": {"error": "伺服器錯誤"}},
            {"name": "401", "status": 401, "json": {"error": "登入已逾時"}},
            {"name": "400", "status": 400, "json": {"error": "該時段已額滿"}},
            {"name": "409", "status": 409, "json": {"error": "衝突"}},
            {"name": "200nostatus", "status": 200, "json": {"foo": 1}},
            {"name": "200html", "status": 200},
            {"name": "ok", "status": 200, "json": {"status": "success"}},
            {"name": "net", "reject": True, "status": 0},
            {"name": "double", "status": 200, "json": {"status": "success"}, "double": True},
        ])
        for name in ("html500", "200html", "200nostatus", "net"):
            self.assertEqual(r[name]["reloads"], 0, name)
            self.assertIn("預約未建立", r[name]["alerts"][0] if name != "200nostatus" else "預約未建立")
        self.assertIn("伺服器錯誤", r["json500"]["alerts"][0])
        self.assertIn("登入已逾時", r["401"]["alerts"][0])
        self.assertIn("額滿", r["400"]["alerts"][0])
        self.assertIn("衝突", r["409"]["alerts"][0])
        for name in ("html500", "json500", "401", "400", "409", "net", "200html", "200nostatus"):
            self.assertEqual((r[name]["reloads"], r[name]["fetches"], r[name]["btnDisabled"]), (0, 1, False), name)
        self.assertEqual((r["ok"]["reloads"], r["ok"]["alerts"]), (1, ["新增預約成功！"]))
        self.assertEqual(r["double"]["fetches"], 1, "連點只能送出一次")


class LockAndAtomicityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        beauty.DB_PATH = Path(self.tmp.name) / "b.db"
        saved = {k: beauty.app.config.get(k) for k in ("TESTING", "PROPAGATE_EXCEPTIONS", "SESSION_COOKIE_SECURE")}
        self.addCleanup(beauty.app.config.update, saved)  # 還原，避免污染其他測試檔
        self.addCleanup(setattr, beauty, "DB_PATH", beauty.DB_PATH)
        beauty.app.config.update(TESTING=False, PROPAGATE_EXCEPTIONS=False, SESSION_COOKIE_SECURE=False)
        beauty._auth_failures.clear()
        beauty.init_db()
        self.client = beauty.app.test_client()
        d = next(date.today() + timedelta(days=o) for o in range(3, 12) if beauty.get_spa_slots("store_a", date.today() + timedelta(days=o)))
        self.day = d.isoformat()
        self.slot = beauty.get_spa_slots("store_a", d)[0]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _count(self) -> int:
        c = sqlite3.connect(beauty.DB_PATH)
        try:
            return c.execute("SELECT COUNT(*) FROM spa_bookings").fetchone()[0]
        finally:
            c.close()

    def _form(self, **kw: str) -> dict[str, str]:
        d = {"store_id": "store_a", "booking_date": self.day, "booking_time": self.slot, "customer_name": "測",
             "customer_phone": "0912345678", "customer_type": "new", "service_type": "body", "pax": "2"}
        d.update(kw)
        return d

    def test_companion_insert_failure_rolls_back_main_booking(self) -> None:
        c = sqlite3.connect(beauty.DB_PATH)
        c.execute("CREATE TRIGGER t_fail BEFORE INSERT ON spa_bookings WHEN NEW.customer_name LIKE '%(同行)' "
                  "BEGIN SELECT RAISE(ABORT, 'boom'); END")
        c.commit()
        c.close()
        with self.client.session_transaction() as s:
            s["main_authed"] = True
        r = self.client.post("/api/spa/admin/book", data=self._form())
        self.assertEqual(r.status_code, 500)
        self.assertTrue(r.is_json)
        self.assertNotIn("boom", r.get_data(as_text=True))
        self.assertEqual(self._count(), 0)

    def test_unauthenticated_get_pages_still_show_html_lock(self) -> None:
        for path in ("/", "/spa/admin/bookings", "/api/spa/slots?store_id=store_a&date=2030-01-01"):
            r = self.client.get(path)
            self.assertEqual(r.status_code, 200, path)
            self.assertFalse(r.is_json, path)

    def test_unauthenticated_spa_api_posts_cannot_write_and_return_json_401(self) -> None:
        for path in ("/api/spa/admin/book", "/api/spa/capacity/update", "/api/spa/bookings/1/confirm",
                     "/api/spa/bookings/1/delete", "/api/spa/bookings/1/update_time"):
            r = self.client.post(path, data=self._form())
            self.assertEqual(r.status_code, 401, path)
            self.assertTrue(r.is_json, path)
        self.assertEqual(self._count(), 0)

    def test_member_and_points_form_posts_keep_html_lock_behaviour(self) -> None:
        """會員／點數表單 POST 不受影響：逾時仍是 200 鎖頁 HTML，且不會寫入。"""
        for path in ("/api/customers/1/add_points", "/api/customers/1/update", "/api/customers/1/delete"):
            r = self.client.post(path, data={"points": "10"})
            self.assertEqual(r.status_code, 200, path)
            self.assertFalse(r.is_json, path)

    def test_public_customer_booking_endpoint_stays_open_without_login(self) -> None:
        far = next(date.today() + timedelta(days=o) for o in range(4, 28) if beauty.get_spa_slots("store_a", date.today() + timedelta(days=o)))
        slot = beauty.get_spa_slots("store_a", far)[0]
        r = self.client.post("/api/spa/book", data=self._form(booking_date=far.isoformat(), booking_time=slot, pax="1"))
        self.assertEqual(r.status_code, 200, r.data[:200])
        self.assertEqual(self._count(), 1)

    def test_500_body_does_not_leak_internals(self) -> None:
        with self.client.session_transaction() as s:
            s["main_authed"] = True
        original = beauty._process_spa_booking
        beauty._process_spa_booking = lambda **_k: (_ for _ in ()).throw(RuntimeError("secret /home/x/path"))
        try:
            r = self.client.post("/api/spa/admin/book", data=self._form())
        finally:
            beauty._process_spa_booking = original
        body = r.get_data(as_text=True)
        self.assertEqual(r.status_code, 500)
        for leak in ("Traceback", "RuntimeError", "/home/x", "secret"):
            self.assertNotIn(leak, body)


if __name__ == "__main__":
    unittest.main()
