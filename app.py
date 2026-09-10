from __future__ import annotations

import calendar
import csv
import io
import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from flask import Flask, g, redirect, render_template, request, session, url_for, send_file

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "beauty_vip.db"
RULES_PATH = BASE_DIR / "rules.json"

APP_VERSION = "1.3.0"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("APP_SECRET_KEY", "beauty-vip-demo")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.getenv("SESSION_COOKIE_SECURE", "1") != "0",
    MAX_CONTENT_LENGTH=1 * 1024 * 1024,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)
MANAGER_PIN = os.getenv("MANAGER_PIN", "1225")
MAIN_PIN = os.getenv("MAIN_PIN", "27789254")

AUTH_WINDOW_SECONDS = 300
AUTH_MAX_FAILURES = 5
_auth_failures: dict[str, list[float]] = {}
_auth_lock = threading.Lock()


def _client_key(scope: str) -> str:
    # 不信任可由客戶端偽造的 X-Forwarded-For；PythonAnywhere 會提供 remote_addr。
    address = request.remote_addr or "unknown"
    return f"{scope}:{address}"


def _auth_is_limited(scope: str) -> bool:
    now = time.monotonic()
    key = _client_key(scope)
    with _auth_lock:
        recent = [stamp for stamp in _auth_failures.get(key, []) if now - stamp < AUTH_WINDOW_SECONDS]
        if recent:
            _auth_failures[key] = recent
        else:
            _auth_failures.pop(key, None)
        return len(recent) >= AUTH_MAX_FAILURES


def _record_auth_failure(scope: str) -> None:
    key = _client_key(scope)
    with _auth_lock:
        _auth_failures.setdefault(key, []).append(time.monotonic())


def _clear_auth_failures(scope: str) -> None:
    key = _client_key(scope)
    with _auth_lock:
        _auth_failures.pop(key, None)


@app.after_request
def add_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; script-src 'self' 'unsafe-inline'; connect-src 'self'; "
        "frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
    )
    if request.is_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response

@app.before_request
def require_main_auth():
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("Origin")
        # 反向代理可能讓 Flask 看到 http、瀏覽器看到 https；比對 host 才不會誤擋正式站寫入。
        if origin and urlsplit(origin).netloc.lower() != request.host.lower():
            return {"status": "error", "message": "cross-site request rejected"}, 403
    if request.path.startswith("/static/"):
        return
    if request.path.startswith("/my"):
        return
    if request.path in ["/main_unlock", "/manager/unlock"]:
        return
    if request.path == "/spa/booking":
        return
    if request.path in ["/api/spa/availability", "/api/spa/book"]:
        return
    
    if session.get("main_authed") or session.get("manager_authed"):
        return
        
    return render_template("main_lock.html", error=None)

@app.route("/main_unlock", methods=["POST"])
def main_unlock():
    if _auth_is_limited("main"):
        return render_template("main_lock.html", error="嘗試次數過多，請五分鐘後再試。"), 429
    pin = (request.form.get("pin") or "").strip()
    if pin == MAIN_PIN:
        _clear_auth_failures("main")
        session["main_authed"] = True
        session.permanent = True
        return redirect(url_for("index"))
    _record_auth_failure("main")
    return render_template("main_lock.html", error="密碼錯誤，請再試一次。")

@app.route("/main_logout")
def main_logout():
    session.pop("main_authed", None)
    session.pop("manager_authed", None)
    return redirect(url_for("index"))

# Birthday recharge campaign plans
BIRTHDAY_RECHARGE_PLANS = [
    {"amount": 10000, "coins": 500},
    {"amount": 20000, "coins": 1000},
    {"amount": 30000, "coins": 1500},
]

# Tier hierarchy (lowest → highest)
TIER_ORDER = ["一般會員", "S級美咖", "P級美咖", "A級美咖"]


@dataclass
class TierRule:
    name: str
    monthly_threshold: float
    cashback_rate: float
    points_rate: float
    upgrade_gift: int


def load_rules() -> dict[str, Any]:
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def current_month_key(d: date) -> str:
    return d.strftime("%Y-%m")


def parse_date_or_today(value: str) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def get_spa_slots(store_id: str, booking_day: date) -> tuple[str, ...]:
    """單一後端時段政策，避免 UI 與 API 各自維護後產生繞過漏洞。"""
    weekday = booking_day.weekday()  # Monday=0, Sunday=6
    if store_id == "store_a":
        return ("09:00", "13:00", "15:00", "17:00") if weekday < 5 else ("09:00", "14:00", "16:00")
    if store_id == "store_b":
        return ("09:00", "13:00", "15:00", "17:00") if weekday < 6 else ()
    return ()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS stores (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT DEFAULT "",
            birthday TEXT NOT NULL,
            created_at TEXT NOT NULL,
            coin_balance INTEGER NOT NULL DEFAULT 0,
            UNIQUE(name, birthday)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            store_id TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            month_key TEXT NOT NULL,
            amount REAL NOT NULL,
            birthday_discount_applied INTEGER NOT NULL DEFAULT 0,
            final_amount REAL NOT NULL,
            cashback REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            coins_earned INTEGER NOT NULL DEFAULT 0,
            coins_redeemed INTEGER NOT NULL DEFAULT 0,
            recharge_plan TEXT DEFAULT NULL,
            recharge_amount REAL DEFAULT NULL,
            entry_mode TEXT NOT NULL DEFAULT 'normal',
            note TEXT DEFAULT '',
            FOREIGN KEY(customer_id) REFERENCES customers(id),
            FOREIGN KEY(store_id) REFERENCES stores(id)
        );

        CREATE TABLE IF NOT EXISTS review_flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT NOT NULL,
            item_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'unreviewed',
            note TEXT DEFAULT '',
            updated_at TEXT NOT NULL,
            UNIQUE(item_type, item_key)
        );

        CREATE TABLE IF NOT EXISTS tier_upgrades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            upgrade_date TEXT NOT NULL,
            tier_before TEXT NOT NULL,
            tier_after TEXT NOT NULL,
            trigger_txn_id INTEGER,
            trigger_reason TEXT NOT NULL DEFAULT '',
            gift_name TEXT NOT NULL DEFAULT '',
            gift_status TEXT NOT NULL DEFAULT 'pending',
            gift_delivered_at TEXT,
            gift_delivered_by TEXT,
            note TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );

        CREATE TABLE IF NOT EXISTS spa_bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id TEXT NOT NULL,
            booking_date TEXT NOT NULL,
            booking_time TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            customer_phone TEXT NOT NULL,
            customer_type TEXT NOT NULL,
            service_type TEXT NOT NULL,
            note TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS spa_capacity_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id TEXT NOT NULL,
            override_date TEXT NOT NULL,
            override_time TEXT NOT NULL,
            capacity INTEGER NOT NULL DEFAULT 2,
            UNIQUE(store_id, override_date, override_time)
        );

        CREATE TABLE IF NOT EXISTS point_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            points INTEGER NOT NULL,
            reason TEXT NOT NULL,
            operator TEXT NOT NULL DEFAULT 'staff',
            created_at TEXT NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );

        CREATE TABLE IF NOT EXISTS coin_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            source_txn_id INTEGER,
            source_adjustment_id INTEGER,
            earned_amount INTEGER NOT NULL,
            remaining_amount INTEGER NOT NULL,
            credit_date TEXT NOT NULL,
            expires_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            is_legacy INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );

        CREATE TABLE IF NOT EXISTS coin_redemptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            redeem_txn_id INTEGER NOT NULL,
            batch_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES coin_batches(id)
        );

        CREATE TABLE IF NOT EXISTS customer_operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            customer_id INTEGER NOT NULL,
            related_customer_id INTEGER,
            reason TEXT NOT NULL,
            before_json TEXT NOT NULL DEFAULT '{}',
            after_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS customer_merge_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keep_customer_id INTEGER NOT NULL,
            absorbed_customer_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            keep_before_json TEXT NOT NULL,
            absorbed_before_json TEXT NOT NULL,
            moved_ids_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'merged',
            created_at TEXT NOT NULL,
            reverted_at TEXT
        );
        """
    )

    # Migrate existing DBs
    for table, col, typedef in [
        ("customers", "coin_balance", "INTEGER NOT NULL DEFAULT 0"),
        ("transactions", "coins_earned", "INTEGER NOT NULL DEFAULT 0"),
        ("transactions", "coins_redeemed", "INTEGER NOT NULL DEFAULT 0"),
        ("transactions", "recharge_plan", "TEXT DEFAULT NULL"),
        ("transactions", "recharge_amount", "REAL DEFAULT NULL"),
        ("transactions", "entry_mode", "TEXT NOT NULL DEFAULT 'normal'"),
        ("transactions", "note", "TEXT DEFAULT ''"),
        # 2026美咖會員制度V3 Phase 0：交易改軟刪除（作廢保留稽核紀錄），不再硬刪除。
        ("transactions", "voided_at", "TEXT DEFAULT NULL"),
        ("transactions", "void_reason", "TEXT DEFAULT ''"),
        ("transactions", "voided_by", "TEXT DEFAULT ''"),
        # 2026美咖會員制度V3 Phase 1：會員年度制（S/P/A 效期自正式升等日起一年，到期重判）。
        ("customers", "member_tier", "TEXT NOT NULL DEFAULT '一般會員'"),
        ("customers", "tier_effective_date", "TEXT DEFAULT NULL"),
        ("customers", "tier_expires_date", "TEXT DEFAULT NULL"),
        ("customers", "pending_tier", "TEXT DEFAULT NULL"),
        ("customers", "pending_effective_date", "TEXT DEFAULT NULL"),
        # 合併不硬刪來源顧客，保留可撤銷的資料鏈。
        ("customers", "merged_into_customer_id", "INTEGER DEFAULT NULL"),
        ("customers", "merged_at", "TEXT DEFAULT NULL"),
        # 2026美咖會員制度V3 Phase 2：退款回溯完整化，tier_upgrades 也記錄非「達標升等」的事件。
        ("tier_upgrades", "event_type", "TEXT NOT NULL DEFAULT 'upgrade'"),
    ]:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    stores = [("store_a", "斗六店"), ("store_b", "虎尾店")]
    cur.executemany("INSERT OR IGNORE INTO stores(id, name) VALUES(?, ?)", stores)
    cur.executemany("UPDATE stores SET name=? WHERE id=?", [("斗六店", "store_a"), ("虎尾店", "store_b")])

    conn.commit()
    conn.close()


def is_birthday_month(birthday_str: str, txn_day: date) -> bool:
    try:
        bday = datetime.strptime(birthday_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    return bday.month == txn_day.month


def birthday_discount_used_this_month(db: sqlite3.Connection, customer_id: int, month_key: str) -> bool:
    row = db.execute(
        "SELECT COUNT(*) AS cnt FROM transactions WHERE customer_id=? AND month_key=? AND birthday_discount_applied=1",
        (customer_id, month_key),
    ).fetchone()
    return int(row["cnt"]) > 0


def calc_tier(single_amount: float, annual_amount: float, rules: dict[str, Any]) -> TierRule:
    """Determine VIP tier based on PAST transactions only.
    - single_amount: best single-txn ever BEFORE this txn
    - annual_amount: year-to-date spend BEFORE this txn
    Tier upgrade takes effect from the NEXT transaction, not the current one.
    """
    tier_name = "一般會員"
    if single_amount >= 30000 or annual_amount >= 60000:
        tier_name = "A級美咖"
    elif single_amount >= 12000 or annual_amount >= 24000:
        tier_name = "P級美咖"
    elif single_amount >= 8000 or annual_amount >= 15000:
        tier_name = "S級美咖"

    for t in rules["vip_tiers"]:
        if t["name"] == tier_name:
            return TierRule(
                name=t["name"],
                monthly_threshold=0,
                cashback_rate=t["cashback_rate"],
                points_rate=t["points_rate"],
                upgrade_gift=t["upgrade_gift"]
            )
    return TierRule("一般會員", 0, 0, 0, 0)


# ── 2026美咖會員制度V3 Phase 1：會員年度制 ──────────────────────────────────

def _add_days(date_str: str, n: int) -> str:
    return (date.fromisoformat(date_str) + timedelta(days=n)).isoformat()


def _add_years(date_str: str, n: int) -> str:
    d = date.fromisoformat(date_str)
    try:
        return d.replace(year=d.year + n).isoformat()
    except ValueError:
        # 閏年 2/29 在非閏年目標年份不存在，退到 2/28
        return d.replace(year=d.year + n, day=28).isoformat()


def _tier_rule_by_name(tier_name: str, rules: dict[str, Any]) -> TierRule:
    for t in rules["vip_tiers"]:
        if t["name"] == tier_name:
            return TierRule(
                name=t["name"], monthly_threshold=0,
                cashback_rate=t["cashback_rate"], points_rate=t["points_rate"],
                upgrade_gift=t["upgrade_gift"],
            )
    return TierRule("一般會員", 0, 0, 0, 0)


def _tier_from_window_total(window_total: float) -> str:
    """會員年度效期屆滿的重新判定（V3 §6）：只看該年度累計，沒有單筆門檻這個選項。"""
    if window_total >= 60000:
        return "A級美咖"
    if window_total >= 24000:
        return "P級美咖"
    if window_total >= 15000:
        return "S級美咖"
    return "一般會員"


def _window_total(db: sqlite3.Connection, customer_id: int, window_start: str, window_end: str) -> float:
    row = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) AS total FROM transactions "
        "WHERE customer_id=? AND txn_date>=? AND txn_date<? AND final_amount>=1000 AND voided_at IS NULL",
        (customer_id, window_start, window_end),
    ).fetchone()
    return float(row["total"] or 0)


def _is_backdated_entry(db: sqlite3.Connection, customer_id: int, txn_day: date) -> bool:
    """這筆交易日期是否早於顧客現有交易紀錄——店員補登過去單子時，不能讓等級時間軸倒著跑。"""
    row = db.execute(
        "SELECT MAX(txn_date) AS latest FROM transactions WHERE customer_id=? AND voided_at IS NULL",
        (customer_id,),
    ).fetchone()
    latest = row["latest"] if row else None
    return bool(latest) and txn_day.isoformat() < latest


def _project_tier_state(db: sqlite3.Connection, customer_id: int, as_of: str) -> tuple[dict, list[dict]] | tuple[None, list]:
    """純讀取：算出 as_of 這天套用「pending 升等生效」與「會員年度到期重判」後的等級狀態，不寫入。
    第二個回傳值是效期重判觸發的事件清單（renewal/downgrade/reset），呼叫端要不要拿來寫
    tier_upgrades 稽核紀錄／觸發 Phase 4 續會禮，由呼叫端決定（get_effective_tier 就直接丟棄）。"""
    row = db.execute(
        "SELECT member_tier, tier_effective_date, tier_expires_date, pending_tier, pending_effective_date "
        "FROM customers WHERE id=?",
        (customer_id,),
    ).fetchone()
    if not row:
        return None, []
    state = dict(row)
    events: list[dict] = []

    # 套用已到期的 pending 升等
    if state["pending_tier"] and as_of >= state["pending_effective_date"]:
        state["member_tier"] = state["pending_tier"]
        state["tier_effective_date"] = state["pending_effective_date"]
        state["tier_expires_date"] = _add_years(state["pending_effective_date"], 1)
        state["pending_tier"] = None
        state["pending_effective_date"] = None

    # 會員年度到期重判（只有 S/P/A 有效期；最多跑5輪避免極端案例卡住）
    iterations = 0
    while (
        state["member_tier"] != "一般會員"
        and state["tier_expires_date"]
        and as_of > state["tier_expires_date"]
        and iterations < 5
    ):
        old_tier = state["member_tier"]
        window_total = _window_total(db, customer_id, state["tier_effective_date"], state["tier_expires_date"])
        new_tier = _tier_from_window_total(window_total)
        event_date = state["tier_expires_date"]

        if new_tier == "一般會員":
            state["member_tier"] = "一般會員"
            state["tier_effective_date"] = None
            state["tier_expires_date"] = None
            event_type = "reset"
        else:
            state["member_tier"] = new_tier
            state["tier_effective_date"] = state["tier_expires_date"]  # 新一期接續上一期到期日
            state["tier_expires_date"] = _add_years(state["tier_effective_date"], 1)
            idx_old = TIER_ORDER.index(old_tier) if old_tier in TIER_ORDER else 0
            idx_new = TIER_ORDER.index(new_tier) if new_tier in TIER_ORDER else 0
            if idx_new > idx_old:
                event_type = "upgrade"  # 效期屆滿重判「升到更高等級」跟「維持原等級續會」是兩回事，
                # 只有真正同級續會才算 renewal（V3.1：只有 A→A 才是 A 級續會，P→A/S→A 是升等）
            elif idx_new == idx_old:
                event_type = "renewal"
            else:
                event_type = "downgrade"

        events.append({
            "date": event_date, "tier_before": old_tier, "tier_after": new_tier,
            "window_total": window_total, "event_type": event_type,
        })
        iterations += 1

    return state, events


def _persist_tier_state(db: sqlite3.Connection, customer_id: int, state: dict) -> None:
    db.execute(
        "UPDATE customers SET member_tier=?, tier_effective_date=?, tier_expires_date=?, "
        "pending_tier=?, pending_effective_date=? WHERE id=?",
        (
            state["member_tier"], state["tier_effective_date"], state["tier_expires_date"],
            state["pending_tier"], state["pending_effective_date"], customer_id,
        ),
    )


def get_effective_tier(db: sqlite3.Connection, customer_id: int, as_of: date | None = None) -> str:
    """給其他頁面（report/my/contacts）顯示用：只算不寫，顧客沒來店不會被寫入變動，
    但顯示的等級仍然是「如果現在重新判定」會得到的正確結果。"""
    as_of_str = (as_of or date.today()).isoformat()
    state, _events = _project_tier_state(db, customer_id, as_of_str)
    return state["member_tier"] if state else "一般會員"


def get_effective_tier_state(db: sqlite3.Connection, customer_id: int, as_of: date | None = None) -> dict:
    """跟 get_effective_tier 一樣只算不寫，但連會員年度起訖日一起回傳——
    給通訊名單／我的頁面顯示「升等日期」「到期日」用。一般會員兩個日期都是 None。"""
    as_of_str = (as_of or date.today()).isoformat()
    state, _events = _project_tier_state(db, customer_id, as_of_str)
    return {
        "tier": state["member_tier"] if state else "一般會員",
        "tier_effective_date": state["tier_effective_date"] if state else None,
        "tier_expires_date": state["tier_expires_date"] if state else None,
    }


_EMPTY_TIER_STATE = {
    "member_tier": "一般會員", "tier_effective_date": None, "tier_expires_date": None,
    "pending_tier": None, "pending_effective_date": None,
}


def _log_tier_events(db: sqlite3.Connection, customer_id: int, events: list[dict]) -> None:
    """把會員年度到期重判觸發的事件寫入 tier_upgrades（V3 §15.7 稽核要求）。
    A級續會禮（1,500點）只給「A→A」真正續會（V3.1 §6 明確定義：P→A、S→A 是升等不是續會）；
    重判後等級真的比之前高（upgrade）比照一般升等禮，發對應等級的標準升級禮（V3.1 §5：
    第一次達到 S/P/A 級都算，不限於透過單筆/年度消費達標，效期屆滿重判達到也算）。
    比照現有升等禮 workflow 只記錄 pending，實際點數入帳仍是店家用手動贈點處理，不引進
    新的自動入帳行為。其他情況（維持S/P、降級、回一般會員）沒有禮，直接記 skipped，
    純稽核留痕。"""
    now_str = datetime.now().isoformat(timespec="seconds")
    for e in events:
        if e["event_type"] == "renewal" and e["tier_after"] == "A級美咖":
            gift_name, gift_status = "A級續會禮", "pending"
            reason = f"會員年度效期屆滿，續會達 A級美咖（本期年度累計 {int(e['window_total']):,} 元）"
        elif e["event_type"] == "renewal":
            gift_name, gift_status = "", "skipped"
            reason = f"會員年度效期屆滿，續會維持 {e['tier_after']}（本期年度累計 {int(e['window_total']):,} 元）"
        elif e["event_type"] == "upgrade":
            gift_name, gift_status = f"{e['tier_after']} 升級禮", "pending"
            reason = (
                f"會員年度效期屆滿重新判定，由 {e['tier_before']} 升為 {e['tier_after']}"
                f"（本期年度累計 {int(e['window_total']):,} 元）"
            )
        elif e["event_type"] == "downgrade":
            gift_name, gift_status = "", "skipped"
            reason = f"會員年度效期屆滿，依累計降為 {e['tier_after']}（本期年度累計 {int(e['window_total']):,} 元）"
        else:  # reset
            gift_name, gift_status = "", "skipped"
            reason = f"會員年度效期屆滿，累計未達門檻降回一般會員（本期年度累計 {int(e['window_total']):,} 元）"
        db.execute(
            """
            INSERT INTO tier_upgrades(
                customer_id, upgrade_date, tier_before, tier_after,
                trigger_txn_id, trigger_reason, gift_name,
                gift_status, event_type, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                customer_id, e["date"], e["tier_before"], e["tier_after"],
                None, reason, gift_name, gift_status, e["event_type"], now_str,
            ),
        )


def reevaluate_and_persist_tier(db: sqlite3.Connection, customer_id: int, as_of: date) -> dict:
    """entry() 記一筆之前、或管理後台手動批次重新評估時呼叫：把最新狀態寫回 customers，
    並把效期重判事件記進 tier_upgrades，回傳完整狀態。"""
    state, events = _project_tier_state(db, customer_id, as_of.isoformat())
    if state:
        _persist_tier_state(db, customer_id, state)
        _log_tier_events(db, customer_id, events)
    return state or dict(_EMPTY_TIER_STATE)


def _history_tier_state_for_customers(db: sqlite3.Connection, customer_ids: list[int], as_of: date) -> dict:
    """Derive the strongest tier now supported by the merged customer's current-year history.

    A merge can join two partial customer histories after their transactions were entered.
    Unlike normal entry, there is no new transaction to schedule the pending upgrade, so a
    threshold already crossed in the past must be materialised as effective on the next day.
    """
    if not customer_ids:
        return dict(_EMPTY_TIER_STATE)
    marks = ",".join("?" * len(customer_ids))
    rows = db.execute(
        f"SELECT txn_date, final_amount FROM transactions "
        f"WHERE customer_id IN ({marks}) AND entry_mode='normal' AND final_amount>=1000 "
        "AND voided_at IS NULL AND txn_date<=? ORDER BY txn_date ASC, id ASC",
        [*customer_ids, as_of.isoformat()],
    ).fetchall()
    running_max = 0.0
    totals_by_year: dict[str, float] = {}
    best_name = "一般會員"
    best_effective: str | None = None
    best_rank = 0

    for row in rows:
        amount = float(row["final_amount"])
        year = row["txn_date"][:4]
        running_max = max(running_max, amount)
        totals_by_year[year] = totals_by_year.get(year, 0.0) + amount
        candidate = calc_tier(running_max, totals_by_year[year], load_rules()).name
        candidate_rank = TIER_ORDER.index(candidate)
        if candidate_rank > best_rank:
            best_name = candidate
            best_rank = candidate_rank
            best_effective = _add_days(row["txn_date"], 1)

    if best_name == "一般會員" or not best_effective:
        return dict(_EMPTY_TIER_STATE)
    return {
        "member_tier": best_name,
        "tier_effective_date": best_effective,
        "tier_expires_date": _add_years(best_effective, 1),
        "pending_tier": None,
        "pending_effective_date": None,
    }


def _merged_history_tier_state(db: sqlite3.Connection, customer_id: int, as_of: date) -> dict:
    return _history_tier_state_for_customers(db, [customer_id], as_of)


def _reconcile_tier_after_merge(
    db: sqlite3.Connection, customer_id: int, pre_merge_states: list[dict], as_of: date,
) -> dict:
    """Keep the strongest valid existing membership, or restore one proved by merged history."""
    candidates = [s for s in pre_merge_states if s]
    candidates.append(_merged_history_tier_state(db, customer_id, as_of))

    def sort_key(state: dict) -> tuple[int, str, str]:
        tier = state.get("member_tier", "一般會員")
        rank = TIER_ORDER.index(tier) if tier in TIER_ORDER else 0
        return rank, state.get("tier_expires_date") or "", state.get("tier_effective_date") or ""

    selected = max(candidates, key=sort_key, default=dict(_EMPTY_TIER_STATE))
    _persist_tier_state(db, customer_id, selected)
    return selected


def reevaluate_tier_after_void(
    db: sqlite3.Connection, customer_id: int, rules: dict[str, Any], as_of: date,
    trigger_txn_id: int, void_reason: str,
) -> None:
    """V3 Phase 2：作廢一筆 normal 交易後，用扣除該筆之後的累計重新判定等級——
    可能造成即時降級（跟 Phase 1 的到期重判是兩回事，這個是退款觸發，不等窗口到期）。
    降級會寫入 tier_upgrades（event_type='downgrade'）留稽核紀錄，但不擋退款本身。"""
    state = reevaluate_and_persist_tier(db, customer_id, as_of)  # 先正常化狀態（套用到期的pending等）
    current_tier_name = state["member_tier"]

    new_max_single = get_past_max_single(db, customer_id)
    # 升等門檻的「年度累計」是曆年累計，不是目前會員效期開始後的累計。
    # 先前使用 member_tier 的效期窗口，會在作廢重複交易時遺失會員升等前
    # 已累積的消費，讓仍符合年度門檻的會員被錯誤降級。
    annual_total = customer_year_total(db, customer_id, as_of.strftime("%Y"))
    recomputed = calc_tier(new_max_single, annual_total, rules)

    idx_current = TIER_ORDER.index(current_tier_name) if current_tier_name in TIER_ORDER else 0
    idx_recomputed = TIER_ORDER.index(recomputed.name) if recomputed.name in TIER_ORDER else 0
    if idx_recomputed >= idx_current:
        return  # 扣除這筆之後，累計仍然支撐得起現有等級，不用動

    now_str = datetime.now().isoformat(timespec="seconds")
    reason = f"作廢交易 id={trigger_txn_id} 後年度累計降為 {int(annual_total):,} 元" + (
        f"（{void_reason}）" if void_reason else ""
    )
    for step in range(idx_current, idx_recomputed, -1):
        step_from = TIER_ORDER[step]
        step_to = TIER_ORDER[step - 1]
        db.execute(
            """
            INSERT INTO tier_upgrades(
                customer_id, upgrade_date, tier_before, tier_after,
                trigger_txn_id, trigger_reason, gift_name,
                gift_status, event_type, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                customer_id, as_of.isoformat(),
                step_from, step_to,
                trigger_txn_id, reason, "",
                "skipped", "downgrade", now_str,
            ),
        )

    if recomputed.name == "一般會員":
        db.execute(
            "UPDATE customers SET member_tier=?, tier_effective_date=NULL, tier_expires_date=NULL, "
            "pending_tier=NULL, pending_effective_date=NULL WHERE id=?",
            (recomputed.name, customer_id),
        )
    else:
        new_expires = _add_years(as_of.isoformat(), 1)
        db.execute(
            "UPDATE customers SET member_tier=?, tier_effective_date=?, tier_expires_date=?, "
            "pending_tier=NULL, pending_effective_date=NULL WHERE id=?",
            (recomputed.name, as_of.isoformat(), new_expires, customer_id),
        )


def get_or_create_customer(db: sqlite3.Connection, name: str, birthday: str, phone: str = "") -> int:
    row = db.execute(
        "SELECT id FROM customers WHERE name=? AND birthday=? AND merged_into_customer_id IS NULL", (name, birthday)
    ).fetchone()
    if row:
        if phone:
            db.execute("UPDATE customers SET phone=? WHERE id=?", (phone, row["id"]))
        return int(row["id"])
    now = datetime.now().isoformat(timespec="seconds")
    cur = db.execute(
        "INSERT INTO customers(name, phone, birthday, created_at) VALUES(?,?,?,?)",
        (name, phone, birthday, now),
    )
    return int(cur.lastrowid)


def customer_year_total(db: sqlite3.Connection, customer_id: int, year_str: str) -> float:
    # 單筆未滿 NT$1,000 不列入會員年度累計（V3 §8.2），但該筆交易仍正常建檔、正常賺美咖幣。
    row = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) AS total FROM transactions "
        "WHERE customer_id=? AND substr(txn_date,1,4)=? AND final_amount>=1000 AND voided_at IS NULL",
        (customer_id, year_str),
    ).fetchone()
    return float(row["total"] or 0)


def get_past_max_single(db: sqlite3.Connection, customer_id: int) -> float:
    row = db.execute(
        "SELECT COALESCE(MAX(final_amount),0) AS max_amt FROM transactions WHERE customer_id=? AND voided_at IS NULL",
        (customer_id,),
    ).fetchone()
    return float(row["max_amt"] or 0)


def customer_month_total(db: sqlite3.Connection, customer_id: int, month_key: str) -> float:
    row = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) AS total FROM transactions WHERE customer_id=? AND month_key=? AND voided_at IS NULL",
        (customer_id, month_key),
    ).fetchone()
    return float(row["total"] or 0)


def get_customer_coin_balance(db: sqlite3.Connection, customer_id: int) -> int:
    row = db.execute("SELECT coin_balance FROM customers WHERE id=?", (customer_id,)).fetchone()
    return int(row["coin_balance"] or 0) if row else 0


# ── 2026美咖會員制度V3 Phase 3：美咖幣分筆效期帳本 ──────────────────────────

def _add_one_month(d: date) -> date:
    if d.month == 12:
        y, m = d.year + 1, 1
    else:
        y, m = d.year, d.month + 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last_day))


def create_coin_batch(
    db: sqlite3.Connection, customer_id: int, amount: int, txn_day: date,
    source_txn_id: int | None = None, source_adjustment_id: int | None = None,
    immediate: bool = False,
) -> None:
    """建立一筆點數批次。immediate=True 用於手動贈點（立即可用）；一般消費/壽星充值
    賺的點數依 V3 §7.4 次月才入帳可用（credit_date = txn_day + 1個月）。"""
    if amount <= 0:
        return
    credit_date = txn_day if immediate else _add_one_month(txn_day)
    expires_date = _add_years(credit_date.isoformat(), 1)
    db.execute(
        "INSERT INTO coin_batches(customer_id, source_txn_id, source_adjustment_id, earned_amount, "
        "remaining_amount, credit_date, expires_date, status, is_legacy, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            customer_id, source_txn_id, source_adjustment_id, amount, amount,
            credit_date.isoformat(), expires_date, "active", 0,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )


def get_usable_coin_total(db: sqlite3.Connection, customer_id: int, as_of: date | None = None) -> int:
    as_of_str = (as_of or date.today()).isoformat()
    row = db.execute(
        "SELECT COALESCE(SUM(remaining_amount),0) AS total FROM coin_batches "
        "WHERE customer_id=? AND status='active' AND credit_date<=? AND expires_date>=?",
        (customer_id, as_of_str, as_of_str),
    ).fetchone()
    return int(row["total"] or 0)


def redeem_coins_fifo(
    db: sqlite3.Connection, customer_id: int, amount: int, redeem_txn_id: int, as_of: date | None = None,
) -> bool:
    """依 credit_date（舊到新）FIFO 扣點。點數不足回傳 False、不做任何寫入。"""
    if amount <= 0:
        return True
    as_of_str = (as_of or date.today()).isoformat()
    usable = db.execute(
        "SELECT id, remaining_amount FROM coin_batches WHERE customer_id=? AND status='active' "
        "AND credit_date<=? AND expires_date>=? AND remaining_amount>0 ORDER BY credit_date, id",
        (customer_id, as_of_str, as_of_str),
    ).fetchall()
    if sum(int(b["remaining_amount"]) for b in usable) < amount:
        return False

    remaining_to_deduct = amount
    now_str = datetime.now().isoformat(timespec="seconds")
    for b in usable:
        if remaining_to_deduct <= 0:
            break
        take = min(remaining_to_deduct, int(b["remaining_amount"]))
        db.execute("UPDATE coin_batches SET remaining_amount = remaining_amount - ? WHERE id=?", (take, b["id"]))
        db.execute(
            "INSERT INTO coin_redemptions(redeem_txn_id, batch_id, amount, created_at) VALUES(?,?,?,?)",
            (redeem_txn_id, b["id"], take, now_str),
        )
        remaining_to_deduct -= take
    return True


def reverse_redemption(db: sqlite3.Connection, redeem_txn_id: int) -> None:
    """把某筆扣點交易動用過的批次還原（即使批次現在已過期也要還——這些點數
    原本就會過期，行為正確）。作廢/調整扣點交易時呼叫。"""
    rows = db.execute(
        "SELECT id, batch_id, amount FROM coin_redemptions WHERE redeem_txn_id=?", (redeem_txn_id,)
    ).fetchall()
    for r in rows:
        db.execute(
            "UPDATE coin_batches SET remaining_amount = remaining_amount + ? WHERE id=?",
            (r["amount"], r["batch_id"]),
        )
        db.execute("DELETE FROM coin_redemptions WHERE id=?", (r["id"],))


def reverse_earning(
    db: sqlite3.Connection, customer_id: int,
    source_txn_id: int | None = None, source_adjustment_id: int | None = None,
) -> None:
    """把某筆賺點交易/手動贈點對應的批次作廢。完全沒被動用過才自動作廢；已被部分
    消費就不自動扣回，寫 review_flags 交人工複核（V3 §18.4 提案）。"""
    if source_txn_id is not None:
        where_clause, key = "source_txn_id=?", source_txn_id
    elif source_adjustment_id is not None:
        where_clause, key = "source_adjustment_id=?", source_adjustment_id
    else:
        return
    batches = db.execute(
        f"SELECT id, earned_amount, remaining_amount FROM coin_batches WHERE {where_clause} AND status='active'",
        (key,),
    ).fetchall()
    for b in batches:
        if int(b["remaining_amount"]) == int(b["earned_amount"]):
            db.execute("UPDATE coin_batches SET status='voided' WHERE id=?", (b["id"],))
        else:
            db.execute(
                "INSERT INTO review_flags(item_type, item_key, status, note, updated_at) VALUES(?,?,?,?,?)",
                (
                    "coin_batch_partial_use_on_void",
                    f"batch{b['id']}_{datetime.now().timestamp()}",
                    "unreviewed",
                    f"點數批次 id={b['id']}（來源交易/贈點 id={key}）已被部分使用"
                    f"（剩餘{b['remaining_amount']}/原{b['earned_amount']}），無法自動扣回，請人工複核",
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )


def adjust_coin_batch_for_edit(
    db: sqlite3.Connection, source_txn_id: int, new_earned_amount: int, customer_id: int, txn_day: date,
) -> None:
    """交易被編輯（金額/日期變動）後調整對應點數批次。批次完全沒被動用過才直接調整
    金額與 credit_date／expires_date；已被部分消費就不自動改，寫 review_flags。
    原本沒有批次但新金額 > 0 時，視同新賺一筆（適用於原本 0 元改成有金額的情況）。"""
    batch = db.execute(
        "SELECT id, earned_amount, remaining_amount FROM coin_batches WHERE source_txn_id=? AND status='active'",
        (source_txn_id,),
    ).fetchone()
    if not batch:
        if new_earned_amount > 0:
            create_coin_batch(db, customer_id, new_earned_amount, txn_day, source_txn_id=source_txn_id)
        return
    if int(batch["remaining_amount"]) != int(batch["earned_amount"]):
        db.execute(
            "INSERT INTO review_flags(item_type, item_key, status, note, updated_at) VALUES(?,?,?,?,?)",
            (
                "coin_batch_partial_use_on_edit",
                f"batch{batch['id']}_txn{source_txn_id}_{datetime.now().timestamp()}",
                "unreviewed",
                f"交易 id={source_txn_id} 被編輯，但對應點數批次 id={batch['id']} 已被部分使用"
                f"（剩餘{batch['remaining_amount']}/原{batch['earned_amount']}），沒有自動調整批次金額，請人工複核",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        return
    if new_earned_amount <= 0:
        db.execute("UPDATE coin_batches SET status='voided' WHERE id=?", (batch["id"],))
    else:
        credit_date = _add_one_month(txn_day)
        expires_date = _add_years(credit_date.isoformat(), 1)
        db.execute(
            "UPDATE coin_batches SET earned_amount=?, remaining_amount=?, credit_date=?, expires_date=? WHERE id=?",
            (new_earned_amount, new_earned_amount, credit_date.isoformat(), expires_date, batch["id"]),
        )


def adjust_coin_batch_for_adjustment_edit(
    db: sqlite3.Connection, source_adjustment_id: int, new_points: int, customer_id: int,
) -> None:
    """手動贈點紀錄被編輯後調整對應批次，邏輯同 adjust_coin_batch_for_edit，
    但贈點批次是立即入帳，沒有 credit_date 要跟著改的問題。"""
    batch = db.execute(
        "SELECT id, earned_amount, remaining_amount FROM coin_batches "
        "WHERE source_adjustment_id=? AND status='active'",
        (source_adjustment_id,),
    ).fetchone()
    if not batch:
        if new_points > 0:
            create_coin_batch(
                db, customer_id, new_points, date.today(),
                source_adjustment_id=source_adjustment_id, immediate=True,
            )
        return
    if int(batch["remaining_amount"]) != int(batch["earned_amount"]):
        db.execute(
            "INSERT INTO review_flags(item_type, item_key, status, note, updated_at) VALUES(?,?,?,?,?)",
            (
                "coin_batch_partial_use_on_edit",
                f"batch{batch['id']}_adj{source_adjustment_id}_{datetime.now().timestamp()}",
                "unreviewed",
                f"贈點紀錄 id={source_adjustment_id} 被編輯，但對應點數批次 id={batch['id']} 已被部分使用"
                f"（剩餘{batch['remaining_amount']}/原{batch['earned_amount']}），沒有自動調整批次金額，請人工複核",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        return
    if new_points <= 0:
        db.execute("UPDATE coin_batches SET status='voided' WHERE id=?", (batch["id"],))
    else:
        db.execute(
            "UPDATE coin_batches SET earned_amount=?, remaining_amount=? WHERE id=?",
            (new_points, new_points, batch["id"]),
        )


def sync_coin_balance(db: sqlite3.Connection, customer_id: int, as_of: date | None = None) -> int:
    balance = get_usable_coin_total(db, customer_id, as_of)
    db.execute("UPDATE customers SET coin_balance=? WHERE id=?", (balance, customer_id))
    return balance


def sync_all_coin_balances(db: sqlite3.Connection, as_of: date | None = None) -> None:
    """效期是時間到了自動失效、不是靠事件觸發，manager/contacts/report 這些直接讀
    customers.coin_balance 欄位顯示的頁面，渲染前要先跑這個重新整理。"""
    as_of_str = (as_of or date.today()).isoformat()
    db.execute(
        """
        UPDATE customers AS c SET coin_balance = (
            SELECT COALESCE(SUM(remaining_amount),0) FROM coin_batches
            WHERE coin_batches.customer_id = c.id
            AND status='active' AND credit_date<=? AND expires_date>=?
        )
        WHERE c.coin_balance != COALESCE((
            SELECT SUM(remaining_amount) FROM coin_batches
            WHERE coin_batches.customer_id = c.id
            AND status='active' AND credit_date<=? AND expires_date>=?
        ), 0)
        """,
        (as_of_str, as_of_str, as_of_str, as_of_str),
    )


@app.before_request
def refresh_management_coin_balance_cache():
    """Keep the display cache aligned with the dated coin-batch ledger.

    Coin batches can become usable or expire merely because the calendar changes.
    The batch ledger is canonical, while ``customers.coin_balance`` is a display
    cache used by several management APIs.  Refresh the cache once at the start
    of each application request so the first request after a calendar change
    repairs stale values before any handler can return them.
    """
    if request.path.startswith("/static/"):
        return
    db = get_db()
    sync_all_coin_balances(db)
    db.commit()


def has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(r[1]) == column for r in rows)
    except Exception:
        return False


def _confirmed_reason() -> str | None:
    """Return the declared reason only for deliberately confirmed high-risk operations."""
    reason = request.form.get("reason", "").strip()
    if request.form.get("confirmed") != "1":
        return None
    return reason if len(reason) >= 2 else None


def _log_customer_operation(
    db: sqlite3.Connection, action: str, customer_id: int, reason: str,
    before: dict | None = None, after: dict | None = None, related_customer_id: int | None = None,
) -> None:
    db.execute(
        "INSERT INTO customer_operation_log(action,customer_id,related_customer_id,reason,before_json,after_json,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (action, customer_id, related_customer_id, reason, json.dumps(before or {}, ensure_ascii=False),
         json.dumps(after or {}, ensure_ascii=False), datetime.now().isoformat(timespec="seconds")),
    )


def _customer_merge_summary(db: sqlite3.Connection, customer_id: int) -> dict | None:
    row = db.execute(
        "SELECT id,name,phone,birthday,member_tier,tier_effective_date,tier_expires_date,coin_balance,merged_into_customer_id "
        "FROM customers WHERE id=?",
        (customer_id,),
    ).fetchone()
    if not row or row["merged_into_customer_id"]:
        return None
    data = dict(row)
    spend = db.execute(
        "SELECT COUNT(*) AS txn_count,COALESCE(SUM(final_amount),0) AS total_spend FROM transactions "
        "WHERE customer_id=? AND voided_at IS NULL", (customer_id,)
    ).fetchone()
    data.update(dict(spend))
    return data


def get_customer_tier_map(db: sqlite3.Connection, year: str) -> dict[int, str]:
    """回傳每位顧客「現在」的有效等級（V3 Phase 1 起用 get_effective_tier 現算，不寫入）。
    year 參數保留給既有呼叫端相容，但等級一律代表現況，不是某個歷史日曆年的回推。"""
    customer_ids = [int(r["id"]) for r in db.execute("SELECT id FROM customers WHERE merged_into_customer_id IS NULL").fetchall()]
    return {cid: get_effective_tier(db, cid) for cid in customer_ids}


@app.route("/")
def index():
    return render_template("home.html", version=APP_VERSION)


@app.route("/api/customers/search")
def search_customers():
    query = request.args.get("q", "").strip()
    if not query:
        return {"customers": []}

    db = get_db()
    rows = db.execute(
        "SELECT id, name, phone, birthday, coin_balance FROM customers WHERE merged_into_customer_id IS NULL AND (name LIKE ? OR phone LIKE ?) LIMIT 10",
        (f"%{query}%", f"%{query}%")
    ).fetchall()

    return {"customers": [dict(r) for r in rows]}


@app.route("/api/customers/duplicate-candidates")
def duplicate_candidates():
    """Expose likely existing profiles before staff creates a second record.

    Name is intentionally only an exact match here: partial-name suggestions remain in
    search_customers, while this endpoint must not imply that similarly named people are
    the same person.
    """
    name = request.args.get("name", "").strip()
    phone = request.args.get("phone", "").strip()
    if not name and not phone:
        return {"customers": []}
    clauses: list[str] = []
    params: list[str] = []
    if name:
        clauses.append("name=?")
        params.append(name)
    if phone:
        clauses.append("phone=?")
        params.append(phone)
    db = get_db()
    rows = db.execute(
        "SELECT id,name,phone,birthday,coin_balance FROM customers "
        "WHERE merged_into_customer_id IS NULL AND (" + " OR ".join(clauses) + ") ORDER BY id DESC LIMIT 10",
        params,
    ).fetchall()
    return {"customers": [dict(r) for r in rows]}


@app.route("/entry", methods=["GET", "POST"])
def entry():
    db = get_db()
    rules = load_rules()
    stores = db.execute("SELECT id, name FROM stores ORDER BY name").fetchall()

    selected_store = (request.args.get("store") or "").strip()
    result = None
    error_message = None

    if request.method == "POST":
        store_id = request.form.get("store_id", "").strip()
        selected_store = store_id
        entry_mode = request.form.get("entry_mode", "normal").strip()  # normal | coin_deduct | birthday_recharge
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        birthday = request.form.get("birthday", "").strip()
        try:
            txn_day = parse_date_or_today(request.form.get("txn_date", ""))
        except ValueError:
            txn_day = date.today()

        # ── Birthday Recharge Mode ────────────────────────────────────────────
        if entry_mode == "birthday_recharge":
            recharge_plan_str = request.form.get("recharge_plan", "").strip()
            if not (store_id and name and birthday and recharge_plan_str):
                error_message = "請填寫完整資料及選擇充值方案！"
            else:
                plan_amount = None
                plan_coins = None
                for p in BIRTHDAY_RECHARGE_PLANS:
                    if str(p["amount"]) == recharge_plan_str:
                        plan_amount = p["amount"]
                        plan_coins = p["coins"]
                        break
                if plan_amount is None:
                    error_message = "無效的充值方案，請重新選擇。"
                else:
                    # Verify customer is in birthday month
                    in_bday_month = is_birthday_month(birthday, txn_day)
                    if not in_bday_month:
                        error_message = "此顧客本月非生日月份，不符合壽星充值活動資格！"
                    else:
                        customer_id = get_or_create_customer(db, name, birthday, phone)
                        month_key = current_month_key(txn_day)
                        now_str = datetime.now().isoformat(timespec="seconds")
                        # Record as a recharge transaction (amount=0 consumption, coins_earned=plan_coins)
                        db.execute(
                            """
                            INSERT INTO transactions(
                                customer_id, store_id, txn_date, month_key, amount,
                                birthday_discount_applied, final_amount, cashback, created_at,
                                coins_earned, coins_redeemed, recharge_plan, recharge_amount, entry_mode
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                customer_id, store_id, txn_day.isoformat(), month_key,
                                0, 0, 0, 0, now_str,
                                plan_coins, 0, recharge_plan_str, plan_amount, "birthday_recharge",
                            ),
                        )
                        recharge_txn_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        # 美咖幣次月才入帳可用（V3 §7.4），跟一般消費賺點同一套規則
                        create_coin_batch(db, customer_id, plan_coins, txn_day, source_txn_id=recharge_txn_id)
                        sync_coin_balance(db, customer_id, txn_day)
                        db.commit()
                        coin_balance = get_customer_coin_balance(db, customer_id)
                        result = {
                            "mode": "birthday_recharge",
                            "name": name,
                            "recharge_amount": plan_amount,
                            "coins_earned": plan_coins,
                            "coin_balance": coin_balance,
                        }

        # ── Coin Deduct Mode ─────────────────────────────────────────────────
        elif entry_mode == "coin_deduct":
            try:
                coins_to_deduct = int(request.form.get("coins_deduct", "0") or 0)
            except ValueError:
                coins_to_deduct = 0
            deduct_note = request.form.get("deduct_note", "").strip()

            if not (store_id and name and birthday):
                error_message = "請填寫完整顧客資料！"
            elif coins_to_deduct <= 0:
                error_message = "扣點數量必須大於 0！"
            else:
                customer_id = get_or_create_customer(db, name, birthday, phone)
                usable_coins = get_usable_coin_total(db, customer_id, txn_day)
                if coins_to_deduct > usable_coins:
                    error_message = f"點數不足！目前可用：{usable_coins} 點，欲扣：{coins_to_deduct} 點。"
                else:
                    month_key = current_month_key(txn_day)
                    now_str = datetime.now().isoformat(timespec="seconds")
                    db.execute(
                        """
                        INSERT INTO transactions(
                            customer_id, store_id, txn_date, month_key, amount,
                            birthday_discount_applied, final_amount, cashback, created_at,
                            coins_earned, coins_redeemed, entry_mode, note
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            customer_id, store_id, txn_day.isoformat(), month_key,
                            0, 0, 0, 0, now_str,
                            0, coins_to_deduct, "coin_deduct", deduct_note,
                        ),
                    )
                    deduct_txn_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                    redeem_coins_fifo(db, customer_id, coins_to_deduct, deduct_txn_id, txn_day)
                    sync_coin_balance(db, customer_id, txn_day)
                    db.commit()
                    coin_balance = get_customer_coin_balance(db, customer_id)
                    result = {
                        "mode": "coin_deduct",
                        "name": name,
                        "coins_redeemed": coins_to_deduct,
                        "deduct_note": deduct_note,
                        "coin_balance": coin_balance,
                    }

        # ── Normal Mode ──────────────────────────────────────────────────────
        else:
            try:
                amount = float(request.form.get("amount", "0") or 0)
            except ValueError:
                amount = 0.0

            if store_id and name and birthday and amount > 0:
                customer_id = get_or_create_customer(db, name, birthday, phone)
                month_key = current_month_key(txn_day)
                year_str = txn_day.strftime("%Y")

                discount_applied = False
                final_amount = amount

                year_total_so_far = customer_year_total(db, customer_id, year_str)
                past_max_single = get_past_max_single(db, customer_id)

                # 會員年度制（V3 Phase 1）：先確認這筆補登日期沒有早於既有交易，
                # 再套用到期的 pending 升等／會員年度到期重判，取得「這筆交易當下」該用的等級。
                backdated = _is_backdated_entry(db, customer_id, txn_day)
                if backdated:
                    cur = db.execute(
                        "SELECT member_tier, tier_effective_date, tier_expires_date FROM customers WHERE id=?",
                        (customer_id,),
                    ).fetchone()
                    tier_before_name = cur["member_tier"]
                    window_start, window_end = cur["tier_effective_date"], cur["tier_expires_date"]
                    db.execute(
                        "INSERT INTO review_flags(item_type, item_key, status, note, updated_at) "
                        "VALUES(?,?,?,?,?)",
                        (
                            "backdated_entry",
                            f"cust{customer_id}_{txn_day.isoformat()}_{datetime.now().timestamp()}",
                            "unreviewed",
                            f"{name} 補登 {txn_day.isoformat()} 的交易，晚於既有交易紀錄，"
                            f"等級未自動重新評估，請人工複核",
                            datetime.now().isoformat(timespec="seconds"),
                        ),
                    )
                else:
                    tier_state = reevaluate_and_persist_tier(db, customer_id, txn_day)
                    tier_before_name = tier_state["member_tier"]
                    window_start, window_end = tier_state["tier_effective_date"], tier_state["tier_expires_date"]

                tier_before = _tier_rule_by_name(tier_before_name, rules)
                cashback = round(final_amount * tier_before.cashback_rate, 2)
                points = int(final_amount * tier_before.points_rate)

                # Coins earned = same as points for now
                coins_earned = points

                db.execute(
                    """
                    INSERT INTO transactions(
                        customer_id, store_id, txn_date, month_key, amount,
                        birthday_discount_applied, final_amount, cashback, created_at,
                        coins_earned, entry_mode
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        customer_id,
                        store_id,
                        txn_day.isoformat(),
                        month_key,
                        amount,
                        1 if discount_applied else 0,
                        round(final_amount, 2),
                        cashback,
                        datetime.now().isoformat(timespec="seconds"),
                        coins_earned,
                        "normal",
                    ),
                )
                # Get the txn_id we just inserted
                last_txn_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

                # 美咖幣次月才入帳可用（V3 §7.4），建一筆點數批次而不是直接加總數
                if coins_earned > 0:
                    create_coin_batch(db, customer_id, coins_earned, txn_day, source_txn_id=last_txn_id)
                sync_coin_balance(db, customer_id, txn_day)

                monthly_total = customer_month_total(db, customer_id, month_key)
                new_max_single = max(past_max_single, final_amount)

                if window_start:
                    # 已有會員年度視窗：達標偵測用視窗累計（V3 §3「本會員年度累計」）
                    accum_total = _window_total(db, customer_id, window_start, window_end)
                else:
                    # 一般會員尚無視窗：沿用日曆年累計判斷是否首次達標
                    accum_total = year_total_so_far + (final_amount if final_amount >= 1000 else 0)

                candidate_tier = calc_tier(new_max_single, accum_total, rules)
                coin_balance = get_customer_coin_balance(db, customer_id)

                # Detect tier upgrade (including multi-level jumps within one day).
                # 補登過去日期的交易不做升等偵測——時間序不可靠，交由人工複核（review_flags）。
                upgrades_recorded = []
                tier_after = tier_before
                if not backdated and candidate_tier.name != tier_before.name:
                    # 同一天內已經排過 pending（例如今天稍早的另一筆已經達標到 S，還沒生效），
                    # 這筆的升等偵測要接續那個基準，避免同一天內重複記錄同一段升等。
                    existing_pending = db.execute(
                        "SELECT pending_tier FROM customers WHERE id=?", (customer_id,)
                    ).fetchone()["pending_tier"]
                    baseline_name = existing_pending or tier_before.name
                    idx_before = TIER_ORDER.index(baseline_name) if baseline_name in TIER_ORDER else 0
                    idx_after = TIER_ORDER.index(candidate_tier.name) if candidate_tier.name in TIER_ORDER else 0
                    if idx_after > idx_before:
                        # Determine trigger reason
                        if new_max_single > past_max_single and new_max_single >= final_amount:
                            reason = f"單筆消費 {int(new_max_single):,} 元達標"
                        else:
                            reason = f"年度累計 {int(accum_total):,} 元達標"
                        now_str_up = datetime.now().isoformat(timespec="seconds")
                        # Record each intermediate upgrade level
                        for step in range(idx_before + 1, idx_after + 1):
                            step_from = TIER_ORDER[step - 1]
                            step_to = TIER_ORDER[step]
                            # Look up gift name from rules
                            gift_name = ""
                            for t in rules["vip_tiers"]:
                                if t["name"] == step_to:
                                    gift_name = f"{step_to} 升級禮"
                                    break
                            db.execute("""
                                INSERT INTO tier_upgrades(
                                    customer_id, upgrade_date, tier_before, tier_after,
                                    trigger_txn_id, trigger_reason, gift_name,
                                    gift_status, created_at
                                ) VALUES(?,?,?,?,?,?,?,?,?)
                            """, (
                                customer_id, txn_day.isoformat(),
                                step_from, step_to,
                                last_txn_id, reason, gift_name,
                                "pending", now_str_up,
                            ))
                            upgrades_recorded.append({"from": step_from, "to": step_to, "gift": gift_name})

                        # 達標當筆仍用舊等級（tier_before 已經是這樣），新等級排到隔天正式生效。
                        db.execute(
                            "UPDATE customers SET pending_tier=?, pending_effective_date=? WHERE id=?",
                            (candidate_tier.name, _add_days(txn_day.isoformat(), 1), customer_id),
                        )
                        tier_after = candidate_tier

                db.commit()

                result = {
                    "mode": "normal",
                    "name": name,
                    "store_id": store_id,
                    "amount": amount,
                    "final_amount": round(final_amount, 2),
                    "birthday_discount_applied": discount_applied,
                    "monthly_total": round(monthly_total, 2),
                    "tier": tier_after,
                    "cashback": cashback,
                    "points": points,
                    "coins_earned": coins_earned,
                    "coin_balance": coin_balance,
                    "upgrades": upgrades_recorded,
                }

    return render_template(
        "entry.html",
        stores=stores,
        result=result,
        selected_store=selected_store,
        error=error_message,
        birthday_recharge_plans=BIRTHDAY_RECHARGE_PLANS,
    )


def _parse_report_filters() -> dict[str, str]:
    """/report 與 /report/export.csv 共用的篩選條件解析（都從 request.args 讀）。"""
    default_month = date.today().strftime("%Y-%m")
    month_from = request.args.get("month_from", "").strip() or request.args.get("month", default_month)
    month_to = request.args.get("month_to", "").strip() or month_from
    if month_to < month_from:
        month_to = month_from
    return {
        "month_from": month_from,
        "month_to": month_to,
        "year": request.args.get("year", month_from[:4]),
        "store_id": request.args.get("store_id", "").strip(),
        "q": request.args.get("q", "").strip(),
        "start_date": request.args.get("start_date", "").strip(),
        "end_date": request.args.get("end_date", "").strip(),
        "birthday_month": request.args.get("birthday_month", "").strip(),
        "vip_tier": request.args.get("vip_tier", "").strip(),
    }


def _build_coin_summary_map(
    db: sqlite3.Connection, customer_ids: set[int], as_of: date | None = None,
) -> dict[int, dict[str, int]]:
    """回傳報表需要的會員點數摘要。

    ``coin_balance`` 是目前可用的快取；分筆帳本才是待入帳／失效點數的正典。將兩者
    一起輸出，畫面才能說清楚「歷史獲得」與「現在可用」為何不同。
    """
    if not customer_ids:
        return {}

    as_of_str = (as_of or date.today()).isoformat()
    placeholders = ",".join("?" * len(customer_ids))
    ids = list(customer_ids)
    rows = db.execute(
        f"""
        WITH transaction_totals AS (
            SELECT customer_id,
                   COALESCE(SUM(coins_earned), 0) AS earned,
                   COALESCE(SUM(coins_redeemed), 0) AS used
            FROM transactions
            WHERE voided_at IS NULL AND customer_id IN ({placeholders})
            GROUP BY customer_id
        ), manual_totals AS (
            SELECT customer_id, COALESCE(SUM(points), 0) AS earned
            FROM point_adjustments
            WHERE customer_id IN ({placeholders})
            GROUP BY customer_id
        ), batch_totals AS (
            SELECT customer_id,
                   COALESCE(SUM(CASE
                       WHEN status='active' AND credit_date > ? THEN remaining_amount
                       ELSE 0 END), 0) AS pending,
                   COALESCE(SUM(CASE
                       WHEN status='active' AND expires_date < ? THEN remaining_amount
                       ELSE 0 END), 0) AS expired,
                   COALESCE(SUM(CASE WHEN is_legacy=1 THEN earned_amount ELSE 0 END), 0) AS legacy_earned
            FROM coin_batches
            WHERE customer_id IN ({placeholders})
            GROUP BY customer_id
        )
        SELECT c.id AS customer_id, c.coin_balance,
               COALESCE(t.earned, 0) + COALESCE(m.earned, 0) +
               MAX(COALESCE(b.legacy_earned, 0) - COALESCE(t.earned, 0) - COALESCE(m.earned, 0), 0) AS total_earned,
               COALESCE(t.used, 0) AS total_used,
               COALESCE(b.pending, 0) AS pending_coins,
               COALESCE(b.expired, 0) AS expired_coins,
               MAX(COALESCE(b.legacy_earned, 0) - COALESCE(t.earned, 0) - COALESCE(m.earned, 0), 0) AS legacy_carryover
        FROM customers c
        LEFT JOIN transaction_totals t ON t.customer_id = c.id
        LEFT JOIN manual_totals m ON m.customer_id = c.id
        LEFT JOIN batch_totals b ON b.customer_id = c.id
        WHERE c.id IN ({placeholders})
        """,
        [*ids, *ids, as_of_str, as_of_str, *ids, *ids],
    ).fetchall()
    return {
        int(row["customer_id"]): {
            "total_earned": int(row["total_earned"] or 0),
            "total_used": int(row["total_used"] or 0),
            "pending_coins": int(row["pending_coins"] or 0),
            "expired_coins": int(row["expired_coins"] or 0),
            "legacy_carryover": int(row["legacy_carryover"] or 0),
            "coin_balance": int(row["coin_balance"] or 0),
        }
        for row in rows
    }


def _build_transaction_coin_status_map(
    db: sqlite3.Connection, transaction_ids: set[int], as_of: date | None = None,
) -> dict[int, dict[str, str]]:
    """回傳交易賺點批次的入帳狀態及可用日期，供日明細直接說明差額來源。"""
    if not transaction_ids:
        return {}

    as_of_str = (as_of or date.today()).isoformat()
    placeholders = ",".join("?" * len(transaction_ids))
    rows = db.execute(
        f"""
        SELECT source_txn_id, status, credit_date, expires_date
        FROM coin_batches
        WHERE source_txn_id IN ({placeholders})
        ORDER BY id DESC
        """,
        list(transaction_ids),
    ).fetchall()
    result: dict[int, dict[str, str]] = {}
    for row in rows:
        txn_id = int(row["source_txn_id"])
        if txn_id in result:
            continue
        if row["status"] == "superseded_by_legacy":
            # 舊制帳本轉入時已涵蓋此筆來源點數；保留批次作稽核，不可再重複計入餘額。
            status = "已入帳"
        elif row["status"] != "active":
            status = "已作廢"
        elif row["credit_date"] > as_of_str:
            status = "待入帳"
        elif row["expires_date"] < as_of_str:
            status = "已失效"
        else:
            status = "已入帳"
        result[txn_id] = {"status": status, "available_date": row["credit_date"]}
    return result


def _build_report_detail_rows(db: sqlite3.Connection, f: dict[str, str]) -> list[dict]:
    """/report 與 /report/export.csv 共用：依篩選條件查出的交易明細（含 VIP 與點數狀態）。"""
    where = ["t.voided_at IS NULL", "t.month_key >= ? AND t.month_key <= ?"]
    params: list[Any] = [f["month_from"], f["month_to"]]

    if f["store_id"]:
        where.append("t.store_id = ?")
        params.append(f["store_id"])
    if f["q"]:
        where.append("c.name LIKE ?")
        params.append(f"%{f['q']}%")
    if f["start_date"]:
        where.append("t.txn_date >= ?")
        params.append(f["start_date"])
    if f["end_date"]:
        where.append("t.txn_date <= ?")
        params.append(f["end_date"])
    if f["birthday_month"]:
        where.append("substr(c.birthday,6,2) = ?")
        params.append(f["birthday_month"].zfill(2))

    coins_earned_expr = "t.coins_earned" if has_column(db, 'transactions', 'coins_earned') else "0"
    coins_redeemed_expr = "t.coins_redeemed" if has_column(db, 'transactions', 'coins_redeemed') else "0"
    entry_mode_expr = "t.entry_mode" if has_column(db, 'transactions', 'entry_mode') else "'normal'"
    recharge_plan_expr = "t.recharge_plan" if has_column(db, 'transactions', 'recharge_plan') else "NULL"
    recharge_amount_expr = "t.recharge_amount" if has_column(db, 'transactions', 'recharge_amount') else "NULL"

    sql = f"""
        SELECT t.id, t.txn_date, t.customer_id, s.id AS store_id, s.name AS store_name,
               c.name AS customer_name, c.birthday,
               t.amount, t.final_amount, t.birthday_discount_applied,
               {coins_earned_expr} AS coins_earned,
               {coins_redeemed_expr} AS coins_redeemed,
               {entry_mode_expr} AS entry_mode,
               {recharge_plan_expr} AS recharge_plan,
               {recharge_amount_expr} AS recharge_amount
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        JOIN stores s ON s.id = t.store_id
        WHERE {' AND '.join(where)}
        ORDER BY t.txn_date DESC, s.name ASC, c.name ASC
    """
    detail_rows_raw = db.execute(sql, params).fetchall()

    tier_map = get_customer_tier_map(db, f["year"])

    # Build per-customer coin summary from the canonical dated ledger.
    customer_ids_in_result = set(int(r["customer_id"]) for r in detail_rows_raw)
    coin_summary_map = _build_coin_summary_map(db, customer_ids_in_result)
    transaction_status_map = _build_transaction_coin_status_map(
        db,
        {int(r["id"]) for r in detail_rows_raw if int(r["coins_earned"] or 0) > 0},
    )

    detail_rows = []
    for r in detail_rows_raw:
        d = dict(r)
        d["vip_tier"] = tier_map.get(int(r["customer_id"]), "一般會員")
        if f["vip_tier"] and d["vip_tier"] != f["vip_tier"]:
            continue
        cs = coin_summary_map.get(int(r["customer_id"]), {})
        d["cust_total_earned"] = cs.get("total_earned", 0)
        d["cust_total_used"] = cs.get("total_used", 0)
        d["cust_pending_coins"] = cs.get("pending_coins", 0)
        d["cust_expired_coins"] = cs.get("expired_coins", 0)
        d["cust_legacy_carryover"] = cs.get("legacy_carryover", 0)
        d["cust_coin_balance"] = cs.get("coin_balance", 0)
        if d["entry_mode"] in ("normal", "birthday_recharge") and int(d["coins_earned"] or 0) > 0:
            batch_status = transaction_status_map.get(int(r["id"]))
            d["coin_status"] = batch_status["status"] if batch_status else "已入帳"
            d["coin_available_date"] = batch_status["available_date"] if batch_status else ""
        else:
            d["coin_status"] = "—"
            d["coin_available_date"] = ""
        detail_rows.append(d)
    return detail_rows


@app.route("/report")
def report():
    db = get_db()
    sync_all_coin_balances(db)
    db.commit()
    f = _parse_report_filters()
    month_from, month_to = f["month_from"], f["month_to"]

    # Build human-readable month label
    if month_from == month_to:
        month_label = month_from
    else:
        month_label = f"{month_from} ~ {month_to}"

    detail_rows = _build_report_detail_rows(db, f)
    member_summaries_by_id: dict[int, dict[str, Any]] = {}
    for row in detail_rows:
        customer_id = int(row["customer_id"])
        if customer_id not in member_summaries_by_id:
            member_summaries_by_id[customer_id] = {
                "customer_name": row["customer_name"],
                "birthday": row["birthday"],
                "vip_tier": row["vip_tier"],
                "total_earned": row["cust_total_earned"],
                "total_used": row["cust_total_used"],
                "pending_coins": row["cust_pending_coins"],
                "expired_coins": row["cust_expired_coins"],
                "legacy_carryover": row["cust_legacy_carryover"],
                "coin_balance": row["cust_coin_balance"],
            }
    member_summaries = sorted(member_summaries_by_id.values(), key=lambda row: (row["customer_name"], row["birthday"]))

    monthly_by_customer = db.execute(
        """
        SELECT s.name AS store_name, c.name AS customer_name, c.birthday,
               SUM(t.final_amount) AS month_total
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        JOIN stores s ON s.id = t.store_id
        WHERE t.month_key >= ? AND t.month_key <= ? AND t.voided_at IS NULL
        GROUP BY s.name, c.name, c.birthday
        ORDER BY s.name, month_total DESC
        """,
        (month_from, month_to),
    ).fetchall()

    yearly_by_customer = db.execute(
        """
        SELECT s.name AS store_name, c.name AS customer_name, c.birthday,
               SUM(t.final_amount) AS year_total
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        JOIN stores s ON s.id = t.store_id
        WHERE substr(t.txn_date,1,4) = ? AND t.voided_at IS NULL
        GROUP BY s.name, c.name, c.birthday
        ORDER BY s.name, year_total DESC
        """,
        (f["year"],),
    ).fetchall()

    stores = db.execute("SELECT id,name FROM stores ORDER BY name").fetchall()

    filters = dict(f)

    return render_template(
        "report.html",
        month=month_from,
        month_label=month_label,
        month_from=month_from,
        month_to=month_to,
        detail_rows=detail_rows,
        member_summaries=member_summaries,
        monthly_by_customer=monthly_by_customer,
        yearly_by_customer=yearly_by_customer,
        stores=stores,
        filters=filters,
    )


@app.route("/report/export.csv")
def report_export_csv():
    """匯出報表明細成 CSV，套用跟 /report 完全一樣的篩選條件——
    不帶任何篩選參數就是「完整」報表，帶了篩選參數就是「有條件」報表。"""
    if not (session.get("manager_authed") or session.get("main_authed")):
        return "Unauthorized", 403
    db = get_db()
    sync_all_coin_balances(db)
    db.commit()
    f = _parse_report_filters()
    detail_rows = _build_report_detail_rows(db, f)

    mode_label = {"normal": "一般消費", "birthday_recharge": "壽星充值", "coin_deduct": "點數扣除"}
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "日期", "分店", "客戶", "生日", "VIP等級", "原始金額", "實收金額",
        "獲得點數", "扣除點數", "模式", "入帳狀態", "可用日期",
        "歷史累計獲得", "歷史累計使用", "待入帳點數", "目前可用點數",
    ])
    for r in detail_rows:
        writer.writerow([
            r["txn_date"], r["store_name"], r["customer_name"], r["birthday"], r["vip_tier"],
            f"{r['amount']:.0f}", f"{r['final_amount']:.0f}",
            r["coins_earned"], r["coins_redeemed"],
            mode_label.get(r["entry_mode"], r["entry_mode"]),
            r["coin_status"], r["coin_available_date"],
            r["cust_total_earned"], r["cust_total_used"],
            r["cust_pending_coins"], r["cust_coin_balance"],
        ])

    # 加 UTF-8 BOM 讓 Excel 開啟中文不亂碼
    mem = io.BytesIO(b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8"))
    mem.seek(0)
    filename = f"beauty_vip_report_{f['month_from']}_{f['month_to']}.csv"
    return send_file(mem, as_attachment=True, download_name=filename, mimetype="text/csv")


@app.route("/api/transactions/<int:txn_id>/update", methods=["POST"])
def update_transaction(txn_id):
    db = get_db()

    # Parse form data
    name = request.form.get("name", "").strip()
    birthday = request.form.get("birthday", "").strip()
    store_id = request.form.get("store_id", "").strip()
    try:
        amount = float(request.form.get("amount", "0") or 0)
    except ValueError:
        amount = 0.0
    try:
        cash_received_raw = request.form.get("cash_received", "").strip()
        cash_received: float | None = float(cash_received_raw) if cash_received_raw else None
    except ValueError:
        cash_received = None
    try:
        d = parse_date_or_today(request.form.get("txn_date", ""))
    except ValueError:
        d = date.today()

    if not (name and birthday and store_id):
        return {"status": "error", "message": "missing required fields"}, 400

    # 1. Fetch old transaction info to revert coins
    old_txn = db.execute(
        "SELECT customer_id, entry_mode, coins_earned, coins_redeemed, voided_at FROM transactions WHERE id=?",
        (txn_id,),
    ).fetchone()
    if not old_txn:
        return {"status": "error", "message": "original transaction not found"}, 404
    if old_txn["voided_at"]:
        return {"status": "error", "message": "cannot edit a voided transaction"}, 400

    old_mode = old_txn["entry_mode"] or "normal"
    old_coins_earned = int(old_txn["coins_earned"] or 0)
    old_customer_id = int(old_txn["customer_id"])

    # 2. Update customer basic info (if changed)
    db.execute(
        "UPDATE customers SET name=?, birthday=? WHERE id=?",
        (name, birthday, old_customer_id),
    )

    month_key = current_month_key(d)
    new_final_amount = cash_received if cash_received is not None else amount
    # 這個路由沒有欄位可以改壽星充值／扣點金額，只有 normal 模式的金額會變，
    # 其他模式沿用舊的 coins_earned，不要把它寫壞成 0。
    new_coins_earned = old_coins_earned

    # 3. Recalculate coins if it's a normal transaction
    if old_mode == "normal":
        rules = load_rules()
        year_str = d.strftime("%Y")
        past_max_single = float(db.execute("SELECT COALESCE(MAX(final_amount),0) FROM transactions WHERE customer_id=? AND id < ? AND voided_at IS NULL", (old_customer_id, txn_id)).fetchone()[0] or 0)
        year_total_so_far = float(db.execute("SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=? AND id < ? AND final_amount>=1000 AND voided_at IS NULL", (old_customer_id, year_str, txn_id)).fetchone()[0] or 0)
        tier = calc_tier(past_max_single, year_total_so_far, rules)
        new_coins_earned = int(new_final_amount * tier.points_rate)

    # 4. Update transaction record
    db.execute(
        """
        UPDATE transactions
        SET store_id=?, txn_date=?, month_key=?, amount=?, final_amount=?, coins_earned=?
        WHERE id=?
        """,
        (store_id, d.isoformat(), month_key, amount, new_final_amount, new_coins_earned, txn_id),
    )

    # 5. 調整對應的點數批次（normal/birthday_recharge 才有批次；coin_deduct 這個路由不會動）
    if old_mode in ("normal", "birthday_recharge"):
        adjust_coin_batch_for_edit(db, txn_id, new_coins_earned, old_customer_id, d)
    sync_coin_balance(db, old_customer_id)

    db.commit()
    return {"status": "ok"}


@app.route("/api/transactions/<int:txn_id>/delete", methods=["POST"])
def delete_transaction(txn_id):
    db = get_db()
    txn = db.execute(
        "SELECT entry_mode, coins_earned, coins_redeemed, customer_id, voided_at "
        "FROM transactions WHERE id=?",
        (txn_id,),
    ).fetchone()
    if not txn:
        return {"status": "error", "message": "transaction not found"}, 404
    if txn["voided_at"]:
        return {"status": "error", "message": "transaction already voided"}, 400

    mode = txn["entry_mode"] or "normal"
    coins_earned = int(txn["coins_earned"] or 0)
    coins_redeemed = int(txn["coins_redeemed"] or 0)
    customer_id = int(txn["customer_id"])
    void_reason = request.form.get("reason", "").strip()
    voided_by = "manager" if session.get("manager_authed") else "staff"

    # 財務資料不做硬刪除，改標記作廢並保留原始紀錄（供稽核／退款回溯查詢），
    # 所有統計/報表查詢都會排除 voided_at IS NOT NULL 的交易。
    db.execute(
        "UPDATE transactions SET voided_at=?, void_reason=?, voided_by=? WHERE id=?",
        (datetime.now().isoformat(timespec="seconds"), void_reason, voided_by, txn_id),
    )

    # Revert coin_balance impact（透過批次帳本，不直接動 customers.coin_balance）
    if mode in ("normal", "birthday_recharge") and coins_earned > 0:
        reverse_earning(db, customer_id, source_txn_id=txn_id)
    elif mode == "coin_deduct" and coins_redeemed > 0:
        reverse_redemption(db, txn_id)
    sync_coin_balance(db, customer_id)

    # V3 Phase 2：normal 交易作廢後，扣除的金額可能讓等級撐不住，重新判定（可能即時降級）。
    # 其他模式（coin_deduct/birthday_recharge）不計入會員年度累計，不影響等級。
    if mode == "normal":
        reevaluate_tier_after_void(db, customer_id, load_rules(), date.today(), txn_id, void_reason)

    db.commit()
    return {"status": "ok"}

@app.route("/api/transactions/<int:txn_id>/update_deduct", methods=["POST"])
def update_transaction_deduct(txn_id):
    db = get_db()
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"status": "error", "message": "Unauthorized"}, 403

    old_txn = db.execute(
        "SELECT customer_id, entry_mode, coins_redeemed FROM transactions WHERE id=? AND voided_at IS NULL",
        (txn_id,),
    ).fetchone()
    if not old_txn or old_txn["entry_mode"] != "coin_deduct":
        return {"status": "error", "message": "找不到此扣點紀錄"}, 404
        
    try:
        new_points = int(request.form.get("points", "0") or 0)
    except ValueError:
        new_points = 0
    new_reason = request.form.get("reason", "").strip()

    if new_points <= 0:
        return {"status": "error", "message": "點數必須大於 0"}, 400

    customer_id = int(old_txn["customer_id"])

    # 先把舊的扣點動用還原回批次，再依新點數重新 FIFO 扣一次
    reverse_redemption(db, txn_id)
    if not redeem_coins_fifo(db, customer_id, new_points, txn_id, date.today()):
        db.rollback()
        return {"status": "error", "message": "點數不足，無法調整成這個數量"}, 400
    db.execute(
        "UPDATE transactions SET coins_redeemed = ?, note = ? WHERE id=?",
        (new_points, new_reason, txn_id)
    )
    sync_coin_balance(db, customer_id)
    db.commit()
    return {"status": "ok"}



@app.route("/manager/unlock", methods=["POST"])
def manager_unlock():
    if _auth_is_limited("manager"):
        return render_template("manager_lock.html", error="嘗試次數過多，請五分鐘後再試。"), 429
    pin = (request.form.get("pin") or "").strip()
    if pin == MANAGER_PIN:
        _clear_auth_failures("manager")
        session["manager_authed"] = True
        session.permanent = True
        return redirect(url_for("manager_dashboard"))
    _record_auth_failure("manager")
    return render_template("manager_lock.html", error="密碼錯誤，請再試一次。")


@app.route("/manager/logout")
def manager_logout():
    session.pop("manager_authed", None)
    return redirect(url_for("manager_dashboard"))


@app.route("/manager/reevaluate_expired", methods=["POST"])
def manager_reevaluate_expired():
    """手動批次：把所有效期已屆滿的 S/P/A 會員重新判定一次。這是操作面安全網，
    不是正確性的唯一來源（沒觸發這個按鈕，get_effective_tier 顯示時一樣算得出正確等級，
    只是 customers.member_tier 欄位本身要等這位顧客下次來店或按這個按鈕才會真的寫入更新）。"""
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    db = get_db()
    today_str = date.today().isoformat()
    expired = db.execute(
        "SELECT id FROM customers WHERE merged_into_customer_id IS NULL AND tier_expires_date IS NOT NULL AND tier_expires_date < ?",
        (today_str,),
    ).fetchall()
    count = 0
    for row in expired:
        reevaluate_and_persist_tier(db, int(row["id"]), date.today())
        count += 1
    db.commit()
    return redirect(url_for("manager_dashboard", _anchor="reevaluated", reevaluated_count=count))


@app.route("/manager")
def manager_dashboard():
    if not session.get("manager_authed"):
        return render_template("manager_lock.html", error=None)

    db = get_db()
    sync_all_coin_balances(db)
    db.commit()
    default_month = date.today().strftime("%Y-%m")
    month_from = request.args.get("month_from", "").strip() or request.args.get("month", default_month)
    month_to = request.args.get("month_to", "").strip() or month_from
    if month_to < month_from:
        month_to = month_from
    year = month_from[:4]
    store_id_filter = request.args.get("store_id", "").strip()
    q_filter = request.args.get("q", "").strip()

    if month_from == month_to:
        month_label = month_from
    else:
        month_label = f"{month_from} ~ {month_to}"

    stores = db.execute("SELECT id, name FROM stores ORDER BY name").fetchall()

    store_stats = db.execute(
        """
        SELECT s.name AS store_name,
               SUM(CASE WHEN t.month_key >= ? AND t.month_key <= ? THEN t.final_amount ELSE 0 END) AS month_revenue,
               SUM(CASE WHEN substr(t.txn_date,1,4) = ? THEN t.final_amount ELSE 0 END) AS year_revenue
        FROM stores s
        LEFT JOIN transactions t ON s.id = t.store_id AND t.voided_at IS NULL
        GROUP BY s.name
        ORDER BY s.name
        """,
        (month_from, month_to, year),
    ).fetchall()

    # Customer stats with optional store + name/phone filter
    cust_where = ""
    cust_params: list[Any] = [month_from, month_to, year]
    if store_id_filter:
        cust_where += " AND t.store_id = ?"
        cust_params.append(store_id_filter)

    if q_filter:
        cust_where += " AND (c.name LIKE ? OR c.phone LIKE ?)"
        cust_params.extend([f"%{q_filter}%", f"%{q_filter}%"])

    customer_stats = db.execute(
        f"""
        SELECT c.id, c.name, c.phone, c.birthday, c.coin_balance,
               GROUP_CONCAT(DISTINCT s.name) AS stores,
               SUM(CASE WHEN t.month_key >= ? AND t.month_key <= ? THEN t.final_amount ELSE 0 END) AS month_spend,
               SUM(CASE WHEN substr(t.txn_date,1,4) = ? THEN t.final_amount ELSE 0 END) AS year_spend,
               SUM(t.cashback) AS total_cashback,
               COALESCE(SUM(t.coins_earned),0) AS total_coins_earned,
               COALESCE(SUM(t.coins_redeemed),0) AS total_coins_redeemed
        FROM customers c
        LEFT JOIN transactions t ON c.id = t.customer_id AND t.voided_at IS NULL {cust_where}
        LEFT JOIN stores s ON t.store_id = s.id
        WHERE c.merged_into_customer_id IS NULL
        GROUP BY c.id, c.name, c.phone, c.birthday
        HAVING year_spend > 0 OR month_spend > 0
        ORDER BY year_spend DESC
        """,
        cust_params,
    ).fetchall()

    filters = {"store_id": store_id_filter, "q": q_filter, "month_from": month_from, "month_to": month_to}

    return render_template(
        "manager.html",
        month=month_from,
        month_label=month_label,
        month_from=month_from,
        month_to=month_to,
        store_stats=store_stats,
        customer_stats=customer_stats,
        stores=stores,
        filters=filters,
    )


@app.route("/manager/backup")
def manager_backup():
    if not session.get("manager_authed"):
        return "Unauthorized", 403

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"beauty_vip_backup_{now_str}.db"

    return send_file(
        DB_PATH,
        as_attachment=True,
        download_name=filename,
        mimetype="application/x-sqlite3"
    )


@app.route("/contacts")
def contacts():
    db = get_db()
    sync_all_coin_balances(db)
    db.commit()
    query = request.args.get("q", "").strip()
    store_id = request.args.get("store_id", "").strip()
    birthday_month = request.args.get("birthday_month", "").strip()
    min_spend = request.args.get("min_spend", "").strip()
    max_spend = request.args.get("max_spend", "").strip()
    last_from = request.args.get("last_from", "").strip()
    last_to = request.args.get("last_to", "").strip()
    vip_tier = request.args.get("vip_tier", "").strip()

    sql = """
        SELECT c.id, c.name, c.phone, c.birthday, c.created_at, c.coin_balance,
               GROUP_CONCAT(DISTINCT s.name) AS stores,
               COALESCE(SUM(t.final_amount),0) AS total_spend,
               MAX(t.txn_date) AS last_txn_date,
               GROUP_CONCAT(DISTINCT t.store_id) AS store_ids,
               COALESCE(SUM(t.coins_earned),0) AS total_coins_earned,
               COALESCE(SUM(t.coins_redeemed),0) AS total_coins_redeemed
        FROM customers c
        LEFT JOIN transactions t ON c.id = t.customer_id AND t.voided_at IS NULL
        LEFT JOIN stores s ON t.store_id = s.id
        WHERE c.merged_into_customer_id IS NULL
    """
    params: list[Any] = []
    if query:
        sql += " AND (c.name LIKE ? OR c.phone LIKE ?)"
        params.extend([f"%{query}%", f"%{query}%"])
    if birthday_month:
        sql += " AND substr(c.birthday,6,2)=?"
        params.append(birthday_month.zfill(2))

    sql += " GROUP BY c.id"

    having = []
    if store_id:
        having.append("instr(',' || COALESCE(store_ids,'') || ',', ?) > 0")
        params.append(f",{store_id},")
    if min_spend:
        having.append("total_spend >= ?")
        params.append(float(min_spend))
    if max_spend:
        having.append("total_spend <= ?")
        params.append(float(max_spend))
    if last_from:
        having.append("COALESCE(last_txn_date,'') >= ?")
        params.append(last_from)
    if last_to:
        having.append("COALESCE(last_txn_date,'') <= ?")
        params.append(last_to)

    if having:
        sql += " HAVING " + " AND ".join(having)

    sql += " ORDER BY c.name"
    customer_rows = db.execute(sql, params).fetchall()
    customers = []
    for row in customer_rows:
        d = dict(row)
        tier_state = get_effective_tier_state(db, int(row["id"]))
        d["tier"] = tier_state["tier"]
        d["tier_effective_date"] = tier_state["tier_effective_date"]
        d["tier_expires_date"] = tier_state["tier_expires_date"]
        if vip_tier and d["tier"] != vip_tier:
            continue
        customers.append(d)
    stores = db.execute("SELECT id,name FROM stores ORDER BY name").fetchall()

    filters = {
        "q": query,
        "store_id": store_id,
        "birthday_month": birthday_month,
        "min_spend": min_spend,
        "max_spend": max_spend,
        "last_from": last_from,
        "last_to": last_to,
        "vip_tier": vip_tier,
    }

    return render_template("contacts.html", customers=customers, query=query, stores=stores, filters=filters)


@app.route("/api/customers/<int:customer_id>/delete", methods=["POST"])
def delete_customer(customer_id):
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    reason = _confirmed_reason()
    if not reason:
        return "刪除是高風險操作，請填寫原因並再次確認。", 400
    db = get_db()
    customer = db.execute("SELECT * FROM customers WHERE id=? AND merged_into_customer_id IS NULL", (customer_id,)).fetchone()
    if not customer:
        return "找不到此顧客", 404
    ledger_count = db.execute(
        "SELECT (SELECT COUNT(*) FROM transactions WHERE customer_id=?) + "
        "(SELECT COUNT(*) FROM coin_batches WHERE customer_id=?) + "
        "(SELECT COUNT(*) FROM point_adjustments WHERE customer_id=?) + "
        "(SELECT COUNT(*) FROM tier_upgrades WHERE customer_id=?)",
        (customer_id, customer_id, customer_id, customer_id),
    ).fetchone()[0]
    if ledger_count:
        return "此顧客已有交易、點數或會員稽核紀錄，為保留帳務歷史不可刪除。", 409
    db.execute(
        "DELETE FROM review_flags WHERE item_type='birthday_suspicious' AND item_key=?",
        (str(customer_id),),
    )
    db.execute("DELETE FROM customers WHERE id=?", (customer_id,))
    _log_customer_operation(db, "delete_empty_customer", customer_id, reason, dict(customer), {})
    db.commit()
    return redirect(url_for("contacts"))


@app.route("/api/customers/<int:customer_id>/add_points", methods=["POST"])
def add_points(customer_id):
    db = get_db()

    try:
        points = int(request.form.get("points", "0") or 0)
    except ValueError:
        points = 0

    reason = request.form.get("reason", "").strip()

    if points <= 0:
        return "點數數量必須大於 0", 400
    if not reason:
        return "請填寫贈點原因", 400

    cust = db.execute("SELECT id FROM customers WHERE id=?", (customer_id,)).fetchone()
    if not cust:
        return "找不到此顧客", 404

    # Determine operator identity from session
    if session.get("manager_authed"):
        operator = "manager"
    elif session.get("main_authed"):
        operator = "staff"
    else:
        return "Unauthorized", 403

    now_str = datetime.now().isoformat(timespec="seconds")
    db.execute(
        "INSERT INTO point_adjustments(customer_id, points, reason, operator, created_at) VALUES(?,?,?,?,?)",
        (customer_id, points, reason, operator, now_str),
    )
    adj_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    # 手動贈點立即可用，不用等次月入帳（不是消費觸發的，是店家直接發放）
    create_coin_batch(db, customer_id, points, date.today(), source_adjustment_id=adj_id, immediate=True)
    sync_coin_balance(db, customer_id)
    db.commit()
    return redirect(url_for("contacts"))


@app.route("/api/customers/<int:customer_id>/point_adjustments")
def get_point_adjustments(customer_id):
    db = get_db()
    adjustments = db.execute(
        "SELECT id, points, reason, operator, created_at FROM point_adjustments WHERE customer_id=?",
        (customer_id,),
    ).fetchall()
    deducts = db.execute(
        "SELECT id, coins_redeemed as points, note as reason, created_at FROM transactions "
        "WHERE customer_id=? AND entry_mode='coin_deduct' AND voided_at IS NULL",
        (customer_id,),
    ).fetchall()

    combined = []
    for r in adjustments:
        combined.append({
            "id": r["id"],
            "type": "add",
            "points": r["points"],
            "reason": r["reason"],
            "operator": r["operator"],
            "created_at": r["created_at"]
        })
    for r in deducts:
        combined.append({
            "id": r["id"],
            "type": "deduct",
            "points": r["points"],
            "reason": r["reason"] or "點數扣除",
            "operator": "staff",
            "created_at": r["created_at"]
        })
    combined.sort(key=lambda x: x["created_at"], reverse=True)
    return {"adjustments": combined}


@app.route("/api/point_adjustments/<int:adj_id>/update", methods=["POST"])
def update_point_adjustment(adj_id):
    db = get_db()
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"status": "error", "message": "Unauthorized"}, 403

    old = db.execute(
        "SELECT id, customer_id, points FROM point_adjustments WHERE id=?", (adj_id,)
    ).fetchone()
    if not old:
        return {"status": "error", "message": "找不到此紀錄"}, 404

    try:
        new_points = int(request.form.get("points", "0") or 0)
    except ValueError:
        new_points = 0
    new_reason = request.form.get("reason", "").strip()

    if new_points <= 0:
        return {"status": "error", "message": "點數必須大於 0"}, 400
    if not new_reason:
        return {"status": "error", "message": "原因不能為空"}, 400

    db.execute(
        "UPDATE point_adjustments SET points=?, reason=? WHERE id=?",
        (new_points, new_reason, adj_id),
    )
    adjust_coin_batch_for_adjustment_edit(db, adj_id, new_points, int(old["customer_id"]))
    sync_coin_balance(db, int(old["customer_id"]))
    db.commit()
    return {"status": "ok"}


@app.route("/api/point_adjustments/<int:adj_id>/delete", methods=["POST"])
def delete_point_adjustment(adj_id):
    db = get_db()
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"status": "error", "message": "Unauthorized"}, 403

    old = db.execute(
        "SELECT id, customer_id, points FROM point_adjustments WHERE id=?", (adj_id,)
    ).fetchone()
    if not old:
        return {"status": "error", "message": "找不到此紀錄"}, 404

    db.execute("DELETE FROM point_adjustments WHERE id=?", (adj_id,))
    reverse_earning(db, int(old["customer_id"]), source_adjustment_id=adj_id)
    sync_coin_balance(db, int(old["customer_id"]))
    db.commit()
    return {"status": "ok"}


@app.route("/api/customers/<int:customer_id>/update", methods=["POST"])
def update_customer(customer_id):
    if not (session.get("manager_authed") or session.get("main_authed")):
        return "Unauthorized", 403
    db = get_db()
    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    birthday = request.form.get("birthday", "").strip()
    tier_expires_date = request.form.get("tier_expires_date", "").strip()
    try:
        if birthday:
            datetime.strptime(birthday, "%Y-%m-%d")
        if tier_expires_date:
            datetime.strptime(tier_expires_date, "%Y-%m-%d")
    except ValueError:
        return "日期格式錯誤，請使用 YYYY-MM-DD", 400

    before_row = db.execute("SELECT * FROM customers WHERE id=? AND merged_into_customer_id IS NULL", (customer_id,)).fetchone()
    if not before_row:
        return "找不到有效顧客", 404
    before = dict(before_row)
    sensitive_change = (birthday and birthday != before["birthday"]) or bool(tier_expires_date)
    reason = _confirmed_reason() if sensitive_change else "一般資料修正"
    if sensitive_change and not reason:
        return "生日或會員效期異動是高風險操作，請填寫原因並再次確認。", 400
    updates = []
    params: list[Any] = []
    if name:
        updates.append("name=?")
        params.append(name)
    if phone is not None:
        updates.append("phone=?")
        params.append(phone)
    if birthday:
        updates.append("birthday=?")
        params.append(birthday)
    if updates:
        params.append(customer_id)
        try:
            db.execute(f"UPDATE customers SET {', '.join(updates)} WHERE id=? AND merged_into_customer_id IS NULL", params)
            db.commit()
        except Exception as e:
            err_msg = str(e)
            if "UNIQUE" in err_msg:
                # Conflict with existing customer — check if auto-merge is possible
                new_name = name or db.execute("SELECT name FROM customers WHERE id=?", (customer_id,)).fetchone()["name"]
                new_bday = birthday or db.execute("SELECT birthday FROM customers WHERE id=?", (customer_id,)).fetchone()["birthday"]
                conflict = db.execute(
                    "SELECT id FROM customers WHERE name=? AND birthday=? AND id!=?",
                    (new_name, new_bday, customer_id),
                ).fetchone()
                if conflict:
                    return (
                        f"<p style='color:red;padding:20px;'>⚠️ 儲存失敗：已存在相同姓名+生日的顧客（ID {conflict['id']}）。"
                        f"如需合併，請至<a href='/review'>資料審核頁</a>操作合併功能。</p>"
                        f"<p><a href='/contacts'>← 返回通訊錄</a></p>",
                        409,
                    )
            return f"<p style='color:red;padding:20px;'>儲存失敗：{err_msg}</p><p><a href='/contacts'>← 返回通訊錄</a></p>", 500

    if tier_expires_date:
        # tier_expires_date 是 get_effective_tier_state() 投影出的值，不是原始欄位；
        # 先重新評估寫回，確保手動覆寫的起點跟畫面上看到的一致，再套用覆寫。
        state = reevaluate_and_persist_tier(db, customer_id, date.today())
        if not state.get("tier_effective_date"):
            return (
                "<p style='color:red;padding:20px;'>儲存失敗：此顧客目前為一般會員，沒有等級效期可調整。</p>"
                "<p><a href='/contacts'>← 返回通訊錄</a></p>",
                400,
            )
        if tier_expires_date <= state["tier_effective_date"]:
            return (
                f"<p style='color:red;padding:20px;'>儲存失敗：到期日必須晚於生效日（{state['tier_effective_date']}）。</p>"
                "<p><a href='/contacts'>← 返回通訊錄</a></p>",
                400,
            )
        db.execute(
            "UPDATE customers SET tier_expires_date=? WHERE id=? AND merged_into_customer_id IS NULL",
            (tier_expires_date, customer_id),
        )
        db.commit()

    after_row = db.execute("SELECT * FROM customers WHERE id=?", (customer_id,)).fetchone()
    if dict(after_row) != before:
        _log_customer_operation(db, "update_customer", customer_id, reason or "一般資料修正", before, dict(after_row))
        db.commit()

    return redirect(url_for("contacts"))


# ── Review page ──────────────────────────────────────────────────────────────

SUSPICIOUS_BIRTHDAYS = ["2000-01-01", "1900-01-01", "1990-01-01", "2001-01-01"]


def _get_review_flag(db: sqlite3.Connection, item_type: str, item_key: str) -> str:
    row = db.execute(
        "SELECT status FROM review_flags WHERE item_type=? AND item_key=?",
        (item_type, item_key),
    ).fetchone()
    return row["status"] if row else "unreviewed"


@app.route("/review")
def review_page():
    if not session.get("manager_authed"):
        return render_template("manager_lock.html", error=None)

    db = get_db()
    name_q = request.args.get("q", "").strip()
    store_filter = request.args.get("store_id", "").strip()
    status_filter = request.args.get("status", "").strip()

    placeholders = ",".join("?" * len(SUSPICIOUS_BIRTHDAYS))
    sql_a = f"""
        SELECT c.id, c.name, c.phone, c.birthday, c.created_at,
               GROUP_CONCAT(DISTINCT s.name) AS stores,
               GROUP_CONCAT(DISTINCT t.store_id) AS store_ids
        FROM customers c
        LEFT JOIN transactions t ON c.id = t.customer_id
        LEFT JOIN stores s ON t.store_id = s.id
        WHERE c.merged_into_customer_id IS NULL AND c.birthday IN ({placeholders})
        """
    params_a: list[Any] = list(SUSPICIOUS_BIRTHDAYS)
    if name_q:
        sql_a += " AND c.name LIKE ?"
        params_a.append(f"%{name_q}%")
    sql_a += " GROUP BY c.id ORDER BY c.name"
    suspicious_rows_raw = db.execute(sql_a, params_a).fetchall()

    suspicious_rows = []
    for r in suspicious_rows_raw:
        d = dict(r)
        if store_filter and store_filter not in (d.get("store_ids") or ""):
            continue
        d["review_status"] = _get_review_flag(db, "birthday_suspicious", str(d["id"]))
        if status_filter and d["review_status"] != status_filter:
            continue
        suspicious_rows.append(d)

    sql_b = """
        SELECT name, GROUP_CONCAT(id) AS ids, GROUP_CONCAT(birthday) AS birthdays,
               COUNT(DISTINCT birthday) AS bday_count,
               GROUP_CONCAT(phone) AS phones,
               GROUP_CONCAT(created_at) AS created_ats
        FROM customers WHERE merged_into_customer_id IS NULL
        GROUP BY name
        HAVING bday_count > 1
        ORDER BY name
    """
    multi_bday_raw = db.execute(sql_b).fetchall()

    multi_bday_rows = []
    for r in multi_bday_raw:
        d = dict(r)
        if name_q and name_q not in d["name"]:
            continue
        item_key = f"multibd_{d['name']}"
        d["review_status"] = _get_review_flag(db, "multi_birthday", item_key)
        if status_filter and d["review_status"] != status_filter:
            continue
        ids = (d["ids"] or "").split(",")
        bdays = (d["birthdays"] or "").split(",")
        phones = (d["phones"] or "").split(",")
        created = (d["created_ats"] or "").split(",")
        d["members"] = [
            {"id": ids[i], "birthday": bdays[i], "phone": phones[i] if i < len(phones) else "", "created_at": created[i] if i < len(created) else ""}
            for i in range(len(ids))
        ]
        multi_bday_rows.append(d)

    stores = db.execute("SELECT id, name FROM stores ORDER BY name").fetchall()
    filters = {"q": name_q, "store_id": store_filter, "status": status_filter}

    recent_merges = db.execute(
        "SELECT m.id,m.created_at,m.reason,k.name AS keep_name,a.name AS absorbed_name FROM customer_merge_operations m "
        "JOIN customers k ON k.id=m.keep_customer_id JOIN customers a ON a.id=m.absorbed_customer_id "
        "WHERE m.status='merged' ORDER BY m.id DESC LIMIT 10"
    ).fetchall()
    return render_template("review.html", suspicious_rows=suspicious_rows, multi_bday_rows=multi_bday_rows, recent_merges=recent_merges, stores=stores, filters=filters)


@app.route("/review/update_birthday/<int:customer_id>", methods=["POST"])
def review_update_birthday(customer_id):
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    db = get_db()
    reason = _confirmed_reason()
    if not reason:
        return "生日更正是高風險操作，請填寫原因並再次確認。", 400
    birthday = request.form.get("birthday", "").strip()
    try:
        if birthday:
            datetime.strptime(birthday, "%Y-%m-%d")
    except ValueError:
        return "生日格式錯誤", 400
    before_row = db.execute("SELECT * FROM customers WHERE id=? AND merged_into_customer_id IS NULL", (customer_id,)).fetchone()
    if not before_row:
        return "找不到有效顧客", 404
    try:
        db.execute("UPDATE customers SET birthday=? WHERE id=? AND merged_into_customer_id IS NULL", (birthday, customer_id))
        after_row = db.execute("SELECT * FROM customers WHERE id=?", (customer_id,)).fetchone()
        _log_customer_operation(db, "update_birthday", customer_id, reason, dict(before_row), dict(after_row))
        db.commit()
    except Exception as e:
        if "UNIQUE" in str(e):
            cur_name = db.execute("SELECT name FROM customers WHERE id=?", (customer_id,)).fetchone()["name"]
            conflict = db.execute(
                "SELECT id FROM customers WHERE name=? AND birthday=? AND id!=?",
                (cur_name, birthday, customer_id),
            ).fetchone()
            if conflict:
                return (
                    f"<p style='color:red;padding:20px;'>⚠️ 儲存失敗：已存在相同姓名+生日的顧客（ID {conflict['id']}）。"
                    f"請使用下方合併功能，將 ID {customer_id} 合併至 ID {conflict['id']}。</p>"
                    f"<p><a href='/review'>← 返回審核頁</a></p>",
                    409,
                )
        return f"<p style='color:red;padding:20px;'>儲存失敗：{e}</p><p><a href='/review'>← 返回審核頁</a></p>", 500
    return redirect(request.referrer or url_for("review_page"))


@app.route("/api/customers/merge", methods=["POST"])
def merge_customers():
    """Confirmed soft merge: keep source data recoverable for a later undo."""
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    reason = _confirmed_reason()
    if not reason:
        return "合併是高風險操作，請填寫原因並在預覽頁再次確認。", 400
    db = get_db()
    try:
        keep_id = int(request.form.get("keep_id", "0"))
        absorb_id = int(request.form.get("absorb_id", "0"))
    except (ValueError, TypeError):
        return "參數錯誤", 400

    if keep_id == absorb_id or not keep_id or not absorb_id:
        return "請選擇兩個不同的顧客", 400

    keep = db.execute("SELECT * FROM customers WHERE id=? AND merged_into_customer_id IS NULL", (keep_id,)).fetchone()
    absorb = db.execute("SELECT * FROM customers WHERE id=? AND merged_into_customer_id IS NULL", (absorb_id,)).fetchone()

    if not keep or not absorb:
        return "找不到指定顧客", 404

    # Snapshot both effective states before deleting either record.  The retained
    # account may be lower tier solely because the qualifying history is on the
    # duplicate account.
    pre_merge_states = [
        _project_tier_state(db, customer_id, date.today().isoformat())[0]
        for customer_id in (keep_id, absorb_id)
    ]
    moved_ids = {
        table: [r["id"] for r in db.execute(f"SELECT id FROM {table} WHERE customer_id=?", (absorb_id,)).fetchall()]
        for table in ("transactions", "coin_batches", "point_adjustments", "tier_upgrades")
    }
    now_str = datetime.now().isoformat(timespec="seconds")

    # Move all transactions from absorb → keep
    db.execute("UPDATE transactions SET customer_id=? WHERE customer_id=?", (keep_id, absorb_id))
    # 點數批次也要一起搬過去，不然餘額會對不上（coin_balance 是從批次算出來的）
    db.execute("UPDATE coin_batches SET customer_id=? WHERE customer_id=?", (keep_id, absorb_id))
    # 人工調整與升等禮都是顧客帳本的一部分；不搬會在刪除 duplicate 後留下孤兒資料。
    db.execute("UPDATE point_adjustments SET customer_id=? WHERE customer_id=?", (keep_id, absorb_id))
    db.execute("UPDATE tier_upgrades SET customer_id=? WHERE customer_id=?", (keep_id, absorb_id))

    # Keep the more complete phone (prefer non-empty)
    merged_phone = keep["phone"] or absorb["phone"] or ""
    db.execute("UPDATE customers SET phone=? WHERE id=?", (merged_phone, keep_id))
    _reconcile_tier_after_merge(db, keep_id, pre_merge_states, date.today())
    sync_coin_balance(db, keep_id)

    # Keep the absorbed identity as a soft-merged record so this operation is reversible.
    db.execute("UPDATE customers SET merged_into_customer_id=?, merged_at=? WHERE id=?", (keep_id, now_str, absorb_id))

    # Clean up review flags for absorbed customer
    db.execute("DELETE FROM review_flags WHERE item_type='birthday_suspicious' AND item_key=?", (str(absorb_id),))

    keep_after = dict(db.execute("SELECT * FROM customers WHERE id=?", (keep_id,)).fetchone())
    db.execute(
        "INSERT INTO customer_merge_operations(keep_customer_id,absorbed_customer_id,reason,keep_before_json,absorbed_before_json,moved_ids_json,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (keep_id, absorb_id, reason, json.dumps(dict(keep), ensure_ascii=False), json.dumps(dict(absorb), ensure_ascii=False),
         json.dumps(moved_ids), now_str),
    )
    _log_customer_operation(db, "merge", keep_id, reason, dict(keep), keep_after, absorb_id)
    db.commit()
    return redirect(request.referrer or url_for("review_page"))


@app.route("/api/customers/merge/preview")
def merge_customers_preview():
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    try:
        keep_id, absorb_id = int(request.args["keep_id"]), int(request.args["absorb_id"])
    except (KeyError, TypeError, ValueError):
        return "參數錯誤", 400
    db = get_db()
    keep, absorb = _customer_merge_summary(db, keep_id), _customer_merge_summary(db, absorb_id)
    if not keep or not absorb or keep_id == absorb_id:
        return "找不到可合併的兩位有效顧客", 404
    simulated = _history_tier_state_for_customers(db, [keep_id, absorb_id], date.today())
    # Simulation includes both rows' effective membership states without writing anything.
    states = [_project_tier_state(db, cid, date.today().isoformat())[0] for cid in (keep_id, absorb_id)]
    candidates = states + [simulated]
    after = max(candidates, key=lambda s: TIER_ORDER.index(s.get("member_tier", "一般會員")))
    return render_template("merge_preview.html", keep=keep, absorb=absorb, after=after)


@app.route("/api/customer-merges/<int:operation_id>/undo", methods=["POST"])
def undo_customer_merge(operation_id: int):
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    reason = _confirmed_reason()
    if not reason:
        return "還原是高風險操作，請填寫原因並再次確認。", 400
    db = get_db()
    op = db.execute("SELECT * FROM customer_merge_operations WHERE id=? AND status='merged'", (operation_id,)).fetchone()
    if not op:
        return "找不到可還原的合併紀錄", 404
    moved = json.loads(op["moved_ids_json"])
    keep_before, absorbed_before = json.loads(op["keep_before_json"]), json.loads(op["absorbed_before_json"])
    # Do not split a customer again after new transactions were entered; it would be ambiguous.
    moved_txns = moved.get("transactions", [])
    placeholders = ",".join("?" * len(moved_txns)) or "0"
    newer = db.execute(
        f"SELECT COUNT(*) FROM transactions WHERE customer_id=? AND created_at>=? AND id NOT IN ({placeholders})",
        [op["keep_customer_id"], op["created_at"], *moved_txns],
    ).fetchone()[0]
    newer += db.execute(
        "SELECT COUNT(*) FROM point_adjustments WHERE customer_id=? AND created_at>=?",
        (op["keep_customer_id"], op["created_at"]),
    ).fetchone()[0]
    if newer:
        return "合併後已有新消費，為避免錯分帳本，請由資料管理員人工處理。", 409
    for table in ("transactions", "coin_batches", "point_adjustments", "tier_upgrades"):
        ids = moved.get(table, [])
        if ids:
            marks = ",".join("?" * len(ids))
            db.execute(f"UPDATE {table} SET customer_id=? WHERE id IN ({marks})", [op["absorbed_customer_id"], *ids])
    for snapshot in (keep_before, absorbed_before):
        db.execute(
            "UPDATE customers SET phone=?,member_tier=?,tier_effective_date=?,tier_expires_date=?,pending_tier=?,pending_effective_date=?,merged_into_customer_id=?,merged_at=? WHERE id=?",
            (snapshot["phone"], snapshot["member_tier"], snapshot["tier_effective_date"], snapshot["tier_expires_date"],
             snapshot["pending_tier"], snapshot["pending_effective_date"], snapshot.get("merged_into_customer_id"), snapshot.get("merged_at"), snapshot["id"]),
        )
    sync_coin_balance(db, int(op["keep_customer_id"]))
    sync_coin_balance(db, int(op["absorbed_customer_id"]))
    db.execute("UPDATE customer_merge_operations SET status='undone',reverted_at=? WHERE id=?", (datetime.now().isoformat(timespec="seconds"), operation_id))
    _log_customer_operation(db, "merge_undo", int(op["keep_customer_id"]), reason, keep_before, absorbed_before, int(op["absorbed_customer_id"]))
    db.commit()
    return redirect(url_for("review_page"))


@app.route("/review/mark/<item_type>/<path:item_key>", methods=["POST"])
def review_mark(item_type, item_key):
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    db = get_db()
    status = request.form.get("status", "reviewed")
    note = request.form.get("note", "")
    now = datetime.now().isoformat(timespec="seconds")
    db.execute(
        """INSERT INTO review_flags(item_type, item_key, status, note, updated_at)
           VALUES(?,?,?,?,?)
           ON CONFLICT(item_type, item_key) DO UPDATE SET status=excluded.status, note=excluded.note, updated_at=excluded.updated_at""",
        (item_type, item_key, status, note, now),
    )
    db.commit()
    return redirect(request.referrer or url_for("review_page"))


# ── Tier Upgrade Management ──────────────────────────────────────────────────

@app.route("/upgrades")
def upgrades_page():
    """升級禮追蹤頁面"""
    db = get_db()
    status_filter = request.args.get("status", "").strip()
    q_filter = request.args.get("q", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    where = ["1=1"]
    params: list[Any] = []
    if status_filter:
        where.append("u.gift_status = ?")
        params.append(status_filter)
    if q_filter:
        where.append("(c.name LIKE ? OR c.phone LIKE ?)")
        params.extend([f"%{q_filter}%", f"%{q_filter}%"])
    if start_date:
        where.append("u.upgrade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("u.upgrade_date <= ?")
        params.append(end_date)

    rows = db.execute(
        f"""
        SELECT u.*, c.name AS customer_name, c.phone AS customer_phone, c.birthday
        FROM tier_upgrades u
        JOIN customers c ON c.id = u.customer_id
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE u.gift_status WHEN 'pending' THEN 0 ELSE 1 END,
            u.upgrade_date DESC
        """,
        params,
    ).fetchall()

    # Count by status (all time, ignores date filter for the general tabs if we want, or we can apply it. The original code didn't apply date filter to status tabs, but let's leave status_counts as is)
    counts = db.execute(
        "SELECT gift_status, COUNT(*) AS cnt FROM tier_upgrades GROUP BY gift_status"
    ).fetchall()
    status_counts = {r["gift_status"]: r["cnt"] for r in counts}

    # Count delivered gifts by tier (applying date filter)
    where_delivered = ["gift_status = 'delivered'"]
    params_delivered = []
    if start_date:
        where_delivered.append("upgrade_date >= ?")
        params_delivered.append(start_date)
    if end_date:
        where_delivered.append("upgrade_date <= ?")
        params_delivered.append(end_date)
        
    tier_counts = db.execute(
        f"""
        SELECT tier_after, COUNT(*) AS cnt 
        FROM tier_upgrades
        WHERE {' AND '.join(where_delivered)}
        GROUP BY tier_after
        """, params_delivered
    ).fetchall()
    delivered_tier_counts = {r["tier_after"]: r["cnt"] for r in tier_counts}

    filters = {"status": status_filter, "q": q_filter, "start_date": start_date, "end_date": end_date}
    return render_template(
        "upgrades.html",
        upgrades=rows,
        filters=filters,
        status_counts=status_counts,
        delivered_tier_counts=delivered_tier_counts,
    )


@app.route("/api/upgrades/<int:upgrade_id>/deliver", methods=["POST"])
def deliver_upgrade_gift(upgrade_id):
    """標記升級禮已送達"""
    db = get_db()
    note = request.form.get("note", "").strip()
    now_str = datetime.now().isoformat(timespec="seconds")
    db.execute(
        "UPDATE tier_upgrades SET gift_status='delivered', gift_delivered_at=?, gift_delivered_by='staff', note=? WHERE id=?",
        (now_str, note, upgrade_id),
    )
    db.commit()
    return redirect(request.referrer or url_for("upgrades_page"))


@app.route("/api/upgrades/<int:upgrade_id>/skip", methods=["POST"])
def skip_upgrade_gift(upgrade_id):
    """標記升級禮略過（不發放）"""
    db = get_db()
    note = request.form.get("note", "").strip()
    now_str = datetime.now().isoformat(timespec="seconds")
    db.execute(
        "UPDATE tier_upgrades SET gift_status='skipped', gift_delivered_at=?, note=? WHERE id=?",
        (now_str, note, upgrade_id),
    )
    db.commit()
    return redirect(request.referrer or url_for("upgrades_page"))


@app.route("/api/upgrades/<int:upgrade_id>/reopen", methods=["POST"])
def reopen_upgrade_gift(upgrade_id):
    """重新開啟升級禮為待發放（補發機制）"""
    db = get_db()
    db.execute(
        "UPDATE tier_upgrades SET gift_status='pending', gift_delivered_at=NULL, gift_delivered_by=NULL WHERE id=?",
        (upgrade_id,),
    )
    db.commit()
    return redirect(request.referrer or url_for("upgrades_page"))


# ── Customer Self-Service Lookup ─────────────────────────────────────────────

@app.route("/my", methods=["GET", "POST"])
def customer_lookup():
    """Customer-facing readonly page to view own records."""
    result = None
    error = None
    candidates = None  # For multi-match selection

    # Direct lookup by customer_id (from selection list)
    cid_direct = request.args.get("cid", "").strip()
    if cid_direct:
        try:
            cid = int(cid_direct)
        except ValueError:
            cid = None
        if cid:
            result = _build_customer_result(cid)
            if not result:
                error = "查無此顧客。"

    elif request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        birthday = request.form.get("birthday", "").strip()

        if not name:
            error = "請輸入姓名"
        elif not phone and not birthday:
            error = "請輸入手機號碼或生日進行驗證"
        else:
            db = get_db()

            rows = []
            if phone and birthday:
                rows = db.execute(
                    "SELECT id, name, phone, birthday FROM customers WHERE merged_into_customer_id IS NULL AND name=? AND phone=? AND birthday=?",
                    (name, phone, birthday),
                ).fetchall()
            elif phone:
                rows = db.execute(
                    "SELECT id, name, phone, birthday FROM customers WHERE merged_into_customer_id IS NULL AND name=? AND phone=?",
                    (name, phone),
                ).fetchall()
            elif birthday:
                rows = db.execute(
                    "SELECT id, name, phone, birthday FROM customers WHERE merged_into_customer_id IS NULL AND name=? AND birthday=?",
                    (name, birthday),
                ).fetchall()

            if not rows:
                error = "查無此顧客，請確認輸入資料是否正確。"
            elif len(rows) == 1:
                result = _build_customer_result(int(rows[0]["id"]))
            else:
                # Multiple matches — let customer pick (if multiple same name and phone exist)
                candidates = [dict(r) for r in rows]

    return render_template("my.html", result=result, error=error, candidates=candidates)


def _build_customer_result(cid: int) -> dict | None:
    """Build the customer self-service view model from the canonical coin ledger.

    The summary deliberately comes from the same batches and transaction rows as
    the point history.  The customer page must explain a balance, never derive a
    second balance in the browser.
    """
    db = get_db()
    sync_coin_balance(db, cid)
    db.commit()
    row = db.execute(
        "SELECT id, name, phone, birthday, coin_balance, created_at FROM customers WHERE id=? AND merged_into_customer_id IS NULL",
        (cid,),
    ).fetchone()
    if not row:
        return None

    customer = dict(row)
    year_str = date.today().strftime("%Y")

    tier_state = get_effective_tier_state(db, cid)
    customer["tier"] = tier_state["tier"]
    customer["tier_effective_date"] = tier_state["tier_effective_date"]
    customer["tier_expires_date"] = tier_state["tier_expires_date"]
    if tier_state["tier_effective_date"]:
        year_total = _window_total(db, cid, tier_state["tier_effective_date"], tier_state["tier_expires_date"])
    else:
        year_total = customer_year_total(db, cid, year_str)
    customer["year_total"] = year_total

    coin_summary = _build_coin_summary_map(db, {cid}).get(cid, {})
    customer.update(coin_summary)
    # Keep this key for callers that used the previous display model.
    customer["total_redeemed"] = customer["total_used"]
    pending_date_row = db.execute(
        """
        SELECT MIN(credit_date) AS next_pending_date
        FROM coin_batches
        WHERE customer_id=? AND status='active' AND remaining_amount>0
          AND credit_date > ?
        """,
        (cid, date.today().isoformat()),
    ).fetchone()
    customer["next_pending_date"] = pending_date_row["next_pending_date"] if pending_date_row else None

    txns = db.execute(
        """
        SELECT t.id, t.txn_date, s.name AS store_name, t.amount, t.final_amount,
               t.coins_earned, t.coins_redeemed, t.entry_mode,
               t.recharge_plan, t.recharge_amount, t.note, t.created_at
        FROM transactions t
        JOIN stores s ON s.id = t.store_id
        WHERE t.customer_id = ? AND t.voided_at IS NULL
        ORDER BY t.txn_date DESC, t.id DESC
        """,
        (cid,),
    ).fetchall()

    upgrades = db.execute(
        """
        SELECT upgrade_date, tier_before, tier_after, trigger_reason, gift_name, gift_status, gift_delivered_at
        FROM tier_upgrades
        WHERE customer_id = ?
        ORDER BY upgrade_date DESC, id DESC
        """,
        (cid,),
    ).fetchall()

    point_adjustments = db.execute(
        """
        SELECT points, reason, operator, created_at
        FROM point_adjustments
        WHERE customer_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (cid,),
    ).fetchall()

    transaction_statuses = _build_transaction_coin_status_map(
        db, {int(t["id"]) for t in txns if int(t["coins_earned"] or 0) > 0},
    )
    point_history: list[dict[str, Any]] = []
    for t in txns:
        txn = dict(t)
        if int(txn["coins_earned"] or 0) > 0:
            status = transaction_statuses.get(int(txn["id"]), {"status": "已入帳", "available_date": ""})
            point_history.append({
                "date": txn["txn_date"],
                "timestamp": txn.get("created_at") or txn["txn_date"],
                "points_change": int(txn["coins_earned"]),
                "kind": "壽星充值" if txn["entry_mode"] == "birthday_recharge" else "一般消費",
                "detail": f"消費 ${int(float(txn['final_amount'] or txn['amount'] or 0)):,}",
                "status": status["status"],
                "available_date": status["available_date"] if status["status"] == "待入帳" else "",
            })
        if txn["entry_mode"] == "coin_deduct" and int(txn["coins_redeemed"] or 0) > 0:
            point_history.append({
                "date": txn["txn_date"],
                "timestamp": txn.get("created_at") or txn["txn_date"],
                "points_change": -int(txn["coins_redeemed"]),
                "kind": "點數使用",
                "detail": txn.get("note") or "消費折抵",
                "status": "已使用",
                "available_date": "",
            })

    for pa in point_adjustments:
        adjustment = dict(pa)
        points = int(adjustment["points"] or 0)
        point_history.append({
            "date": pa["created_at"][:10],
            "kind": "人工調整",
            "detail": pa["reason"],
            "points_change": points,
            "status": "已入帳" if points >= 0 else "已調整",
            "available_date": "",
            "timestamp": pa["created_at"],
        })

    # Some pre-V3 points were migrated directly into batches and therefore have
    # no transaction or manual-adjustment source.  They remain part of the
    # canonical ledger and must be visible, otherwise a historical total could
    # not be traced from the self-service page.
    source_free_batches = db.execute(
        """
        SELECT earned_amount, remaining_amount, credit_date, expires_date, status,
               is_legacy, created_at
        FROM coin_batches
        WHERE customer_id=? AND source_txn_id IS NULL AND source_adjustment_id IS NULL
          AND status != 'superseded_by_legacy'
        ORDER BY created_at DESC, id DESC
        """,
        (cid,),
    ).fetchall()
    today_str = date.today().isoformat()
    for batch in source_free_batches:
        if batch["status"] != "active":
            status = "已作廢"
        elif batch["credit_date"] > today_str:
            status = "待入帳"
        elif batch["expires_date"] < today_str:
            status = "已失效"
        else:
            status = "已入帳"
        point_history.append({
            "date": batch["created_at"][:10],
            "timestamp": batch["created_at"],
            "points_change": int(batch["earned_amount"]),
            "kind": "舊制轉入點數" if batch["is_legacy"] else "帳本點數",
            "detail": "歷史點數轉入" if batch["is_legacy"] else "既有帳本點數",
            "status": status,
            "available_date": batch["credit_date"] if status == "待入帳" else "",
        })

    # Expiry has no separate event table.  Active batches with remaining points
    # past their expiry are nevertheless real ledger state, so expose them as a
    # negative event instead of hiding why historical earned differs from usable.
    expired_batches = db.execute(
        """
        SELECT remaining_amount, expires_date, created_at
        FROM coin_batches
        WHERE customer_id=? AND status='active' AND remaining_amount>0
          AND expires_date < ?
        ORDER BY expires_date DESC, id DESC
        """,
        (cid, date.today().isoformat()),
    ).fetchall()
    for batch in expired_batches:
        point_history.append({
            "date": batch["expires_date"],
            "timestamp": f"{batch['expires_date']}T23:59:59",
            "points_change": -int(batch["remaining_amount"]),
            "kind": "點數失效",
            "detail": "點數已超過可使用期限",
            "status": "已失效",
            "available_date": "",
        })

    point_history.sort(key=lambda item: item["timestamp"], reverse=True)

    return {
        "customer": customer,
        "transactions": [dict(t) for t in txns],
        "upgrades": [dict(u) for u in upgrades],
        "point_history": point_history,
    }


@app.route("/spa/booking")
def spa_booking():
    db = get_db()
    stores = db.execute("SELECT id, name FROM stores ORDER BY name").fetchall()
    return render_template("spa_booking.html", stores=stores)

@app.route("/api/spa/availability")
def spa_availability():
    store_id = request.args.get("store_id", "").strip()
    year_month = request.args.get("month", "").strip() # YYYY-MM
    if not store_id or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", year_month):
        return {"error": "Missing parameters"}, 400
        
    db = get_db()
    if not db.execute("SELECT 1 FROM stores WHERE id=?", (store_id,)).fetchone():
        return {"error": "Invalid store"}, 400
    # Find all bookings for this store in this month
    rows = db.execute(
        "SELECT booking_date, booking_time, COUNT(*) as cnt FROM spa_bookings WHERE store_id = ? AND booking_date LIKE ? AND status != 'cancelled' GROUP BY booking_date, booking_time",
        (store_id, f"{year_month}-%")
    ).fetchall()
    
    # booking_limits per time slot
    # We return the counts so the frontend can disable time slots
    booked_counts = {}
    for r in rows:
        dt = r["booking_date"]
        tm = r["booking_time"]
        if dt not in booked_counts:
            booked_counts[dt] = {}
        booked_counts[dt][tm] = r["cnt"]

    # Also fetch capacity overrides
    cap_rows = db.execute(
        "SELECT override_date, override_time, capacity FROM spa_capacity_overrides WHERE store_id = ? AND override_date LIKE ?",
        (store_id, f"{year_month}-%")
    ).fetchall()
    capacities = {}
    for r in cap_rows:
        dt = r["override_date"]
        tm = r["override_time"]
        if dt not in capacities:
            capacities[dt] = {}
        capacities[dt][tm] = r["capacity"]
        
    return {"booked_counts": booked_counts, "capacities": capacities}

@app.route("/api/spa/capacity/update", methods=["POST"])
def spa_capacity_update():
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"error": "Unauthorized"}, 403
    store_id = request.form.get("store_id", "").strip()
    override_date = request.form.get("override_date", "").strip()
    override_time = request.form.get("override_time", "").strip()
    capacity = request.form.get("capacity", "").strip()
    
    if not all([store_id, override_date, override_time, capacity]):
        return {"error": "Missing parameters"}, 400
        
    try:
        capacity_int = int(capacity)
        datetime.strptime(override_date, "%Y-%m-%d")
        datetime.strptime(override_time, "%H:%M")
    except ValueError:
        return {"error": "Invalid capacity"}, 400
    if not 0 <= capacity_int <= 10:
        return {"error": "Capacity must be between 0 and 10"}, 400
        
    db = get_db()
    if not db.execute("SELECT 1 FROM stores WHERE id=?", (store_id,)).fetchone():
        return {"error": "Invalid store"}, 400
    if override_time not in get_spa_slots(store_id, datetime.strptime(override_date, "%Y-%m-%d").date()):
        return {"error": "Invalid booking slot"}, 400
    db.execute("BEGIN IMMEDIATE")
    booked = db.execute(
        "SELECT COUNT(*) AS cnt FROM spa_bookings WHERE store_id=? AND booking_date=? "
        "AND booking_time=? AND status!='cancelled'",
        (store_id, override_date, override_time),
    ).fetchone()["cnt"]
    if capacity_int < booked:
        return {"error": f"此時段已有 {booked} 位預約，上限不可調低於現有人數"}, 409
    db.execute(
        """
        INSERT INTO spa_capacity_overrides (store_id, override_date, override_time, capacity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(store_id, override_date, override_time) DO UPDATE SET capacity=excluded.capacity
        """,
        (store_id, override_date, override_time, capacity_int)
    )
    db.commit()
    return {"status": "success"}

@app.route("/api/spa/book", methods=["POST"])
def spa_book():
    store_id = request.form.get("store_id", "").strip()
    booking_date = request.form.get("booking_date", "").strip()
    booking_time = request.form.get("booking_time", "").strip()
    customer_name = request.form.get("customer_name", "").strip()
    customer_phone = request.form.get("customer_phone", "").strip()
    customer_type = request.form.get("customer_type", "").strip()
    service_type = request.form.get("service_type", "").strip()
    note = request.form.get("note", "").strip()
    pax_str = request.form.get("pax", "1").strip()
    
    try:
        pax = int(pax_str)
        if pax < 1 or pax > 2:
            pax = 1
    except ValueError:
        pax = 1
    
    if not all([store_id, booking_date, booking_time, customer_name, customer_phone, customer_type, service_type]):
        return {"error": "請填寫完整資料"}, 400
    if customer_type not in {"existing", "new"}:
        return {"error": "客群類型錯誤"}, 400
    if service_type not in {"body", "facial", "nail", "other"}:
        return {"error": "服務項目錯誤"}, 400
    if len(customer_name) > 60 or len(customer_phone) > 30 or len(note) > 500:
        return {"error": "輸入內容過長"}, 400
    if not re.fullmatch(r"[0-9+()\-\s]{8,30}", customer_phone):
        return {"error": "電話格式錯誤"}, 400
        
    is_admin = session.get("manager_authed") or session.get("main_authed")
    
    # Check 4-week limit
    try:
        b_date = datetime.strptime(booking_date, "%Y-%m-%d").date()
        datetime.strptime(booking_time, "%H:%M")
        today_date = date.today()
        if not is_admin:
            if (b_date - today_date).days > 28:
                return {"error": "僅開放4週(28天)內的預約"}, 400
            if b_date < today_date:
                return {"error": "無法預約過去的日期"}, 400
            if (b_date - today_date).days < 3:
                return {"error": "預約需至少提前3天"}, 400
    except ValueError:
        return {"error": "日期格式錯誤"}, 400

    db = get_db()
    if not db.execute("SELECT 1 FROM stores WHERE id=?", (store_id,)).fetchone():
        return {"error": "分店錯誤"}, 400
    if booking_time not in get_spa_slots(store_id, b_date):
        return {"error": "此分店當日不開放該時段"}, 400
    # SQLite 的讀取後寫入不是原子操作；先取得寫入鎖，避免兩個同時送出的請求都通過容量檢查。
    db.execute("BEGIN IMMEDIATE")
    
    # Check if same person already booked on the same day
    same_day_bookings = db.execute(
        "SELECT COUNT(*) as cnt FROM spa_bookings WHERE booking_date = ? AND customer_name = ? AND customer_phone = ? AND status != 'cancelled'",
        (booking_date, customer_name, customer_phone)
    ).fetchone()["cnt"]
    
    if same_day_bookings >= 1:
        return {"error": "同一天只能預約一個時段，若需更改請聯繫客服"}, 400
    
    # Check if this slot is full based on dynamic capacity
    cnt = db.execute(
        "SELECT COUNT(*) as cnt FROM spa_bookings WHERE store_id = ? AND booking_date = ? AND booking_time = ? AND status != 'cancelled'",
        (store_id, booking_date, booking_time)
    ).fetchone()["cnt"]
    
    cap_row = db.execute(
        "SELECT capacity FROM spa_capacity_overrides WHERE store_id = ? AND override_date = ? AND override_time = ?",
        (store_id, booking_date, booking_time)
    ).fetchone()
    
    max_cap = cap_row["capacity"] if cap_row else 2
    
    if cnt + pax > max_cap:
        return {"error": f"該時段剩餘空檔不足 {pax} 位，請選擇其他時段或減少人數"}, 400
        
    now_str = datetime.now().isoformat(timespec="seconds")
    
    # Insert main booker
    db.execute(
        """
        INSERT INTO spa_bookings (store_id, booking_date, booking_time, customer_name, customer_phone, customer_type, service_type, note, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (store_id, booking_date, booking_time, customer_name, customer_phone, customer_type, service_type, note, now_str)
    )
    
    # Insert companion if pax == 2
    if pax == 2:
        db.execute(
            """
            INSERT INTO spa_bookings (store_id, booking_date, booking_time, customer_name, customer_phone, customer_type, service_type, note, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (store_id, booking_date, booking_time, f"{customer_name} (同行)", customer_phone, customer_type, service_type, note, now_str)
        )
        
    db.commit()
    return {"status": "success"}

@app.route("/spa/admin/bookings")
def spa_admin_bookings():
    db = get_db()
    stores = db.execute("SELECT id, name FROM stores ORDER BY name").fetchall()
    
    store_id = request.args.get("store_id", "").strip()
    month = request.args.get("month", date.today().strftime("%Y-%m")).strip()
    status = request.args.get("status", "").strip()
    
    where = ["booking_date LIKE ?"]
    params = [f"{month}-%"]
    
    if store_id:
        where.append("store_id = ?")
        params.append(store_id)
        
    if status:
        where.append("status = ?")
        params.append(status)
        
    query = f"""
        SELECT sb.*, s.name as store_name
        FROM spa_bookings sb
        JOIN stores s ON s.id = sb.store_id
        WHERE {" AND ".join(where)}
        ORDER BY sb.booking_date DESC, sb.booking_time DESC
    """
    bookings = db.execute(query, params).fetchall()
    
    return render_template("spa_admin.html", bookings=bookings, stores=stores, month=month, store_id=store_id, status=status)

@app.route("/api/spa/bookings/<int:booking_id>/confirm", methods=["POST"])
def spa_admin_confirm(booking_id):
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"error": "Unauthorized"}, 403
    db = get_db()
    result = db.execute(
        "UPDATE spa_bookings SET status = 'confirmed' WHERE id = ? AND status='pending'",
        (booking_id,),
    )
    if result.rowcount == 0:
        return {"error": "Booking not found or not pending"}, 404
    db.commit()
    return {"status": "success"}

@app.route("/api/spa/bookings/<int:booking_id>/update_time", methods=["POST"])
def spa_admin_update_time(booking_id):
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"error": "Unauthorized"}, 403
    db = get_db()
    new_date = request.form.get("booking_date", "").strip()
    new_time = request.form.get("booking_time", "").strip()
    if not new_date or not new_time:
        return {"error": "Missing parameters"}, 400
    try:
        datetime.strptime(new_date, "%Y-%m-%d")
        datetime.strptime(new_time, "%H:%M")
    except ValueError:
        return {"error": "Invalid date or time"}, 400
    booking = db.execute("SELECT store_id FROM spa_bookings WHERE id=?", (booking_id,)).fetchone()
    if not booking:
        return {"error": "Booking not found"}, 404
    new_day = datetime.strptime(new_date, "%Y-%m-%d").date()
    if new_time not in get_spa_slots(booking["store_id"], new_day):
        return {"error": "此分店當日不開放該時段"}, 400
    db.execute("BEGIN IMMEDIATE")
    count = db.execute(
        "SELECT COUNT(*) AS cnt FROM spa_bookings WHERE store_id=? AND booking_date=? AND booking_time=? "
        "AND status!='cancelled' AND id!=?",
        (booking["store_id"], new_date, new_time, booking_id),
    ).fetchone()["cnt"]
    cap = db.execute(
        "SELECT capacity FROM spa_capacity_overrides WHERE store_id=? AND override_date=? AND override_time=?",
        (booking["store_id"], new_date, new_time),
    ).fetchone()
    if count >= (int(cap["capacity"]) if cap else 2):
        return {"error": "該時段已額滿"}, 409
    db.execute("UPDATE spa_bookings SET booking_date=?, booking_time=? WHERE id=?", (new_date, new_time, booking_id))
    db.commit()
    return {"status": "success"}

@app.route("/api/spa/bookings/<int:booking_id>/delete", methods=["POST"])
def spa_admin_delete(booking_id):
    if not (session.get("manager_authed") or session.get("main_authed")):
        return {"error": "Unauthorized"}, 403
    db = get_db()
    # 預約資料屬營運紀錄，取消時保留原始內容，不做不可追溯的硬刪除。
    result = db.execute(
        "UPDATE spa_bookings SET status='cancelled' WHERE id=? AND status!='cancelled'",
        (booking_id,),
    )
    if result.rowcount == 0:
        return {"error": "Booking not found or already cancelled"}, 404
    db.commit()
    return {"status": "success"}


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5090, debug=False)
