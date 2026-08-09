from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from flask import Flask, g, redirect, render_template, request, session, url_for, send_file

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "beauty_vip.db"
RULES_PATH = BASE_DIR / "rules.json"

APP_VERSION = "1.2.0"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("APP_SECRET_KEY", "beauty-vip-demo")
MANAGER_PIN = os.getenv("MANAGER_PIN", "1225")
MAIN_PIN = os.getenv("MAIN_PIN", "27789254")

@app.before_request
def require_main_auth():
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
    pin = (request.form.get("pin") or "").strip()
    if pin == MAIN_PIN:
        session["main_authed"] = True
        return redirect(url_for("index"))
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


def _project_tier_state(db: sqlite3.Connection, customer_id: int, as_of: str) -> dict | None:
    """純讀取：算出 as_of 這天套用「pending 升等生效」與「會員年度到期重判」後的等級狀態，不寫入。"""
    row = db.execute(
        "SELECT member_tier, tier_effective_date, tier_expires_date, pending_tier, pending_effective_date "
        "FROM customers WHERE id=?",
        (customer_id,),
    ).fetchone()
    if not row:
        return None
    state = dict(row)

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
        window_total = _window_total(db, customer_id, state["tier_effective_date"], state["tier_expires_date"])
        new_tier = _tier_from_window_total(window_total)
        if new_tier == "一般會員":
            state["member_tier"] = "一般會員"
            state["tier_effective_date"] = None
            state["tier_expires_date"] = None
        else:
            state["member_tier"] = new_tier
            state["tier_effective_date"] = state["tier_expires_date"]  # 新一期接續上一期到期日
            state["tier_expires_date"] = _add_years(state["tier_effective_date"], 1)
        iterations += 1

    return state


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
    state = _project_tier_state(db, customer_id, as_of_str)
    return state["member_tier"] if state else "一般會員"


_EMPTY_TIER_STATE = {
    "member_tier": "一般會員", "tier_effective_date": None, "tier_expires_date": None,
    "pending_tier": None, "pending_effective_date": None,
}


def reevaluate_and_persist_tier(db: sqlite3.Connection, customer_id: int, as_of: date) -> dict:
    """entry() 記一筆之前、或管理後台手動批次重新評估時呼叫：把最新狀態寫回 customers，回傳完整狀態。"""
    state = _project_tier_state(db, customer_id, as_of.isoformat())
    if state:
        _persist_tier_state(db, customer_id, state)
    return state or dict(_EMPTY_TIER_STATE)


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
    if state["tier_effective_date"]:
        accum_total = _window_total(db, customer_id, state["tier_effective_date"], state["tier_expires_date"])
    else:
        accum_total = customer_year_total(db, customer_id, as_of.strftime("%Y"))
    recomputed = calc_tier(new_max_single, accum_total, rules)

    idx_current = TIER_ORDER.index(current_tier_name) if current_tier_name in TIER_ORDER else 0
    idx_recomputed = TIER_ORDER.index(recomputed.name) if recomputed.name in TIER_ORDER else 0
    if idx_recomputed >= idx_current:
        return  # 扣除這筆之後，累計仍然支撐得起現有等級，不用動

    now_str = datetime.now().isoformat(timespec="seconds")
    reason = f"作廢交易 id={trigger_txn_id} 後累計降為 {int(accum_total):,} 元" + (
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
        "SELECT id FROM customers WHERE name=? AND birthday=?", (name, birthday)
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


def has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(r[1]) == column for r in rows)
    except Exception:
        return False


def get_customer_tier_map(db: sqlite3.Connection, year: str) -> dict[int, str]:
    """回傳每位顧客「現在」的有效等級（V3 Phase 1 起用 get_effective_tier 現算，不寫入）。
    year 參數保留給既有呼叫端相容，但等級一律代表現況，不是某個歷史日曆年的回推。"""
    customer_ids = [int(r["id"]) for r in db.execute("SELECT id FROM customers").fetchall()]
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
        "SELECT id, name, phone, birthday, coin_balance FROM customers WHERE name LIKE ? OR phone LIKE ? LIMIT 10",
        (f"%{query}%", f"%{query}%")
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
                        # Credit coins to customer
                        db.execute(
                            "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
                            (plan_coins, customer_id),
                        )
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
                current_coins = get_customer_coin_balance(db, customer_id)
                if coins_to_deduct > current_coins:
                    error_message = f"點數不足！目前餘額：{current_coins} 點，欲扣：{coins_to_deduct} 點。"
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
                    db.execute(
                        "UPDATE customers SET coin_balance = coin_balance - ? WHERE id=?",
                        (coins_to_deduct, customer_id),
                    )
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
                # Update coin balance
                if coins_earned > 0:
                    db.execute(
                        "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
                        (coins_earned, customer_id),
                    )
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

                # Get the txn_id we just inserted
                last_txn_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

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


@app.route("/report")
def report():
    db = get_db()
    default_month = date.today().strftime("%Y-%m")
    month_from = request.args.get("month_from", "").strip() or request.args.get("month", default_month)
    month_to = request.args.get("month_to", "").strip() or month_from
    # Ensure month_to >= month_from
    if month_to < month_from:
        month_to = month_from
    year = request.args.get("year", month_from[:4])
    store_id = request.args.get("store_id", "").strip()
    q = request.args.get("q", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    birthday_month = request.args.get("birthday_month", "").strip()
    vip_tier = request.args.get("vip_tier", "").strip()

    # Build human-readable month label
    if month_from == month_to:
        month_label = month_from
    else:
        month_label = f"{month_from} ~ {month_to}"

    where = ["t.voided_at IS NULL"]
    params: list[Any] = []

    where.append("t.month_key >= ? AND t.month_key <= ?")
    params.extend([month_from, month_to])

    if store_id:
        where.append("t.store_id = ?")
        params.append(store_id)
    if q:
        where.append("c.name LIKE ?")
        params.append(f"%{q}%")
    if start_date:
        where.append("t.txn_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("t.txn_date <= ?")
        params.append(end_date)
    if birthday_month:
        where.append("substr(c.birthday,6,2) = ?")
        params.append(birthday_month.zfill(2))

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

    tier_map = get_customer_tier_map(db, year)

    # Build per-customer coin summary
    customer_ids_in_result = set(int(r["customer_id"]) for r in detail_rows_raw)
    coin_summary_map: dict[int, dict] = {}
    if customer_ids_in_result:
        placeholders = ",".join("?" * len(customer_ids_in_result))
        coin_rows = db.execute(
            f"""
            SELECT c.id AS customer_id,
                   c.coin_balance,
                   COALESCE(SUM(t.coins_earned), 0) AS total_earned,
                   COALESCE(SUM(t.coins_redeemed), 0) AS total_redeemed
            FROM customers c
            LEFT JOIN transactions t ON t.customer_id = c.id AND t.voided_at IS NULL
            WHERE c.id IN ({placeholders})
            GROUP BY c.id
            """,
            list(customer_ids_in_result),
        ).fetchall()
        for cr in coin_rows:
            coin_summary_map[int(cr["customer_id"])] = {
                "total_earned": int(cr["total_earned"] or 0),
                "total_redeemed": int(cr["total_redeemed"] or 0),
                "coin_balance": int(cr["coin_balance"] or 0),
            }

    detail_rows = []
    for r in detail_rows_raw:
        d = dict(r)
        d["vip_tier"] = tier_map.get(int(r["customer_id"]), "一般會員")
        if vip_tier and d["vip_tier"] != vip_tier:
            continue
        cs = coin_summary_map.get(int(r["customer_id"]), {})
        d["cust_total_earned"] = cs.get("total_earned", 0)
        d["cust_total_redeemed"] = cs.get("total_redeemed", 0)
        d["cust_coin_balance"] = cs.get("coin_balance", 0)
        detail_rows.append(d)

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
        (year,),
    ).fetchall()

    stores = db.execute("SELECT id,name FROM stores ORDER BY name").fetchall()

    filters = {
        "store_id": store_id,
        "q": q,
        "start_date": start_date,
        "end_date": end_date,
        "birthday_month": birthday_month,
        "vip_tier": vip_tier,
        "year": year,
        "month_from": month_from,
        "month_to": month_to,
    }

    return render_template(
        "report.html",
        month=month_from,
        month_label=month_label,
        month_from=month_from,
        month_to=month_to,
        detail_rows=detail_rows,
        monthly_by_customer=monthly_by_customer,
        yearly_by_customer=yearly_by_customer,
        stores=stores,
        filters=filters,
    )


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
    old_coins_redeemed = int(old_txn["coins_redeemed"] or 0)
    old_customer_id = int(old_txn["customer_id"])

    # 2. Revert old coin balance impact
    if old_mode in ("normal", "birthday_recharge") and old_coins_earned > 0:
        db.execute("UPDATE customers SET coin_balance = coin_balance - ? WHERE id=?", (old_coins_earned, old_customer_id))
    elif old_mode == "coin_deduct" and old_coins_redeemed > 0:
        db.execute("UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?", (old_coins_redeemed, old_customer_id))

    # 3. Update customer basic info (if changed)
    db.execute(
        "UPDATE customers SET name=?, birthday=? WHERE id=?",
        (name, birthday, old_customer_id),
    )

    month_key = current_month_key(d)
    new_final_amount = cash_received if cash_received is not None else amount
    new_coins_earned = 0

    # 4. Recalculate coins if it's a normal transaction
    if old_mode == "normal":
        rules = load_rules()
        year_str = d.strftime("%Y")
        past_max_single = float(db.execute("SELECT COALESCE(MAX(final_amount),0) FROM transactions WHERE customer_id=? AND id < ? AND voided_at IS NULL", (old_customer_id, txn_id)).fetchone()[0] or 0)
        year_total_so_far = float(db.execute("SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=? AND id < ? AND final_amount>=1000 AND voided_at IS NULL", (old_customer_id, year_str, txn_id)).fetchone()[0] or 0)
        tier = calc_tier(past_max_single, year_total_so_far, rules)
        new_coins_earned = int(new_final_amount * tier.points_rate)

    # 5. Update transaction record
    db.execute(
        """
        UPDATE transactions
        SET store_id=?, txn_date=?, month_key=?, amount=?, final_amount=?, coins_earned=?
        WHERE id=?
        """,
        (store_id, d.isoformat(), month_key, amount, new_final_amount, new_coins_earned, txn_id),
    )

    # 6. Apply new coin balance impact
    if old_mode == "normal" and new_coins_earned > 0:
        db.execute("UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?", (new_coins_earned, old_customer_id))
    elif old_mode == "birthday_recharge":
        db.execute("UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?", (old_coins_earned, old_customer_id))
    elif old_mode == "coin_deduct":
        db.execute("UPDATE customers SET coin_balance = coin_balance - ? WHERE id=?", (old_coins_redeemed, old_customer_id))

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

    # Revert coin_balance impact
    if mode in ("normal", "birthday_recharge") and coins_earned > 0:
        # Coins were credited — take them back
        db.execute(
            "UPDATE customers SET coin_balance = coin_balance - ? WHERE id=?",
            (coins_earned, customer_id),
        )
    elif mode == "coin_deduct" and coins_redeemed > 0:
        # Coins were debited — give them back
        db.execute(
            "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
            (coins_redeemed, customer_id),
        )

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

    old_points = int(old_txn["coins_redeemed"])
    customer_id = int(old_txn["customer_id"])

    # Revert old impact and apply new impact
    diff = old_points - new_points
    
    db.execute(
        "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
        (diff, customer_id)
    )
    db.execute(
        "UPDATE transactions SET coins_redeemed = ?, note = ? WHERE id=?",
        (new_points, new_reason, txn_id)
    )
    db.commit()
    return {"status": "ok"}



@app.route("/manager/unlock", methods=["POST"])
def manager_unlock():
    pin = (request.form.get("pin") or "").strip()
    if pin == MANAGER_PIN:
        session["manager_authed"] = True
        return redirect(url_for("manager_dashboard"))
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
        "SELECT id FROM customers WHERE tier_expires_date IS NOT NULL AND tier_expires_date < ?",
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
    query = request.args.get("q", "").strip()
    store_id = request.args.get("store_id", "").strip()
    birthday_month = request.args.get("birthday_month", "").strip()
    min_spend = request.args.get("min_spend", "").strip()
    max_spend = request.args.get("max_spend", "").strip()
    last_from = request.args.get("last_from", "").strip()
    last_to = request.args.get("last_to", "").strip()

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
        WHERE 1=1
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
        d["tier"] = get_effective_tier(db, int(row["id"]))
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
    }

    return render_template("contacts.html", customers=customers, query=query, stores=stores, filters=filters)


@app.route("/api/customers/<int:customer_id>/delete", methods=["POST"])
def delete_customer(customer_id):
    db = get_db()
    db.execute("DELETE FROM transactions WHERE customer_id=?", (customer_id,))
    db.execute("DELETE FROM customers WHERE id=?", (customer_id,))
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
    db.execute(
        "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
        (points, customer_id),
    )
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

    diff = new_points - int(old["points"])
    db.execute(
        "UPDATE point_adjustments SET points=?, reason=? WHERE id=?",
        (new_points, new_reason, adj_id),
    )
    if diff != 0:
        db.execute(
            "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
            (diff, old["customer_id"]),
        )
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
    db.execute(
        "UPDATE customers SET coin_balance = coin_balance - ? WHERE id=?",
        (old["points"], old["customer_id"]),
    )
    db.commit()
    return {"status": "ok"}


@app.route("/api/customers/<int:customer_id>/update", methods=["POST"])
def update_customer(customer_id):
    db = get_db()
    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    birthday = request.form.get("birthday", "").strip()
    try:
        if birthday:
            datetime.strptime(birthday, "%Y-%m-%d")
    except ValueError:
        return "生日格式錯誤，請使用 YYYY-MM-DD", 400

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
            db.execute(f"UPDATE customers SET {', '.join(updates)} WHERE id=?", params)
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
        WHERE c.birthday IN ({placeholders})
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
        FROM customers
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

    return render_template("review.html", suspicious_rows=suspicious_rows, multi_bday_rows=multi_bday_rows, stores=stores, filters=filters)


@app.route("/review/update_birthday/<int:customer_id>", methods=["POST"])
def review_update_birthday(customer_id):
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    db = get_db()
    birthday = request.form.get("birthday", "").strip()
    try:
        if birthday:
            datetime.strptime(birthday, "%Y-%m-%d")
    except ValueError:
        return "生日格式錯誤", 400
    try:
        db.execute("UPDATE customers SET birthday=? WHERE id=?", (birthday, customer_id))
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
    """Merge keep_id ← absorb_id: move all transactions, then delete absorb_id."""
    if not session.get("manager_authed"):
        return "Unauthorized", 403
    db = get_db()
    try:
        keep_id = int(request.form.get("keep_id", "0"))
        absorb_id = int(request.form.get("absorb_id", "0"))
    except (ValueError, TypeError):
        return "參數錯誤", 400

    if keep_id == absorb_id or not keep_id or not absorb_id:
        return "請選擇兩個不同的顧客", 400

    keep = db.execute("SELECT id, name, phone, birthday, coin_balance FROM customers WHERE id=?", (keep_id,)).fetchone()
    absorb = db.execute("SELECT id, name, phone, birthday, coin_balance FROM customers WHERE id=?", (absorb_id,)).fetchone()

    if not keep or not absorb:
        return "找不到指定顧客", 404

    # Move all transactions from absorb → keep
    db.execute("UPDATE transactions SET customer_id=? WHERE customer_id=?", (keep_id, absorb_id))

    # Merge coin balance
    merged_coins = int(keep["coin_balance"] or 0) + int(absorb["coin_balance"] or 0)
    # Keep the more complete phone (prefer non-empty)
    merged_phone = keep["phone"] or absorb["phone"] or ""
    db.execute(
        "UPDATE customers SET coin_balance=?, phone=? WHERE id=?",
        (merged_coins, merged_phone, keep_id),
    )

    # Delete absorbed customer
    db.execute("DELETE FROM customers WHERE id=?", (absorb_id,))

    # Clean up review flags for absorbed customer
    db.execute("DELETE FROM review_flags WHERE item_type='birthday_suspicious' AND item_key=?", (str(absorb_id),))

    db.commit()
    return redirect(request.referrer or url_for("review_page"))


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
                    "SELECT id, name, phone, birthday FROM customers WHERE name=? AND phone=? AND birthday=?",
                    (name, phone, birthday),
                ).fetchall()
            elif phone:
                rows = db.execute(
                    "SELECT id, name, phone, birthday FROM customers WHERE name=? AND phone=?",
                    (name, phone),
                ).fetchall()
            elif birthday:
                rows = db.execute(
                    "SELECT id, name, phone, birthday FROM customers WHERE name=? AND birthday=?",
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
    """Build the full customer result dict for display."""
    db = get_db()
    row = db.execute(
        "SELECT id, name, phone, birthday, coin_balance, created_at FROM customers WHERE id=?",
        (cid,),
    ).fetchone()
    if not row:
        return None

    customer = dict(row)
    year_str = date.today().strftime("%Y")

    customer["tier"] = get_effective_tier(db, cid)
    tier_row = db.execute(
        "SELECT tier_effective_date, tier_expires_date FROM customers WHERE id=?", (cid,)
    ).fetchone()
    if tier_row["tier_effective_date"]:
        year_total = _window_total(db, cid, tier_row["tier_effective_date"], tier_row["tier_expires_date"])
    else:
        year_total = customer_year_total(db, cid, year_str)
    customer["year_total"] = year_total

    points_row = db.execute(
        "SELECT COALESCE(SUM(coins_earned),0) AS earned, COALESCE(SUM(coins_redeemed),0) AS redeemed "
        "FROM transactions WHERE customer_id=? AND voided_at IS NULL",
        (cid,),
    ).fetchone()
    customer["total_earned"] = int(points_row["earned"] or 0)
    customer["total_redeemed"] = int(points_row["redeemed"] or 0)

    txns = db.execute(
        """
        SELECT t.txn_date, s.name AS store_name, t.amount, t.final_amount,
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

    adjustments_history = []
    for pa in point_adjustments:
        adjustments_history.append({
            "date": pa["created_at"][:10],
            "reason": pa["reason"],
            "points_change": pa["points"],
            "timestamp": pa["created_at"],
        })
    for t in txns:
        if t["entry_mode"] == "coin_deduct":
            adjustments_history.append({
                "date": t["txn_date"],
                "reason": dict(t).get("note") or "點數扣除",
                "points_change": -t["coins_redeemed"],
                "timestamp": dict(t).get("created_at") or t["txn_date"],
            })
    adjustments_history.sort(key=lambda x: x["timestamp"], reverse=True)

    return {
        "customer": customer,
        "transactions": [dict(t) for t in txns],
        "upgrades": [dict(u) for u in upgrades],
        "point_adjustments": adjustments_history,
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
    if not store_id or not year_month:
        return {"error": "Missing parameters"}, 400
        
    db = get_db()
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
    store_id = request.form.get("store_id", "").strip()
    override_date = request.form.get("override_date", "").strip()
    override_time = request.form.get("override_time", "").strip()
    capacity = request.form.get("capacity", "").strip()
    
    if not all([store_id, override_date, override_time, capacity]):
        return {"error": "Missing parameters"}, 400
        
    try:
        capacity_int = int(capacity)
    except ValueError:
        return {"error": "Invalid capacity"}, 400
        
    db = get_db()
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
        
    is_admin = session.get("manager_authed") or session.get("main_authed")
    
    # Check 4-week limit
    try:
        b_date = datetime.strptime(booking_date, "%Y-%m-%d").date()
        today_date = date.today()
        if not is_admin:
            if (b_date - today_date).days > 28:
                return {"error": "僅開放4週(28天)內的預約"}, 400
            if b_date < today_date:
                return {"error": "無法預約過去的日期"}, 400
    except ValueError:
        return {"error": "日期格式錯誤"}, 400

    db = get_db()
    
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
    db = get_db()
    db.execute("UPDATE spa_bookings SET status = 'confirmed' WHERE id = ?", (booking_id,))
    db.commit()
    return {"status": "success"}

@app.route("/api/spa/bookings/<int:booking_id>/update_time", methods=["POST"])
def spa_admin_update_time(booking_id):
    db = get_db()
    new_date = request.form.get("booking_date", "").strip()
    new_time = request.form.get("booking_time", "").strip()
    if not new_date or not new_time:
        return {"error": "Missing parameters"}, 400
    db.execute("UPDATE spa_bookings SET booking_date=?, booking_time=? WHERE id=?", (new_date, new_time, booking_id))
    db.commit()
    return {"status": "success"}

@app.route("/api/spa/bookings/<int:booking_id>/delete", methods=["POST"])
def spa_admin_delete(booking_id):
    db = get_db()
    db.execute("DELETE FROM spa_bookings WHERE id = ?", (booking_id,))
    db.commit()
    return {"status": "success"}


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5090, debug=False)
