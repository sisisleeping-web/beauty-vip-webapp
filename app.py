from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
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
        """
    )

    # Migrate existing DBs
    for col, typedef in [
        ("coin_balance", "INTEGER NOT NULL DEFAULT 0"),
        ("coins_earned", "INTEGER NOT NULL DEFAULT 0"),
        ("coins_redeemed", "INTEGER NOT NULL DEFAULT 0"),
        ("recharge_plan", "TEXT DEFAULT NULL"),
        ("recharge_amount", "REAL DEFAULT NULL"),
        ("entry_mode", "TEXT NOT NULL DEFAULT 'normal'"),
    ]:
        try:
            table = "customers" if col == "coin_balance" else "transactions"
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
    row = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) AS total FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=?",
        (customer_id, year_str),
    ).fetchone()
    return float(row["total"] or 0)


def get_past_max_single(db: sqlite3.Connection, customer_id: int) -> float:
    row = db.execute(
        "SELECT COALESCE(MAX(final_amount),0) AS max_amt FROM transactions WHERE customer_id=?",
        (customer_id,),
    ).fetchone()
    return float(row["max_amt"] or 0)


def customer_month_total(db: sqlite3.Connection, customer_id: int, month_key: str) -> float:
    row = db.execute(
        "SELECT COALESCE(SUM(final_amount),0) AS total FROM transactions WHERE customer_id=? AND month_key=?",
        (customer_id, month_key),
    ).fetchone()
    return float(row["total"] or 0)


def get_customer_coin_balance(db: sqlite3.Connection, customer_id: int) -> int:
    row = db.execute("SELECT coin_balance FROM customers WHERE id=?", (customer_id,)).fetchone()
    return int(row["coin_balance"] or 0) if row else 0


def _tier_name_from_totals(max_single: float, year_total: float) -> str:
    if max_single >= 30000 or year_total >= 60000:
        return "A級美咖"
    if max_single >= 12000 or year_total >= 24000:
        return "P級美咖"
    if max_single >= 8000 or year_total >= 15000:
        return "S級美咖"
    return "一般會員"


def has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(r[1]) == column for r in rows)
    except Exception:
        return False


def get_customer_tier_map(db: sqlite3.Connection, year: str) -> dict[int, str]:
    rows = db.execute(
        """
        SELECT c.id AS customer_id,
               COALESCE(MAX(t.final_amount),0) AS max_single,
               COALESCE(SUM(CASE WHEN substr(t.txn_date,1,4)=? THEN t.final_amount ELSE 0 END),0) AS year_total
        FROM customers c
        LEFT JOIN transactions t ON t.customer_id = c.id
        GROUP BY c.id
        """,
        (year,),
    ).fetchall()
    return {int(r["customer_id"]): _tier_name_from_totals(float(r["max_single"] or 0), float(r["year_total"] or 0)) for r in rows}


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
                            coins_earned, coins_redeemed, entry_mode
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            customer_id, store_id, txn_day.isoformat(), month_key,
                            0, 0, 0, 0, now_str,
                            0, coins_to_deduct, "coin_deduct",
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
                        "coin_balance": coin_balance,
                    }

        # ── Normal Mode ──────────────────────────────────────────────────────
        else:
            try:
                amount = float(request.form.get("amount", "0") or 0)
            except ValueError:
                amount = 0.0

            if store_id and name and birthday and amount > 0:
                if amount < 1000:
                    error_message = "消費金額未達1000元，無法建檔！請確認金額。"
                else:
                    customer_id = get_or_create_customer(db, name, birthday, phone)
                    month_key = current_month_key(txn_day)
                    year_str = txn_day.strftime("%Y")

                    discount_applied = False
                    final_amount = amount

                    year_total_so_far = customer_year_total(db, customer_id, year_str)
                    past_max_single = get_past_max_single(db, customer_id)

                    tier_before = calc_tier(past_max_single, year_total_so_far, rules)
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
                    new_year_total = year_total_so_far + final_amount
                    tier_after = calc_tier(new_max_single, new_year_total, rules)
                    coin_balance = get_customer_coin_balance(db, customer_id)

                    # Get the txn_id we just inserted
                    last_txn_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

                    # Detect tier upgrade (including multi-level jumps)
                    upgrades_recorded = []
                    if tier_after.name != tier_before.name:
                        idx_before = TIER_ORDER.index(tier_before.name) if tier_before.name in TIER_ORDER else 0
                        idx_after = TIER_ORDER.index(tier_after.name) if tier_after.name in TIER_ORDER else 0
                        if idx_after > idx_before:
                            # Determine trigger reason
                            if new_max_single > past_max_single and new_max_single >= final_amount:
                                reason = f"單筆消費 {int(new_max_single):,} 元達標"
                            else:
                                reason = f"年度累計 {int(new_year_total):,} 元達標"
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

    where = ["1=1"]
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
            LEFT JOIN transactions t ON t.customer_id = c.id
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
        WHERE t.month_key >= ? AND t.month_key <= ?
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
        WHERE substr(t.txn_date,1,4) = ?
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
        "SELECT customer_id, entry_mode, coins_earned, coins_redeemed FROM transactions WHERE id=?",
        (txn_id,),
    ).fetchone()
    if not old_txn:
        return {"status": "error", "message": "original transaction not found"}, 404

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
        past_max_single = float(db.execute("SELECT COALESCE(MAX(final_amount),0) FROM transactions WHERE customer_id=? AND id < ?", (old_customer_id, txn_id)).fetchone()[0] or 0)
        year_total_so_far = float(db.execute("SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=? AND id < ?", (old_customer_id, year_str, txn_id)).fetchone()[0] or 0)
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
        "SELECT entry_mode, coins_earned, coins_redeemed, customer_id FROM transactions WHERE id=?",
        (txn_id,),
    ).fetchone()
    if not txn:
        return {"status": "error", "message": "transaction not found"}, 404

    mode = txn["entry_mode"] or "normal"
    coins_earned = int(txn["coins_earned"] or 0)
    coins_redeemed = int(txn["coins_redeemed"] or 0)
    customer_id = int(txn["customer_id"])

    db.execute("DELETE FROM transactions WHERE id=?", (txn_id,))

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
        LEFT JOIN transactions t ON s.id = t.store_id
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
        LEFT JOIN transactions t ON c.id = t.customer_id {cust_where}
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
        LEFT JOIN transactions t ON c.id = t.customer_id
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
    customers = db.execute(sql, params).fetchall()
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


@app.route("/admin/backfill_coins")
def admin_backfill_coins():
    """One-time route to back-fill coins_earned for historical transactions.
    Only accessible when manager is logged in.
    """
    if not session.get("manager_authed"):
        return "Unauthorized", 403

    db = get_db()
    rules = load_rules()

    # Fetch normal transactions that have coins_earned == 0 and final_amount > 0
    rows = db.execute(
        """
        SELECT t.id, t.final_amount, t.customer_id, t.txn_date,
               c.birthday
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        WHERE t.entry_mode = 'normal' AND t.coins_earned = 0 AND t.final_amount > 0
        """
    ).fetchall()

    updated = 0
    for r in rows:
        year_str = r["txn_date"][:4]
        year_total = float(
            db.execute(
                "SELECT COALESCE(SUM(final_amount),0) AS t FROM transactions WHERE customer_id=? AND substr(txn_date,1,4)=? AND id < ?",
                (r["customer_id"], year_str, r["id"]),
            ).fetchone()["t"] or 0
        )
        max_single = float(
            db.execute(
                "SELECT COALESCE(MAX(final_amount),0) AS m FROM transactions WHERE customer_id=? AND id < ?",
                (r["customer_id"], r["id"]),
            ).fetchone()["m"] or 0
        )
        tier = calc_tier(max_single, year_total, rules)
        coins = int(float(r["final_amount"]) * tier.points_rate)
        if coins > 0:
            db.execute(
                "UPDATE transactions SET coins_earned=? WHERE id=?",
                (coins, r["id"]),
            )
            db.execute(
                "UPDATE customers SET coin_balance = coin_balance + ? WHERE id=?",
                (coins, r["customer_id"]),
            )
            updated += 1

    db.commit()
    return f"<p>補填完成，共更新 {updated} 筆交易的 coins_earned。</p><p><a href='/report'>回報表查詢</a> | <a href='/manager'>回主管頁</a></p>"


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

    max_single = get_past_max_single(db, cid)
    year_total = customer_year_total(db, cid, year_str)
    customer["tier"] = _tier_name_from_totals(max_single, year_total)
    customer["year_total"] = year_total

    points_row = db.execute(
        "SELECT COALESCE(SUM(coins_earned),0) AS earned, COALESCE(SUM(coins_redeemed),0) AS redeemed FROM transactions WHERE customer_id=?",
        (cid,),
    ).fetchone()
    customer["total_earned"] = int(points_row["earned"] or 0)
    customer["total_redeemed"] = int(points_row["redeemed"] or 0)

    txns = db.execute(
        """
        SELECT t.txn_date, s.name AS store_name, t.amount, t.final_amount,
               t.coins_earned, t.coins_redeemed, t.entry_mode,
               t.recharge_plan, t.recharge_amount
        FROM transactions t
        JOIN stores s ON s.id = t.store_id
        WHERE t.customer_id = ?
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

    return {
        "customer": customer,
        "transactions": [dict(t) for t in txns],
        "upgrades": [dict(u) for u in upgrades],
    }


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5090, debug=False)
