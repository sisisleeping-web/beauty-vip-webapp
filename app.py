from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from flask import Flask, g, redirect, render_template, request, session, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "beauty_vip.db"
RULES_PATH = BASE_DIR / "rules.json"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("APP_SECRET_KEY", "beauty-vip-demo")
MANAGER_PIN = os.getenv("MANAGER_PIN", "1225")


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
            FOREIGN KEY(customer_id) REFERENCES customers(id),
            FOREIGN KEY(store_id) REFERENCES stores(id)
        );
        """
    )

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
    """Return True if this customer already received a birthday discount this month."""
    row = db.execute(
        "SELECT COUNT(*) AS cnt FROM transactions WHERE customer_id=? AND month_key=? AND birthday_discount_applied=1",
        (customer_id, month_key),
    ).fetchone()
    return int(row["cnt"]) > 0


def calc_tier(single_amount: float, annual_amount: float, rules: dict[str, Any]) -> TierRule:
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
        if phone:  # update phone if provided
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


@app.route("/")
def index():
    return render_template("home.html")


@app.route("/api/customers/search")
def search_customers():
    query = request.args.get("q", "").strip()
    if not query:
        return {"customers": []}
    
    db = get_db()
    # Search by name or phone
    rows = db.execute(
        "SELECT id, name, phone, birthday FROM customers WHERE name LIKE ? OR phone LIKE ? LIMIT 10",
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
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        birthday = request.form.get("birthday", "").strip()
        try:
            amount = float(request.form.get("amount", "0") or 0)
        except ValueError:
            amount = 0.0
        try:
            txn_day = parse_date_or_today(request.form.get("txn_date", ""))
        except ValueError:
            txn_day = date.today()

        if store_id and name and birthday and amount > 0:
            if amount < 1000:
                error_message = "消費金額未達1000元，無法建檔！請確認金額。"
            else:
                customer_id = get_or_create_customer(db, name, birthday, phone)
                month_key = current_month_key(txn_day)
                year_str = txn_day.strftime("%Y")
    
                birthday_discount_rate = float(rules["birthday_offer"]["discount_rate"])
                once_per_month = bool(rules["birthday_offer"].get("once_per_month", True))
                in_birthday_month = is_birthday_month(birthday, txn_day)
                already_used = once_per_month and birthday_discount_used_this_month(db, customer_id, month_key)
                discount_applied = in_birthday_month and not already_used
                final_amount = amount * (1 - birthday_discount_rate) if discount_applied else amount
    
                year_total_so_far = customer_year_total(db, customer_id, year_str)
                past_max_single = get_past_max_single(db, customer_id)
                
                # 回饋金與點數使用「本次消費前」的等級計算
                tier_before = calc_tier(past_max_single, year_total_so_far, rules)
                cashback = round(final_amount * tier_before.cashback_rate, 2)
                points = int(final_amount * tier_before.points_rate)

                db.execute(
                    """
                    INSERT INTO transactions(
                        customer_id, store_id, txn_date, month_key, amount,
                        birthday_discount_applied, final_amount, cashback, created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
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
                    ),
                )
                db.commit()
    
                monthly_total = customer_month_total(db, customer_id, month_key)
                
                # 計算「本次消費後」的新等級（供前台顯示）
                new_max_single = max(past_max_single, final_amount)
                new_year_total = year_total_so_far + final_amount
                tier_after = calc_tier(new_max_single, new_year_total, rules)
    
                result = {
                "name": name,
                "store_id": store_id,
                "amount": amount,
                "final_amount": round(final_amount, 2),
                "birthday_discount_applied": discount_applied,
                "monthly_total": round(monthly_total, 2),
                "tier": tier_after,
                "cashback": cashback,
                "points": points,
            }

    return render_template("entry.html", stores=stores, result=result, selected_store=selected_store, error=error_message)


@app.route("/report")
def report():
    db = get_db()
    month = request.args.get("month", date.today().strftime("%Y-%m"))

    detail_rows = db.execute(
        """
        SELECT t.id, t.txn_date, s.name AS store_name, c.name AS customer_name, c.birthday,
               t.amount, t.final_amount, t.birthday_discount_applied
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        JOIN stores s ON s.id = t.store_id
        WHERE t.month_key = ?
        ORDER BY t.txn_date DESC, s.name ASC, c.name ASC
        """,
        (month,),
    ).fetchall()

    monthly_by_customer = db.execute(
        """
        SELECT s.name AS store_name, c.name AS customer_name, c.birthday,
               SUM(t.final_amount) AS month_total
        FROM transactions t
        JOIN customers c ON c.id = t.customer_id
        JOIN stores s ON s.id = t.store_id
        WHERE t.month_key = ?
        GROUP BY s.name, c.name, c.birthday
        ORDER BY s.name, month_total DESC
        """,
        (month,),
    ).fetchall()

    year = month[:4]
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

    return render_template(
        "report.html",
        month=month,
        detail_rows=detail_rows,
        monthly_by_customer=monthly_by_customer,
        yearly_by_customer=yearly_by_customer,
    )


@app.route("/api/transactions/<int:txn_id>/delete", methods=["POST"])
def delete_transaction(txn_id):
    db = get_db()
    db.execute("DELETE FROM transactions WHERE id=?", (txn_id,))
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
    month = request.args.get("month", date.today().strftime("%Y-%m"))
    year = month[:4]

    store_stats = db.execute(
        """
        SELECT s.name AS store_name,
               SUM(CASE WHEN t.month_key = ? THEN t.final_amount ELSE 0 END) AS month_revenue,
               SUM(CASE WHEN substr(t.txn_date,1,4) = ? THEN t.final_amount ELSE 0 END) AS year_revenue
        FROM stores s
        LEFT JOIN transactions t ON s.id = t.store_id
        GROUP BY s.name
        ORDER BY s.name
        """,
        (month, year),
    ).fetchall()

    customer_stats = db.execute(
        """
        SELECT c.name, c.phone, c.birthday,
               SUM(CASE WHEN t.month_key = ? THEN t.final_amount ELSE 0 END) AS month_spend,
               SUM(CASE WHEN substr(t.txn_date,1,4) = ? THEN t.final_amount ELSE 0 END) AS year_spend,
               SUM(t.cashback) AS total_cashback
        FROM customers c
        LEFT JOIN transactions t ON c.id = t.customer_id
        GROUP BY c.id, c.name, c.phone, c.birthday
        HAVING year_spend > 0 OR month_spend > 0
        ORDER BY year_spend DESC
        """,
        (month, year),
    ).fetchall()

    return render_template("manager.html", month=month, store_stats=store_stats, customer_stats=customer_stats)


@app.route("/contacts")
def contacts():
    db = get_db()
    query = request.args.get("q", "").strip()
    if query:
        customers = db.execute(
            "SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? ORDER BY name",
            (f"%{query}%", f"%{query}%")
        ).fetchall()
    else:
        customers = db.execute("SELECT * FROM customers ORDER BY name").fetchall()
    return render_template("contacts.html", customers=customers, query=query)


@app.route("/api/customers/<int:customer_id>/update", methods=["POST"])
def update_customer(customer_id):
    db = get_db()
    phone = request.form.get("phone", "").strip()
    birthday = request.form.get("birthday", "").strip()
    try:
        if birthday:
            datetime.strptime(birthday, "%Y-%m-%d")
    except ValueError:
        return "生日格式錯誤，請使用 YYYY-MM-DD", 400
        
    db.execute("UPDATE customers SET phone=?, birthday=? WHERE id=?", (phone, birthday, customer_id))
    db.commit()
    return redirect(url_for("contacts"))


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5090, debug=False)
