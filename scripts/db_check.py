#!/usr/bin/env python3
"""
db_check.py — 美咖美容 VIP 資料完整性驗算

執行方式：
  python3 scripts/db_check.py          # 本機 DB
  python3 scripts/db_check.py [path]   # 指定 DB 路徑
"""
import sqlite3, json, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else BASE / "data" / "beauty_vip.db"
RULES_PATH = BASE / "rules.json"

if not DB_PATH.exists():
    print(f"DB not found: {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

errors = []
warnings = []

# ─── Check 1: birthday_discount_applied 應全為 0 ──────────────────────────
disc = conn.execute("SELECT COUNT(*) FROM transactions WHERE birthday_discount_applied=1").fetchone()[0]
if disc > 0:
    errors.append(f"[ERROR] {disc} 筆 birthday_discount_applied=1（應為 0，折扣機制已移除）")
else:
    print(f"[OK] birthday_discount_applied: 全為 0")

# ─── Check 2: normal 交易 final_amount 應等於 amount ─────────────────────
mismatch = conn.execute("""
    SELECT COUNT(*) FROM transactions
    WHERE entry_mode='normal' AND ABS(final_amount - amount) > 0.01
""").fetchone()[0]
if mismatch > 0:
    errors.append(f"[ERROR] {mismatch} 筆 normal 交易 final_amount ≠ amount（不應有折扣）")
else:
    print(f"[OK] normal 交易 final_amount == amount: 全部一致")

# ─── Check 3: coin_balance 完整性 ────────────────────────────────────────
customers = conn.execute("SELECT id, name, coin_balance FROM customers").fetchall()
bad_balances = []
for c in customers:
    row = conn.execute("""
        SELECT
          COALESCE(SUM(CASE WHEN entry_mode IN ('normal','birthday_recharge') THEN coins_earned ELSE 0 END),0) AS earned,
          COALESCE(SUM(CASE WHEN entry_mode='coin_deduct' THEN coins_redeemed ELSE 0 END),0) AS redeemed
        FROM transactions WHERE customer_id=?
    """, (c["id"],)).fetchone()
    expected = int(row["earned"]) - int(row["redeemed"])
    if expected != c["coin_balance"]:
        bad_balances.append(f"  {c['name']}: balance={c['coin_balance']} expected={expected} (Δ{expected - c['coin_balance']:+d})")
if bad_balances:
    errors.append(f"[ERROR] {len(bad_balances)} 位顧客 coin_balance 不一致：\n" + "\n".join(bad_balances))
else:
    print(f"[OK] coin_balance: {len(customers)} 位顧客全部一致")

# ─── Check 4: coin_balance 不應為負 ──────────────────────────────────────
neg = conn.execute("SELECT COUNT(*) FROM customers WHERE coin_balance < 0").fetchone()[0]
if neg > 0:
    errors.append(f"[ERROR] {neg} 位顧客 coin_balance < 0")
else:
    print(f"[OK] coin_balance: 無負值")

# ─── Check 5: 壽星充值僅限當月 ───────────────────────────────────────────
invalid_recharge = conn.execute("""
    SELECT t.id, c.name, c.birthday, t.txn_date FROM transactions t
    JOIN customers c ON c.id = t.customer_id
    WHERE t.entry_mode='birthday_recharge'
    AND substr(c.birthday,6,2) != substr(t.txn_date,6,2)
""").fetchall()
if invalid_recharge:
    warnings.append(f"[WARN] {len(invalid_recharge)} 筆壽星充值日期非生日月：" +
                    ", ".join(f"ID={r['id']} {r['name']}" for r in invalid_recharge))
else:
    print(f"[OK] 壽星充值日期: 全部在生日月份")

# ─── Check 6: 最低消費 1000 元 ───────────────────────────────────────────
under_min = conn.execute("""
    SELECT COUNT(*) FROM transactions WHERE entry_mode='normal' AND amount < 1000
""").fetchone()[0]
if under_min > 0:
    warnings.append(f"[WARN] {under_min} 筆 normal 交易金額 < 1000（低於門檻）")
else:
    print(f"[OK] 最低消費門檻: 無低於 1000 的 normal 交易")

# ─── Summary ─────────────────────────────────────────────────────────────
total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
cust_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
total_rev = conn.execute("SELECT COALESCE(SUM(final_amount),0) FROM transactions WHERE entry_mode='normal'").fetchone()[0]

print(f"\n=== 統計摘要 ===")
print(f"顧客數：{cust_count}")
print(f"交易筆數：{total}")
print(f"正常消費總額：{total_rev:,.0f}")

if warnings:
    print("\n" + "\n".join(warnings))
if errors:
    print("\n" + "\n".join(errors))
    sys.exit(1)
else:
    print("\n✓ 所有檢查通過")

conn.close()
